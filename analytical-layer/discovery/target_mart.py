"""
Module: target_mart.py
Description: Target-to-Phenotype Evidence Mart and Discovery Lakehouse Aggregation Engine.
             Joins OMOP CDM v5.4 clinical condition occurrences and ClinVar genomic variant
             measurements across longitudinal cohorts to calculate target tractability metrics,
             target mutation burden, Haldane-Anscombe phenotypic Odds Ratios, 95% confidence intervals,
             and asymptotic significance statistics. Persists with Delta Lake Liquid Clustering
             and enforces declarative GxP contracts with dead-letter quarantine routing.
Author: Vivi Tsoumaki
"""

import logging
import math
import os
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
)
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    exp,
    greatest,
    least,
    lit,
    log,
    log10,
    regexp_extract,
    sqrt,
    trim,
    upper,
    when,
)
from pyspark.sql.functions import (
    count as spark_count,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    round as spark_round,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from governance.mlflow_tracker import evaluate_data_contract
from medallion.quarantine import (
    QUARANTINE_TABLE_TARGETS,
    ClinicalFailureCode,
    QuarantineDeltaWriter,
    format_quarantine_dataframe,
    get_active_mlflow_run_id,
)
from medallion.writer import DeltaMedallionWriter

logger = logging.getLogger(__name__)

# Abramowitz & Stegun (7.1.26) constants for standard normal CDF approximation
# Maximum error |eps| < 7.5e-8 across all real values
_P_CONST = 0.2316419
_B1 = 0.319381530
_B2 = -0.356563782
_B3 = 1.781477937
_B4 = -1.821255978
_B5 = 1.330274429
_INV_SQRT_2PI = 1.0 / math.sqrt(2.0 * math.pi)

# Standard Target Evidence Schema for OMOP Discovery Mart
TARGET_EVIDENCE_SCHEMA = StructType(
    [
        StructField("target_gene_symbol", StringType(), False),
        StructField("disease_concept_id", LongType(), False),
        StructField("carrier_cases", IntegerType(), False),
        StructField("carrier_controls", IntegerType(), False),
        StructField("non_carrier_cases", IntegerType(), False),
        StructField("non_carrier_controls", IntegerType(), False),
        StructField("total_cohort_size", IntegerType(), False),
        StructField("target_mutation_burden", DoubleType(), False),
        StructField("carrier_frequency", DoubleType(), False),
        StructField("phenotype_prevalence", DoubleType(), False),
        StructField("odds_ratio", DoubleType(), False),
        StructField("log_odds_ratio", DoubleType(), False),
        StructField("se_log_odds_ratio", DoubleType(), False),
        StructField("odds_ratio_ci_lower", DoubleType(), False),
        StructField("odds_ratio_ci_upper", DoubleType(), False),
        StructField("p_value", DoubleType(), False),
        StructField("biomarker_correlation", DoubleType(), False),
        StructField("target_tractability_score", DoubleType(), False),
        StructField("evidence_tier", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("mlflow_run_id", StringType(), False),
    ]
)


@dataclass
class TargetEvidenceMartConfig:
    """Configuration parameters for Target-to-Phenotype Evidence Mart calculations."""

    min_carrier_count: int = 1
    haldane_anscombe_correction: float = 0.5
    confidence_z: float = 1.96  # 95% Confidence Interval z-score
    genomic_concept_id: int = 35917873  # OMOP standard concept: Genomic variant
    pathogenic_concept_ids: list[int] = field(
        default_factory=lambda: [4181412, 36768280]  # ClinVar Pathogenic / Likely pathogenic
    )
    weight_genetic: float = 0.50
    weight_burden: float = 0.25
    weight_biomarker: float = 0.25
    cluster_by: list[str] = field(
        default_factory=lambda: ["target_gene_symbol", "disease_concept_id"]
    )
    table_name: str = "target_disease_evidence"


class TargetEvidenceMart:
    """
    Enterprise PySpark Target-to-Phenotype Evidence Mart Engine.
    Aggregates multi-omics ClinVar genomic observations and longitudinal clinical diagnoses
    across patient cohorts to evaluate target tractability, disease odds ratios, and mutation burdens.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: TargetEvidenceMartConfig | None = None,
    ):
        self.spark = spark
        self.config = config or TargetEvidenceMartConfig()

    def extract_target_variant_carriers(
        self,
        df_measurement: DataFrame,
        df_cohort: DataFrame | None = None,
    ) -> DataFrame:
        """
        Extracts gene symbols, variant counts, and carrier status from OMOP MEASUREMENT table.

        Args:
            df_measurement: OMOP MEASUREMENT DataFrame.
            df_cohort: Optional cohort DataFrame to restrict analysis population.

        Returns:
            DataFrame with columns: [person_id, target_gene_symbol, variant_count, is_carrier, has_pathogenic_variant].
        """
        if df_measurement is None or df_measurement.limit(1).count() == 0:
            empty_schema = StructType(
                [
                    StructField("person_id", LongType(), False),
                    StructField("target_gene_symbol", StringType(), False),
                    StructField("variant_count", IntegerType(), False),
                    StructField("is_carrier", IntegerType(), False),
                    StructField("has_pathogenic_variant", IntegerType(), False),
                ]
            )
            return self.spark.createDataFrame([], empty_schema)

        # Restrict to cohort population if provided
        df_meas = df_measurement
        if df_cohort is not None and df_cohort.limit(1).count() > 0:
            cohort_sub = df_cohort.select(
                col("subject_id" if "subject_id" in df_cohort.columns else "person_id").alias(
                    "person_id"
                )
            ).distinct()
            df_meas = df_meas.join(cohort_sub, on="person_id", how="inner")

        # Resolve gene symbol column or extract from value_source_value
        meas_cols = [c.lower() for c in df_meas.columns]
        if "target_gene_symbol" in meas_cols:
            gene_col = upper(trim(col("target_gene_symbol")))
        elif "gene_symbol" in meas_cols:
            gene_col = upper(trim(col("gene_symbol")))
        elif "value_source_value" in meas_cols:
            # Format: chrom:pos:ref:alt:id:gene_symbol:clinvar_sig (token 6)
            # or INFO GENE=XYZ format
            token_expr = regexp_extract(
                col("value_source_value"),
                r"^[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:([^:]+)",
                1,
            )
            gene_tag_expr = regexp_extract(
                col("value_source_value"),
                r"(?:^|;)GENE=([^;:]+)",
                1,
            )
            extracted_gene = (
                when(token_expr != "", token_expr)
                .when(gene_tag_expr != "", gene_tag_expr)
                .otherwise(lit("UNKNOWN_TARGET"))
            )
            gene_col = upper(trim(extracted_gene))
        else:
            gene_col = lit("UNKNOWN_TARGET")

        # Filter for genomic variant records
        genomic_filter = (col("measurement_concept_id") == self.config.genomic_concept_id) | (
            (col("value_source_value").isNotNull())
            & (
                (
                    regexp_extract(
                        col("value_source_value"), r"^[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:([^:]+)", 1
                    )
                    != ""
                )
                | (regexp_extract(col("value_source_value"), r"(?:^|;)GENE=([^;:]+)", 1) != "")
            )
        )
        df_variants = df_meas.filter(genomic_filter).withColumn("target_gene_symbol", gene_col)

        # Exclude empty or placeholder gene symbols
        df_variants = df_variants.filter(
            (col("target_gene_symbol").isNotNull())
            & (col("target_gene_symbol") != "")
            & (col("target_gene_symbol") != "UNKNOWN_TARGET")
        )

        # Pathogenic variant criteria
        pathogenic_expr = col("value_as_concept_id").isin(
            self.config.pathogenic_concept_ids
        ) | upper(coalesce(col("value_source_value"), lit(""))).contains("PATHOGENIC")

        df_carriers = df_variants.groupBy(
            col("person_id").cast(LongType()),
            col("target_gene_symbol").cast(StringType()),
        ).agg(
            spark_count(lit(1)).cast(IntegerType()).alias("variant_count"),
            spark_max(lit(1)).cast(IntegerType()).alias("is_carrier"),
            spark_max(when(pathogenic_expr, lit(1)).otherwise(lit(0)))
            .cast(IntegerType())
            .alias("has_pathogenic_variant"),
        )

        return df_carriers

    def extract_phenotype_diagnoses(
        self,
        df_condition: DataFrame,
        df_cohort: DataFrame | None = None,
    ) -> DataFrame:
        """
        Extracts distinct condition diagnoses per subject within the cohort population.

        Args:
            df_condition: OMOP CONDITION_OCCURRENCE DataFrame.
            df_cohort: Optional cohort DataFrame.

        Returns:
            DataFrame with columns: [person_id, disease_concept_id].
        """
        if df_condition is None or df_condition.limit(1).count() == 0:
            empty_schema = StructType(
                [
                    StructField("person_id", LongType(), False),
                    StructField("disease_concept_id", LongType(), False),
                ]
            )
            return self.spark.createDataFrame([], empty_schema)

        df_cond = df_condition.filter(
            col("condition_concept_id").isNotNull() & (col("condition_concept_id") > 0)
        )

        if df_cohort is not None and df_cohort.limit(1).count() > 0:
            cohort_sub = df_cohort.select(
                col("subject_id" if "subject_id" in df_cohort.columns else "person_id").alias(
                    "person_id"
                )
            ).distinct()
            df_cond = df_cond.join(cohort_sub, on="person_id", how="inner")

        return df_cond.select(
            col("person_id").cast(LongType()),
            col("condition_concept_id").cast(LongType()).alias("disease_concept_id"),
        ).distinct()

    def compute_biomarker_correlations(
        self,
        df_measurement: DataFrame,
        df_carriers: DataFrame,
    ) -> DataFrame:
        """
        Calculates standardized mean shift (biomarker impact) between variant carriers and wild-types.

        Args:
            df_measurement: OMOP MEASUREMENT DataFrame with quantitative numeric lab values.
            df_carriers: Distinct carriers per target gene.

        Returns:
            DataFrame with columns: [target_gene_symbol, biomarker_correlation].
        """
        empty_schema = StructType(
            [
                StructField("target_gene_symbol", StringType(), False),
                StructField("biomarker_correlation", DoubleType(), False),
            ]
        )
        if (
            df_measurement is None
            or df_measurement.limit(1).count() == 0
            or df_carriers is None
            or df_carriers.limit(1).count() == 0
        ):
            return self.spark.createDataFrame([], empty_schema)

        # Filter for numeric continuous lab observations
        df_numeric = df_measurement.filter(
            col("value_as_number").isNotNull()
            & (col("measurement_concept_id") != self.config.genomic_concept_id)
        ).select(
            col("person_id").cast(LongType()),
            col("value_as_number").cast(DoubleType()),
        )

        if df_numeric.limit(1).count() == 0:
            return self.spark.createDataFrame([], empty_schema)

        # Join carrier status per gene
        joined = df_numeric.join(
            df_carriers.select("person_id", "target_gene_symbol", "is_carrier"),
            on="person_id",
            how="inner",
        )

        # Mean lab values for carriers vs non-carriers across genes
        gene_stats = joined.groupBy("target_gene_symbol").agg(
            spark_round(
                spark_sum(when(col("is_carrier") == 1, col("value_as_number")).otherwise(0.0))
                / greatest(
                    spark_sum(when(col("is_carrier") == 1, lit(1)).otherwise(0)),
                    lit(1),
                ),
                4,
            ).alias("mean_carrier"),
            spark_round(
                spark_sum(when(col("is_carrier") == 0, col("value_as_number")).otherwise(0.0))
                / greatest(
                    spark_sum(when(col("is_carrier") == 0, lit(1)).otherwise(0)),
                    lit(1),
                ),
                4,
            ).alias("mean_ctrl"),
        )

        # Normalized bounded impact metric in [-1.0, 1.0]
        result = (
            gene_stats.withColumn(
                "diff",
                col("mean_carrier") - col("mean_ctrl"),
            )
            .withColumn(
                "denom",
                greatest(col("mean_carrier") + col("mean_ctrl"), lit(1.0)),
            )
            .withColumn(
                "biomarker_correlation",
                spark_round(
                    least(lit(1.0), greatest(lit(-1.0), col("diff") / col("denom"))),
                    4,
                ),
            )
            .select("target_gene_symbol", "biomarker_correlation")
        )

        return result

    def build_target_evidence_mart(
        self,
        df_cohort: DataFrame,
        df_condition: DataFrame,
        df_measurement: DataFrame,
    ) -> DataFrame:
        """
        Constructs the complete Target-to-Phenotype Evidence Mart with Haldane-Anscombe
        Odds Ratios, mutation burdens, 95% CIs, asymptotic p-values, and tractability scores.

        Args:
            df_cohort: Cohort population DataFrame (must contain person_id or subject_id).
            df_condition: OMOP CONDITION_OCCURRENCE DataFrame.
            df_measurement: OMOP MEASUREMENT DataFrame.

        Returns:
            Standardized Target-to-Phenotype Evidence DataFrame adhering to TARGET_EVIDENCE_SCHEMA.
        """
        mlflow_run_id = get_active_mlflow_run_id()

        # Resolve cohort subjects and total cohort denominator N
        person_col = "subject_id" if "subject_id" in df_cohort.columns else "person_id"
        df_subjects = df_cohort.select(
            col(person_col).cast(LongType()).alias("person_id")
        ).distinct()

        if df_subjects.limit(1).count() == 0:
            # Return typed empty DataFrame adhering to TARGET_EVIDENCE_SCHEMA
            return self.spark.createDataFrame([], TARGET_EVIDENCE_SCHEMA)

        total_cohort_size = df_subjects.count()

        # 1. Extract variant carriers
        df_carriers = self.extract_target_variant_carriers(df_measurement, df_cohort=df_subjects)

        # 2. Extract phenotype diagnoses
        df_phenotypes = self.extract_phenotype_diagnoses(df_condition, df_cohort=df_subjects)

        # Early return if either carriers or phenotypes are empty
        if df_carriers.limit(1).count() == 0 or df_phenotypes.limit(1).count() == 0:
            return self.spark.createDataFrame([], TARGET_EVIDENCE_SCHEMA)

        # 3. Gene-level mutation burden and carrier totals
        gene_burden = df_carriers.groupBy("target_gene_symbol").agg(
            spark_count(lit(1)).cast(IntegerType()).alias("total_carriers"),
            spark_round(
                spark_sum("variant_count") / greatest(spark_count(lit(1)), lit(1)),
                4,
            ).alias("target_mutation_burden"),
        )

        # 4. Disease-level case totals
        disease_totals = df_phenotypes.groupBy("disease_concept_id").agg(
            spark_count(lit(1)).cast(IntegerType()).alias("total_cases")
        )

        # 5. Observed co-occurrences (carrier AND disease) -> Cell 'a'
        carrier_disease_pairs = (
            df_carriers.select("person_id", "target_gene_symbol")
            .join(
                df_phenotypes.select("person_id", "disease_concept_id"),
                on="person_id",
                how="inner",
            )
            .groupBy("target_gene_symbol", "disease_concept_id")
            .agg(spark_count(lit(1)).cast(IntegerType()).alias("carrier_cases"))
        )

        # 6. Grid of all evaluated target_gene_symbol x disease_concept_id pairs
        target_genes = df_carriers.select("target_gene_symbol").distinct()
        diseases = df_phenotypes.select("disease_concept_id").distinct()
        cross_grid = target_genes.crossJoin(diseases)

        # Join components together
        mart_raw = (
            cross_grid.join(
                carrier_disease_pairs, on=["target_gene_symbol", "disease_concept_id"], how="left"
            )
            .join(gene_burden, on="target_gene_symbol", how="inner")
            .join(disease_totals, on="disease_concept_id", how="inner")
            .withColumn("carrier_cases", coalesce(col("carrier_cases"), lit(0)).cast(IntegerType()))
            .withColumn("total_cohort_size", lit(total_cohort_size).cast(IntegerType()))
        )

        # 7. Compute 2x2 contingency table cells:
        # a = carrier_cases
        # b = carrier_controls = total_carriers - carrier_cases
        # c = non_carrier_cases = total_cases - carrier_cases
        # d = non_carrier_controls = total_cohort_size - (a + b + c)
        mart_table = (
            mart_raw.withColumn("a", col("carrier_cases"))
            .withColumn(
                "carrier_controls",
                greatest(col("total_carriers") - col("carrier_cases"), lit(0)).cast(IntegerType()),
            )
            .withColumn(
                "non_carrier_cases",
                greatest(col("total_cases") - col("carrier_cases"), lit(0)).cast(IntegerType()),
            )
            .withColumn(
                "non_carrier_controls",
                greatest(
                    col("total_cohort_size")
                    - (
                        col("carrier_cases")
                        + (col("total_carriers") - col("carrier_cases"))
                        + (col("total_cases") - col("carrier_cases"))
                    ),
                    lit(0),
                ).cast(IntegerType()),
            )
        )

        # 8. Haldane-Anscombe continuity correction: +0.5 to each cell
        delta = self.config.haldane_anscombe_correction
        mart_ha = (
            mart_table.withColumn("a_tilde", col("carrier_cases").cast(DoubleType()) + lit(delta))
            .withColumn("b_tilde", col("carrier_controls").cast(DoubleType()) + lit(delta))
            .withColumn("c_tilde", col("non_carrier_cases").cast(DoubleType()) + lit(delta))
            .withColumn("d_tilde", col("non_carrier_controls").cast(DoubleType()) + lit(delta))
        )

        # OR = (a_tilde * d_tilde) / (b_tilde * c_tilde)
        # ln_OR = ln(a_tilde) + ln(d_tilde) - ln(b_tilde) - ln(c_tilde)
        # SE = sqrt(1/a_tilde + 1/b_tilde + 1/c_tilde + 1/d_tilde)
        mart_stats = (
            mart_ha.withColumn(
                "log_odds_ratio",
                spark_round(
                    log(col("a_tilde"))
                    + log(col("d_tilde"))
                    - log(col("b_tilde"))
                    - log(col("c_tilde")),
                    4,
                ),
            )
            .withColumn(
                "odds_ratio",
                spark_round(exp(col("log_odds_ratio")), 4),
            )
            .withColumn(
                "se_log_odds_ratio",
                spark_round(
                    sqrt(
                        (lit(1.0) / col("a_tilde"))
                        + (lit(1.0) / col("b_tilde"))
                        + (lit(1.0) / col("c_tilde"))
                        + (lit(1.0) / col("d_tilde"))
                    ),
                    4,
                ),
            )
            .withColumn(
                "odds_ratio_ci_lower",
                spark_round(
                    exp(
                        col("log_odds_ratio")
                        - lit(self.config.confidence_z) * col("se_log_odds_ratio")
                    ),
                    4,
                ),
            )
            .withColumn(
                "odds_ratio_ci_upper",
                spark_round(
                    exp(
                        col("log_odds_ratio")
                        + lit(self.config.confidence_z) * col("se_log_odds_ratio")
                    ),
                    4,
                ),
            )
        )

        # Asymptotic z-score and two-tailed p-value via Abramowitz & Stegun (7.1.26):
        # z = |ln_OR| / SE
        # t = 1 / (1 + p * z)
        # 1 - Phi(z) = phi(z) * (b1*t + b2*t^2 + b3*t^3 + b4*t^4 + b5*t^5)
        # two_tailed_p = 2 * (1 - Phi(z))
        z_col = spark_abs(col("log_odds_ratio")) / greatest(col("se_log_odds_ratio"), lit(1e-6))
        t_col = lit(1.0) / (lit(1.0) + lit(_P_CONST) * z_col)
        phi_col = lit(_INV_SQRT_2PI) * exp(lit(-0.5) * z_col * z_col)
        poly_col = t_col * (
            lit(_B1)
            + t_col * (lit(_B2) + t_col * (lit(_B3) + t_col * (lit(_B4) + t_col * lit(_B5))))
        )
        p_val_expr = spark_round(
            least(
                lit(1.0),
                greatest(
                    lit(1e-12),
                    lit(2.0) * phi_col * poly_col,
                ),
            ),
            6,
        )

        mart_p = (
            mart_stats.withColumn("z_score", z_col)
            .withColumn("p_value", p_val_expr)
            .withColumn(
                "carrier_frequency",
                spark_round(col("total_carriers") / col("total_cohort_size"), 4),
            )
            .withColumn(
                "phenotype_prevalence",
                spark_round(col("total_cases") / col("total_cohort_size"), 4),
            )
        )

        # 9. Biomarker correlation
        df_biomarkers = self.compute_biomarker_correlations(df_measurement, df_carriers)
        if df_biomarkers.limit(1).count() > 0:
            mart_bio = mart_p.join(df_biomarkers, on="target_gene_symbol", how="left").withColumn(
                "biomarker_correlation",
                coalesce(col("biomarker_correlation"), lit(0.0)).cast(DoubleType()),
            )
        else:
            mart_bio = mart_p.withColumn("biomarker_correlation", lit(0.0).cast(DoubleType()))

        # 10. Target Tractability Score calculation:
        # Genetic score: based on significance (-log10(p)) and effect size (|ln_OR|)
        # Mutation burden score: based on carrier frequency
        # Biomarker score: |biomarker_correlation|
        score_p_expr = least(lit(1.0), -log10(greatest(col("p_value"), lit(1e-10))) / lit(8.0))
        score_or_expr = least(lit(1.0), spark_abs(col("log_odds_ratio")) / lit(3.0))
        score_gen_expr = lit(0.6) * score_p_expr + lit(0.4) * score_or_expr

        score_burden_expr = least(lit(1.0), col("carrier_frequency") * lit(10.0))
        score_bio_expr = spark_abs(col("biomarker_correlation"))

        composite_score = (
            lit(self.config.weight_genetic) * score_gen_expr
            + lit(self.config.weight_burden) * score_burden_expr
            + lit(self.config.weight_biomarker) * score_bio_expr
        )

        mart_scored = mart_bio.withColumn(
            "target_tractability_score",
            spark_round(least(lit(1.0), greatest(lit(0.0), composite_score)), 4),
        )

        # 11. Evidence Tier Assignment
        tier_expr = (
            when(
                (col("p_value") < 0.05) & (col("target_tractability_score") >= 0.50),
                lit("TIER_1_VALIDATED"),
            )
            .when(
                (col("p_value") < 0.20) & (col("target_tractability_score") >= 0.30),
                lit("TIER_2_CANDIDATE"),
            )
            .otherwise(lit("TIER_3_EXPLORATORY"))
        )

        # 12. GxP Traceability Columns
        result = (
            mart_scored.withColumn("evidence_tier", tier_expr)
            .withColumn("created_at", current_timestamp())
            .withColumn("mlflow_run_id", lit(mlflow_run_id))
            .select(
                col("target_gene_symbol").cast(StringType()),
                col("disease_concept_id").cast(LongType()),
                col("carrier_cases").cast(IntegerType()),
                col("carrier_controls").cast(IntegerType()),
                col("non_carrier_cases").cast(IntegerType()),
                col("non_carrier_controls").cast(IntegerType()),
                col("total_cohort_size").cast(IntegerType()),
                col("target_mutation_burden").cast(DoubleType()),
                col("carrier_frequency").cast(DoubleType()),
                col("phenotype_prevalence").cast(DoubleType()),
                col("odds_ratio").cast(DoubleType()),
                col("log_odds_ratio").cast(DoubleType()),
                col("se_log_odds_ratio").cast(DoubleType()),
                col("odds_ratio_ci_lower").cast(DoubleType()),
                col("odds_ratio_ci_upper").cast(DoubleType()),
                col("p_value").cast(DoubleType()),
                col("biomarker_correlation").cast(DoubleType()),
                col("target_tractability_score").cast(DoubleType()),
                col("evidence_tier").cast(StringType()),
                col("created_at").cast(TimestampType()),
                col("mlflow_run_id").cast(StringType()),
            )
        )

        return result

    def persist_target_mart(
        self,
        df_mart: DataFrame,
        base_output_dir: str | None = None,
        mode: str = "overwrite",
    ) -> str:
        """
        Persists the Target Evidence Mart to Delta Lake format with Liquid Clustering
        CLUSTER BY (target_gene_symbol, disease_concept_id) and Change Data Feed enabled.

        Args:
            df_mart: Target Evidence Mart DataFrame.
            base_output_dir: Optional base directory for Delta Lake warehouse.
            mode: Save mode ('overwrite' or 'append').

        Returns:
            Resolved path of the persisted Delta Lake table.
        """
        writer = DeltaMedallionWriter(self.spark, base_output_dir=base_output_dir)
        return writer.write_gold_omop_table(
            df=df_mart,
            table_name=self.config.table_name,
            cluster_by=self.config.cluster_by,
            mode=mode,
            merge_schema=True,
        )


def validate_and_quarantine_target_records(
    df_mart: DataFrame,
    contract_path: str | None = None,
    quarantine_writer: QuarantineDeltaWriter | None = None,
    mlflow_run_id: str | None = None,
    strict_raise: bool = False,
) -> tuple[DataFrame, DataFrame]:
    """
    Validates Target-to-Phenotype Evidence records against declarative GxP contract rules.
    Partitions rows into compliant records and non-compliant records.
    Non-compliant records are routed to dedicated Delta Lake dead-letter sinks
    via `format_quarantine_dataframe` with failure code `TARGET_CONTRACT_VIOLATION`.

    Args:
        df_mart: PySpark DataFrame containing candidate Target Evidence Mart records.
        contract_path: Path to Great Expectations JSON contract specification.
        quarantine_writer: Optional QuarantineDeltaWriter instance to persist dead-letter sinks.
        mlflow_run_id: MLflow run ID for FDA 21 CFR Part 11 audit traceability.
        strict_raise: When True, raises ValueError if any records breach the contract.

    Returns:
        Tuple of (df_valid, df_quarantined).
    """
    run_id = mlflow_run_id or get_active_mlflow_run_id()

    # Invariant failure conditions
    invalid_gene_symbol = (
        col("target_gene_symbol").isNull()
        | (col("target_gene_symbol") == "")
        | (~col("target_gene_symbol").rlike(r"^[A-Z0-9_-]{2,15}$"))
    )
    invalid_disease_concept = col("disease_concept_id").isNull() | (col("disease_concept_id") <= 0)
    invalid_odds_ratio = (
        col("odds_ratio").isNull() | (col("odds_ratio") <= 0.0) | (col("odds_ratio") > 10000.0)
    )
    invalid_tractability = (
        col("target_tractability_score").isNull()
        | (col("target_tractability_score") < 0.0)
        | (col("target_tractability_score") > 1.0)
    )
    invalid_p_value = col("p_value").isNull() | (col("p_value") < 0.0) | (col("p_value") > 1.0)
    invalid_biomarker = col("biomarker_correlation").isNotNull() & (
        (col("biomarker_correlation") < -1.0) | (col("biomarker_correlation") > 1.0)
    )
    invalid_cohort_size = col("total_cohort_size").isNull() | (col("total_cohort_size") < 1)

    breach_expr = (
        invalid_gene_symbol
        | invalid_disease_concept
        | invalid_odds_ratio
        | invalid_tractability
        | invalid_p_value
        | invalid_biomarker
        | invalid_cohort_size
    )

    reason_expr = (
        when(invalid_gene_symbol, lit("Malformed or missing HGNC canonical gene symbol"))
        .when(invalid_disease_concept, lit("Missing or non-positive OMOP disease concept ID"))
        .when(invalid_odds_ratio, lit("Odds ratio out of biologically plausible bounds"))
        .when(
            invalid_tractability, lit("Target tractability score outside unit interval [0.0, 1.0]")
        )
        .when(invalid_p_value, lit("P-value outside valid probability bounds [0.0, 1.0]"))
        .when(invalid_biomarker, lit("Biomarker correlation outside valid interval [-1.0, 1.0]"))
        .when(invalid_cohort_size, lit("Total cohort denominator must be at least 1"))
        .otherwise(lit("Unknown semantic contract breach"))
    )

    df_valid = df_mart.filter(~breach_expr)
    df_breaches = df_mart.filter(breach_expr)

    # Transform breaches into standardized GxP dead-letter quarantine rows
    df_quarantine = format_quarantine_dataframe(
        df=df_breaches,
        table_name=QUARANTINE_TABLE_TARGETS,
        failure_code=ClinicalFailureCode.TARGET_CONTRACT_VIOLATION,
        failure_reason=reason_expr,
        mlflow_run_id=run_id,
    )

    # Persist quarantine records if writer supplied
    if quarantine_writer is not None and df_breaches.limit(1).count() > 0:
        quarantine_writer.write_quarantine_sink(
            df=df_quarantine,
            table_name=QUARANTINE_TABLE_TARGETS,
            mode="append",
        )

    # Evaluate declarative Great Expectations contract if provided
    resolved_contract = contract_path or "governance/contracts/target_contract.json"
    if os.path.exists(resolved_contract):
        try:
            evaluate_data_contract(
                df=df_valid,
                rules_path=resolved_contract,
                experiment_name="gxp_target_discovery_governance",
                strict=strict_raise,
            )
        except (ValueError, KeyError, OSError, RuntimeError) as e:
            logger.warning("[GxP WARNING] Great Expectations contract evaluation note: %s", e)
            if strict_raise:
                raise

    if strict_raise and df_breaches.limit(1).count() > 0:
        raise ValueError(
            f"Target Evidence Mart contract violation: {df_breaches.count()} records breached invariants."
        )

    return df_valid, df_quarantine
