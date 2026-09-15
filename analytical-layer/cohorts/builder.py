"""
Module: builder.py
Description: Configurable OHDSI Phenotyping Engine and Cohort Builder for PySpark OMOP CDM v5.4.
             Translates declarative phenotyping rules (index events, continuous observation lookbacks,
             demographic parameters, clinical exclusions, biomarker cutoffs, genomic variant status)
             into standardized OHDSI COHORT relational tables with Delta Lake Liquid Clustering.
Author: Vivi Tsoumaki
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_add,
    datediff,
    lit,
    to_date,
    upper,
    year,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from omop_cdm_v54.compat import HAS_DELTA

try:
    from medallion.writer import DeltaMedallionWriter
except ImportError:
    DeltaMedallionWriter = None  # type: ignore[assignment, misc]


# Standard OHDSI COHORT Table Schema
COHORT_SCHEMA: StructType = StructType(
    [
        StructField("cohort_definition_id", LongType(), nullable=False),
        StructField("subject_id", LongType(), nullable=False),
        StructField("cohort_start_date", DateType(), nullable=False),
        StructField("cohort_end_date", DateType(), nullable=False),
    ]
)


@dataclass
class CohortCriteria:
    """Declarative phenotyping criteria defining an OHDSI analytical cohort."""

    # Primary Index Event (T0) Selection
    index_condition_concept_ids: list[int] = field(default_factory=list)
    index_measurement_concept_ids: list[int] = field(default_factory=list)
    index_event_selection: Literal["FIRST", "LAST"] = "FIRST"

    # Observation Windows (days)
    prior_observation_days: int = 0
    post_observation_days: int = 0
    default_follow_up_days: int = 365

    # Demographic Criteria
    min_age_at_index: int | None = None
    max_age_at_index: int | None = None
    gender_concept_ids: list[int] = field(default_factory=list)

    # Clinical Exclusion Criteria (prior historical conditions before T0)
    exclusion_condition_concept_ids: list[int] = field(default_factory=list)
    exclusion_lookback_days: int | None = None  # None = all prior history

    # Baseline Biomarker Criteria (lookback window [T0 - lookback, T0])
    biomarker_concept_id: int | None = None
    biomarker_min_value: float | None = None
    biomarker_max_value: float | None = None
    biomarker_lookback_days: int = 365

    # Genomic Variant Multi-Omics Criteria
    require_pathogenic_variant: bool = False
    genomic_concept_id: int = 35917873  # Standard OMOP genomic variant concept ID


@dataclass
class CohortDefinition:
    """Named specification of an OHDSI phenotyping cohort."""

    cohort_definition_id: int
    name: str
    description: str
    criteria: CohortCriteria


class OHDSICohortBuilder:
    """Configurable OHDSI Phenotyping Engine executing on OMOP CDM v5.4 relational DataFrames."""

    def __init__(self, spark: SparkSession):
        """Initializes the cohort builder with an active SparkSession.

        Args:
            spark: Active PySpark SparkSession.
        """
        self.spark = spark

    def build_cohort(
        self,
        definition: CohortDefinition,
        df_person: DataFrame,
        df_condition: DataFrame | None = None,
        df_measurement: DataFrame | None = None,
        study_end_date: str | None = None,
        df_condition_occurrence: DataFrame | None = None,
    ) -> DataFrame:
        """Constructs an OHDSI COHORT table DataFrame based on declarative criteria.

        Args:
            definition: CohortDefinition containing selection and exclusion rules.
            df_person: Gold-tier OMOP CDM PERSON DataFrame.
            df_condition: Gold-tier OMOP CDM CONDITION_OCCURRENCE DataFrame.
            df_measurement: Gold-tier OMOP CDM MEASUREMENT DataFrame.
            study_end_date: Optional fixed study termination date string (YYYY-MM-DD).
            df_condition_occurrence: Legacy alias for ``df_condition``; prefer ``df_condition``.
                When both are provided, ``df_condition`` takes precedence.

        Returns:
            PySpark DataFrame conforming to COHORT_SCHEMA.
        """
        df_cond = df_condition if df_condition is not None else df_condition_occurrence
        if df_cond is None:
            # Use a minimal CONDITION_OCCURRENCE schema so that all downstream column
            # references (condition_concept_id, condition_start_date) resolve safely
            # without raising AnalysisException on an unrelated PERSON schema.
            df_cond = self.spark.createDataFrame(
                [],
                StructType(
                    [
                        StructField("person_id", LongType(), nullable=False),
                        StructField("condition_concept_id", LongType(), nullable=True),
                        StructField("condition_start_date", StringType(), nullable=True),
                    ]
                ),
            )
        if df_measurement is None:
            # Use a minimal MEASUREMENT schema so that measurement_concept_id and
            # value_* column references resolve safely without an AnalysisException.
            df_measurement = self.spark.createDataFrame(
                [],
                StructType(
                    [
                        StructField("person_id", LongType(), nullable=False),
                        StructField("measurement_concept_id", LongType(), nullable=True),
                        StructField("measurement_date", StringType(), nullable=True),
                        StructField("value_as_number", LongType(), nullable=True),
                        StructField("value_as_concept_id", LongType(), nullable=True),
                        StructField("value_source_value", StringType(), nullable=True),
                    ]
                ),
            )

        crit = definition.criteria

        # ---------------------------------------------------------------------
        # 1. Identify Index Event Candidates (T0)
        # ---------------------------------------------------------------------
        candidate_dfs: list[DataFrame] = []

        if crit.index_condition_concept_ids and "condition_concept_id" in df_cond.columns:
            cond_candidates = (
                df_cond.filter(col("condition_concept_id").isin(crit.index_condition_concept_ids))
                .select(
                    col("person_id").alias("subject_id"),
                    to_date(col("condition_start_date")).alias("index_date"),
                )
                .filter(col("index_date").isNotNull())
            )
            candidate_dfs.append(cond_candidates)

        if (
            crit.index_measurement_concept_ids
            and "measurement_concept_id" in df_measurement.columns
        ):
            meas_candidates = (
                df_measurement.filter(
                    col("measurement_concept_id").isin(crit.index_measurement_concept_ids)
                )
                .select(
                    col("person_id").alias("subject_id"),
                    to_date(col("measurement_date")).alias("index_date"),
                )
                .filter(col("index_date").isNotNull())
            )
            candidate_dfs.append(meas_candidates)

        if not candidate_dfs:
            return self.spark.createDataFrame([], COHORT_SCHEMA)

        df_candidates = candidate_dfs[0]
        for c_df in candidate_dfs[1:]:
            df_candidates = df_candidates.unionByName(c_df)

        agg_func = spark_min if crit.index_event_selection == "FIRST" else spark_max
        df_index = df_candidates.groupBy("subject_id").agg(
            agg_func("index_date").alias("cohort_start_date")
        )

        # ---------------------------------------------------------------------
        # 2. Demographic Criteria Evaluation
        # ---------------------------------------------------------------------
        df_cohort = df_index.join(
            df_person.select(
                col("person_id").alias("p_id"),
                col("year_of_birth"),
                col("gender_concept_id"),
                to_date(col("birth_datetime")).alias("birth_date"),
            ),
            df_index["subject_id"] == col("p_id"),
            how="inner",
        ).drop("p_id")

        if crit.gender_concept_ids:
            df_cohort = df_cohort.filter(col("gender_concept_id").isin(crit.gender_concept_ids))

        if crit.min_age_at_index is not None or crit.max_age_at_index is not None:
            # Approximate age at index date from year_of_birth
            df_cohort = df_cohort.withColumn(
                "age_at_index",
                year(col("cohort_start_date")) - col("year_of_birth"),
            )
            if crit.min_age_at_index is not None:
                df_cohort = df_cohort.filter(col("age_at_index") >= crit.min_age_at_index)
            if crit.max_age_at_index is not None:
                df_cohort = df_cohort.filter(col("age_at_index") <= crit.max_age_at_index)
            df_cohort = df_cohort.drop("age_at_index")

        # ---------------------------------------------------------------------
        # 3. Continuous Prior Observation Lookback Window
        # ---------------------------------------------------------------------
        if crit.prior_observation_days > 0:
            # Calculate patient-level earliest baseline observation date from conditions or measurements
            cond_starts = (
                df_cond.groupBy("person_id")
                .agg(spark_min(to_date(col("condition_start_date"))).alias("earliest_cond"))
                .withColumnRenamed("person_id", "obs_person_id")
            )
            df_cohort = df_cohort.join(
                cond_starts,
                df_cohort["subject_id"] == col("obs_person_id"),
                how="left",
            ).drop("obs_person_id")

            df_cohort = df_cohort.filter(
                (col("earliest_cond").isNotNull())
                & (
                    datediff(col("cohort_start_date"), col("earliest_cond"))
                    >= crit.prior_observation_days
                )
            ).drop("earliest_cond")

        # ---------------------------------------------------------------------
        # 4. Clinical Exclusion Criteria (prior conditions before T0)
        # ---------------------------------------------------------------------
        if crit.exclusion_condition_concept_ids:
            excl_conds = df_cond.filter(
                col("condition_concept_id").isin(crit.exclusion_condition_concept_ids)
            ).select(
                col("person_id").alias("excl_subj_id"),
                to_date(col("condition_start_date")).alias("excl_date"),
            )

            cond_join = df_cohort.join(
                excl_conds,
                df_cohort["subject_id"] == col("excl_subj_id"),
                how="inner",
            )

            if crit.exclusion_lookback_days is not None:
                violators = (
                    cond_join.filter(
                        (col("excl_date") <= col("cohort_start_date"))
                        & (
                            datediff(col("cohort_start_date"), col("excl_date"))
                            <= crit.exclusion_lookback_days
                        )
                    )
                    .select("subject_id")
                    .distinct()
                )
            else:
                violators = (
                    cond_join.filter(col("excl_date") <= col("cohort_start_date"))
                    .select("subject_id")
                    .distinct()
                )

            df_cohort = df_cohort.join(violators, on="subject_id", how="left_anti")

        # ---------------------------------------------------------------------
        # 5. Baseline Biomarker Inclusion Criteria
        # ---------------------------------------------------------------------
        if crit.biomarker_concept_id is not None:
            bio_meas = df_measurement.filter(
                (col("measurement_concept_id") == crit.biomarker_concept_id)
                & col("value_as_number").isNotNull()
            ).select(
                col("person_id").alias("bio_subj_id"),
                to_date(col("measurement_date")).alias("bio_date"),
                col("value_as_number").alias("bio_val"),
            )

            bio_join = df_cohort.join(
                bio_meas,
                df_cohort["subject_id"] == col("bio_subj_id"),
                how="inner",
            ).filter(
                (col("bio_date") <= col("cohort_start_date"))
                & (
                    datediff(col("cohort_start_date"), col("bio_date"))
                    <= crit.biomarker_lookback_days
                )
            )

            if crit.biomarker_min_value is not None:
                bio_join = bio_join.filter(col("bio_val") >= crit.biomarker_min_value)
            if crit.biomarker_max_value is not None:
                bio_join = bio_join.filter(col("bio_val") <= crit.biomarker_max_value)

            qualifying_bio_subjs = bio_join.select("subject_id").distinct()
            df_cohort = df_cohort.join(qualifying_bio_subjs, on="subject_id", how="inner")

        # ---------------------------------------------------------------------
        # 6. Multi-Omics Genomic Variant Criteria
        # ---------------------------------------------------------------------
        if crit.require_pathogenic_variant:
            # Filter on the OMOP genomic measurement concept (crit.genomic_concept_id)
            # and confirm pathogenicity via free-text value_source_value containing
            # "PATHOGENIC" *or* via value_as_concept_id == 35917873, which is the
            # standard OMOP concept for "Pathogenic" ClinVar clinical significance.
            # Note: 35917873 here is the *pathogenicity classification concept*, not
            # the same as crit.genomic_concept_id (the measurement type concept).
            genomic_carriers = (
                df_measurement.filter(
                    (col("measurement_concept_id") == crit.genomic_concept_id)
                    & (
                        upper(col("value_source_value")).contains("PATHOGENIC")
                        | (col("value_as_concept_id") == 35917873)
                    )
                )
                .select(col("person_id").alias("subject_id"))
                .distinct()
            )
            df_cohort = df_cohort.join(genomic_carriers, on="subject_id", how="inner")

        # ---------------------------------------------------------------------
        # 7. Cohort End Date Derivation & Final Projection
        # ---------------------------------------------------------------------
        if study_end_date is not None:
            default_end_expr = to_date(lit(study_end_date))
        else:
            default_end_expr = date_add(col("cohort_start_date"), crit.default_follow_up_days)

        df_cohort_final = (
            df_cohort.withColumn("cohort_end_date", default_end_expr)
            .select(
                lit(definition.cohort_definition_id).cast(LongType()).alias("cohort_definition_id"),
                col("subject_id").cast(LongType()).alias("subject_id"),
                col("cohort_start_date").cast(DateType()).alias("cohort_start_date"),
                col("cohort_end_date").cast(DateType()).alias("cohort_end_date"),
            )
            .distinct()
        )

        return df_cohort_final

    def save_cohort(
        self,
        df_cohort: DataFrame,
        base_output_dir: str,
        mode: str = "overwrite",
    ) -> str:
        """Persists the OHDSI COHORT table to Delta Lake with Liquid Clustering.

        Args:
            df_cohort: DataFrame conforming to COHORT_SCHEMA.
            base_output_dir: Root storage directory.
            mode: Write mode ('overwrite' or 'append').

        Returns:
            Destination path string where the cohort table was saved.
        """
        target_path = os.path.join(base_output_dir, "cohort")

        _log = logging.getLogger(__name__)

        if HAS_DELTA and DeltaMedallionWriter is not None:
            try:
                (
                    df_cohort.write.format("delta")
                    .mode(mode)
                    .option("mergeSchema", "true")
                    .clusterBy("cohort_definition_id", "subject_id")
                    .save(target_path)
                )
                _log.info(
                    "[COHORT SINK] Successfully persisted COHORT table to Delta Lake: %s",
                    target_path,
                )
                return target_path
            except Exception as delta_err:
                _log.warning(
                    "[COHORT SINK] Delta Liquid Clustering write encountered error: %s. "
                    "Falling back to Parquet.",
                    delta_err,
                )

        # Fallback to standard Parquet persistence for environments without Delta/Liquid Clustering.
        os.makedirs(target_path, exist_ok=True)
        df_cohort.write.mode(mode).parquet(target_path)
        _log.info("[COHORT SINK] Persisted COHORT table as Parquet: %s", target_path)
        return target_path


# -----------------------------------------------------------------------------
# Out-of-the-box Reference Phenotypes
# -----------------------------------------------------------------------------
def get_type_2_diabetes_cohort_definition(
    cohort_id: int = 1001,
    min_age: int = 18,
    require_hba1c_elevated: bool = False,
) -> CohortDefinition:
    """Constructs an OHDSI cohort definition for Type 2 Diabetes Mellitus.

    Args:
        cohort_id: Unique definition identifier (default 1001).
        min_age: Minimum age at index (default 18).
        require_hba1c_elevated: If True, requires baseline HbA1c >= 7.0%.

    Returns:
        CohortDefinition instance.
    """
    crit = CohortCriteria(
        index_condition_concept_ids=[201826],  # SNOMED 201826: Type 2 diabetes mellitus
        min_age_at_index=min_age,
        prior_observation_days=0,
        default_follow_up_days=365,
    )
    if require_hba1c_elevated:
        crit.biomarker_concept_id = 3004410  # LOINC 4548-4: HbA1c
        crit.biomarker_min_value = 7.0

    return CohortDefinition(
        cohort_definition_id=cohort_id,
        name="Type 2 Diabetes Mellitus Cohort",
        description="Adult patients diagnosed with Type 2 Diabetes Mellitus (SNOMED 201826).",
        criteria=crit,
    )


def get_hypertension_cohort_definition(
    cohort_id: int = 1002,
    min_age: int = 18,
    exclude_prior_mi: bool = False,
) -> CohortDefinition:
    """Constructs an OHDSI cohort definition for Essential Hypertension.

    Args:
        cohort_id: Unique definition identifier (default 1002).
        min_age: Minimum age at index (default 18).
        exclude_prior_mi: If True, excludes patients with prior Acute Myocardial Infarction.

    Returns:
        CohortDefinition instance.
    """
    crit = CohortCriteria(
        index_condition_concept_ids=[316866],  # SNOMED 316866: Essential hypertension
        min_age_at_index=min_age,
        default_follow_up_days=365,
    )
    if exclude_prior_mi:
        crit.exclusion_condition_concept_ids = [
            4329847
        ]  # SNOMED 4329847: Acute Myocardial Infarction

    return CohortDefinition(
        cohort_definition_id=cohort_id,
        name="Essential Hypertension Cohort",
        description="Adult patients with Essential Primary Hypertension (SNOMED 316866).",
        criteria=crit,
    )


def get_genomic_oncology_cohort_definition(
    cohort_id: int = 1003,
) -> CohortDefinition:
    """Constructs an OHDSI cohort definition for Malignant Neoplasm with Pathogenic ClinVar variants.

    Args:
        cohort_id: Unique definition identifier (default 1003).

    Returns:
        CohortDefinition instance.
    """
    crit = CohortCriteria(
        index_condition_concept_ids=[
            254637
        ],  # SNOMED 254637: Malignant neoplasm of bronchus and lung
        require_pathogenic_variant=True,
        default_follow_up_days=730,
    )
    return CohortDefinition(
        cohort_definition_id=cohort_id,
        name="Genomic Lung Oncology Cohort",
        description="Patients with Malignant Neoplasm of Lung and confirmed pathogenic ClinVar mutations.",
        criteria=crit,
    )
