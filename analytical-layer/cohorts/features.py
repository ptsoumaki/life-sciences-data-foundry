"""
Module: features.py
Description: ML-Ready Patient Feature Store Projections & Charlson Comorbidity Index (CCI).
             Transforms longitudinal OMOP CDM v5.4 clinical events, baseline biomarker panels,
             and multi-omics ClinVar genomic variants into wide, numerically-encoded,
             scikit-learn and XGBoost-ready patient feature matrices anchored around index date T0.
Author: Vivi Tsoumaki
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    avg as spark_avg,
)
from pyspark.sql.functions import (
    coalesce,
    col,
    countDistinct,
    datediff,
    lit,
    row_number,
    to_date,
    upper,
    when,
    year,
)
from pyspark.sql.functions import (
    count as spark_count,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.functions import (
    round as spark_round,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)
from pyspark.sql.utils import AnalysisException

from omop_cdm_v54.compat import HAS_DELTA

try:
    from medallion.writer import DeltaMedallionWriter
except ImportError:
    DeltaMedallionWriter = None  # type: ignore[assignment, misc]


logger = logging.getLogger(__name__)


# Standard Charlson Comorbidity Categories, Weights, and Primary SNOMED Concept Codes
CHARLSON_CATEGORIES: dict[str, dict[str, Any]] = {
    "myocardial_infarction": {
        "weight": 1,
        "concept_ids": [4329847, 312327, 434376],  # Acute myocardial infarction, Old MI
    },
    "congestive_heart_failure": {
        "weight": 1,
        "concept_ids": [316139, 319835, 314378],  # Heart failure, Congestive heart failure
    },
    "peripheral_vascular": {
        "weight": 1,
        "concept_ids": [321052, 318790, 4170889],  # Peripheral vascular disease
    },
    "cerebrovascular": {
        "weight": 1,
        "concept_ids": [381591, 443454, 437677],  # Cerebrovascular disease, Stroke
    },
    "dementia": {
        "weight": 1,
        "concept_ids": [4182210, 43530671, 374888],  # Dementia, Alzheimer's disease
    },
    "chronic_pulmonary": {
        "weight": 1,
        "concept_ids": [255573, 40488964, 317009],  # Chronic obstructive pulmonary disease
    },
    "connective_tissue": {
        "weight": 1,
        "concept_ids": [257628, 80809, 437233],  # Rheumatoid arthritis, SLE
    },
    "peptic_ulcer": {
        "weight": 1,
        "concept_ids": [4027663, 4247120, 4028741],  # Peptic ulcer disease
    },
    "mild_liver": {
        "weight": 1,
        "concept_ids": [
            4212540,
            4064161,
            4296766,
        ],  # Chronic hepatitis, Cirrhosis without portal hypertension
    },
    "diabetes_uncomplicated": {
        "weight": 1,
        "concept_ids": [201820, 201254, 4014295],  # Type 2 diabetes without complication
    },
    "diabetes_complicated": {
        "weight": 2,
        "concept_ids": [
            443767,
            443734,
            4193704,
        ],  # Diabetes with renal, neurological, or ophthalmic manifestations
    },
    "hemiplegia": {
        "weight": 2,
        "concept_ids": [374022, 374020, 4148906],  # Hemiplegia or paraplegia
    },
    "renal_disease": {
        "weight": 2,
        "concept_ids": [4030518, 197500, 443611],  # Chronic kidney disease, Renal failure
    },
    "any_malignancy": {
        "weight": 2,
        "concept_ids": [254637, 443392, 432851],  # Malignant neoplasms, Lymphoma, Leukemia
    },
    "moderate_severe_liver": {
        "weight": 3,
        "concept_ids": [4245975, 4172024, 4064162],  # Hepatic failure, Portal hypertension
    },
    "metastatic_tumor": {
        "weight": 6,
        "concept_ids": [4205430, 432851, 4177285],  # Secondary and metastatic malignant neoplasms
    },
    "aids_hiv": {
        "weight": 6,
        "concept_ids": [439727, 438986, 440383],  # Human immunodeficiency virus infection
    },
}

# Standard Baseline Biomarker Concepts aligned with governance/concept_mappings.json
DEFAULT_BIOMARKER_MAP: dict[str, int] = {
    "hba1c": 3004410,  # LOINC 4548-4 (Hemoglobin A1c)
    "glucose": 3000483,  # LOINC 2345-7 (Serum Glucose)
    "cholesterol": 3004249,  # LOINC 2093-3 (Total Cholesterol)
    "creatinine": 3016723,  # LOINC 2160-0 (Serum Creatinine)
}


@dataclass
class FeatureStoreConfig:
    """Configuration options for patient feature store projection."""

    lookback_windows_days: list[int] = field(default_factory=lambda: [30, 180, 365])
    biomarkers: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_BIOMARKER_MAP))
    genomic_concept_id: int = 35917873  # Standard OMOP genomic variant concept ID
    impute_missing_biomarkers_with_zero: bool = True


class PatientFeatureStore:
    """Enterprise Patient Feature Store builder.

    Generates dense, machine-learning-ready patient representations incorporating:
    1. Weighted Charlson Comorbidity Index (CCI) with standard hierarchical suppression.
    2. Multi-window rolling condition counts (30d, 180d, 365d, lifetime).
    3. Longitudinal baseline biomarker aggregations (latest, mean, min, max, missing indicators).
    4. ClinVar genomic variant embeddings (pathogenic variant indicator and count).
    5. Demographic covariates (age at index, sex flags).
    """

    def __init__(self, spark: SparkSession, config: FeatureStoreConfig | None = None) -> None:
        self.spark = spark
        self.config = config or FeatureStoreConfig()

    def build_feature_matrix(
        self,
        df_cohort: DataFrame,
        df_person: DataFrame,
        df_condition_occurrence: DataFrame | None = None,
        df_measurement: DataFrame | None = None,
    ) -> DataFrame:
        """Constructs a wide, ML-ready feature matrix anchored at cohort_start_date (T0).

        Args:
            df_cohort: OHDSI COHORT DataFrame (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date).
            df_person: OMOP PERSON DataFrame (person_id, gender_concept_id, year_of_birth).
            df_condition_occurrence: Optional OMOP CONDITION_OCCURRENCE DataFrame.
            df_measurement: Optional OMOP MEASUREMENT DataFrame.

        Returns:
            Dense, wide DataFrame indexed by (cohort_definition_id, subject_id, cohort_start_date).
        """
        if df_cohort.limit(1).count() == 0:
            return self._build_empty_feature_matrix()

        # 1. Base Demographic Features
        df_features = (
            df_cohort.select(
                col("cohort_definition_id").cast(LongType()),
                col("subject_id").cast(LongType()),
                to_date(col("cohort_start_date")).alias("cohort_start_date"),
                to_date(col("cohort_end_date")).alias("cohort_end_date"),
            )
            .join(
                df_person.select(
                    col("person_id").cast(LongType()).alias("subject_id"),
                    col("gender_concept_id").cast(LongType()),
                    col("year_of_birth").cast(IntegerType()),
                ),
                on="subject_id",
                how="left",
            )
            .withColumn(
                "age_at_index",
                (year(col("cohort_start_date")) - col("year_of_birth")).cast(IntegerType()),
            )
            .withColumn(
                "is_female",
                when(col("gender_concept_id") == 8532, lit(1))
                .otherwise(lit(0))
                .cast(IntegerType()),
            )
            .withColumn(
                "is_male",
                when(col("gender_concept_id") == 8507, lit(1))
                .otherwise(lit(0))
                .cast(IntegerType()),
            )
        )

        # 2. Charlson Comorbidity Index & Historical Category Flags
        df_features = self._attach_charlson_and_condition_counts(
            df_features, df_condition_occurrence
        )

        # 3. Baseline Biomarker Aggregations
        df_features = self._attach_biomarker_features(df_features, df_measurement)

        # 4. Multi-Omics ClinVar Genomic Embeddings
        df_features = self._attach_genomic_features(df_features, df_measurement)

        return df_features

    def _attach_charlson_and_condition_counts(
        self,
        df_features: DataFrame,
        df_condition_occurrence: DataFrame | None,
    ) -> DataFrame:
        """Computes Charlson Comorbidity Index with hierarchy rules and rolling condition counts.

        Args:
            df_features: Feature DataFrame anchored at cohort_start_date (T0), already containing
                demographic columns.
            df_condition_occurrence: Optional OMOP CONDITION_OCCURRENCE DataFrame.

        Returns:
            ``df_features`` extended with CCI category flags, ``charlson_comorbidity_index``,
            and rolling condition count columns for each configured lookback window.
        """
        if df_condition_occurrence is None or df_condition_occurrence.limit(1).count() == 0:
            # Attach zeros for all CCI categories, total CCI, and condition counts.
            df_res = df_features
            for cat in CHARLSON_CATEGORIES:
                df_res = df_res.withColumn(f"cci_{cat}", lit(0).cast(IntegerType()))
            df_res = df_res.withColumn("charlson_comorbidity_index", lit(0).cast(IntegerType()))
            for days in self.config.lookback_windows_days:
                df_res = df_res.withColumn(f"condition_count_{days}d", lit(0).cast(IntegerType()))
            df_res = df_res.withColumn("condition_count_lifetime", lit(0).cast(IntegerType()))
            df_res = df_res.withColumn("distinct_condition_count_365d", lit(0).cast(IntegerType()))
            return df_res

        # Filter conditions occurring on or before cohort_start_date (T0)
        episode_keys = ["cohort_definition_id", "subject_id", "cohort_start_date"]
        cond_prior = (
            df_condition_occurrence.select(
                col("person_id").cast(LongType()).alias("subject_id"),
                col("condition_concept_id").cast(IntegerType()),
                to_date(col("condition_start_date")).alias("condition_start_date"),
            )
            .join(
                df_features.select(*episode_keys).distinct(),
                on="subject_id",
                how="inner",
            )
            .filter(col("condition_start_date") <= col("cohort_start_date"))
            .withColumn(
                "days_prior_to_index",
                datediff(col("cohort_start_date"), col("condition_start_date")),
            )
        )

        # Aggregate rolling condition counts driven by the configured lookback windows.
        # This ensures column names match the empty-cohort schema produced by
        # _build_empty_feature_matrix(), preventing downstream schema divergence.
        window_aggs = [
            spark_sum(when(col("days_prior_to_index") <= days, lit(1)).otherwise(lit(0))).alias(
                f"condition_count_{days}d"
            )
            for days in self.config.lookback_windows_days
        ]
        cond_counts = cond_prior.groupBy(*episode_keys).agg(
            *window_aggs,
            countDistinct(
                when(col("days_prior_to_index") <= 365, col("condition_concept_id")).otherwise(
                    lit(None)
                )
            ).alias("distinct_condition_count_365d"),
            spark_count(lit(1)).alias("condition_count_lifetime"),
        )

        fill_defaults: dict[str, bool | float | int | str] = {
            "distinct_condition_count_365d": 0,
            "condition_count_lifetime": 0,
        }
        for days in self.config.lookback_windows_days:
            fill_defaults[f"condition_count_{days}d"] = 0

        df_joined = df_features.join(cond_counts, on=episode_keys, how="left").fillna(fill_defaults)

        # Build indicators for each Charlson category
        cat_aggs = []
        for cat_name, cat_meta in CHARLSON_CATEGORIES.items():
            cat_concepts = cat_meta["concept_ids"]
            cat_aggs.append(
                spark_max(
                    when(col("condition_concept_id").isin(cat_concepts), lit(1)).otherwise(lit(0))
                ).alias(f"_raw_cci_{cat_name}")
            )

        df_cci_raw = cond_prior.groupBy(*episode_keys).agg(*cat_aggs)
        df_with_cci = df_joined.join(df_cci_raw, on=episode_keys, how="left")

        # Clean nulls to 0 for raw indicators
        for cat_name in CHARLSON_CATEGORIES:
            df_with_cci = df_with_cci.withColumn(
                f"_raw_cci_{cat_name}",
                coalesce(col(f"_raw_cci_{cat_name}"), lit(0)).cast(IntegerType()),
            )

        # Apply Standard Charlson / Quan Hierarchical Exclusions:
        # 1. Diabetes: If complicated (wt 2), uncomplicated (wt 1) = 0
        has_diab_comp = col("_raw_cci_diabetes_complicated") == 1
        adj_diab_uncomp = when(has_diab_comp, lit(0)).otherwise(
            col("_raw_cci_diabetes_uncomplicated")
        )

        # 2. Liver: If moderate/severe (wt 3), mild liver (wt 1) = 0
        has_mod_liver = col("_raw_cci_moderate_severe_liver") == 1
        adj_mild_liver = when(has_mod_liver, lit(0)).otherwise(col("_raw_cci_mild_liver"))

        # 3. Malignancy: If metastatic solid tumor (wt 6), any malignancy (wt 2) = 0
        has_metastatic = col("_raw_cci_metastatic_tumor") == 1
        adj_malignancy = when(has_metastatic, lit(0)).otherwise(col("_raw_cci_any_malignancy"))

        # Project public category flags and compute weighted sum
        cci_weight_terms = []
        for cat_name, cat_meta in CHARLSON_CATEGORIES.items():
            wt = cat_meta["weight"]
            if cat_name == "diabetes_uncomplicated":
                final_flag = adj_diab_uncomp
            elif cat_name == "mild_liver":
                final_flag = adj_mild_liver
            elif cat_name == "any_malignancy":
                final_flag = adj_malignancy
            else:
                final_flag = col(f"_raw_cci_{cat_name}")

            df_with_cci = df_with_cci.withColumn(f"cci_{cat_name}", final_flag.cast(IntegerType()))
            cci_weight_terms.append(col(f"cci_{cat_name}") * lit(wt))

        # Sum all weighted components
        total_cci_expr = cci_weight_terms[0]
        for term in cci_weight_terms[1:]:
            total_cci_expr = total_cci_expr + term

        df_final = df_with_cci.withColumn(
            "charlson_comorbidity_index", total_cci_expr.cast(IntegerType())
        ).drop(*[f"_raw_cci_{c}" for c in CHARLSON_CATEGORIES])

        return df_final

    def _attach_biomarker_features(
        self,
        df_features: DataFrame,
        df_measurement: DataFrame | None,
    ) -> DataFrame:
        """Computes baseline numerical biomarker metrics (latest, mean, min, max) and missingness flags.

        Args:
            df_features: Feature DataFrame anchored at cohort_start_date (T0).
            df_measurement: Optional OMOP MEASUREMENT DataFrame.

        Returns:
            ``df_features`` extended with ``latest_<bio>``, ``mean_<bio>_365d``,
            ``min_<bio>_365d``, ``max_<bio>_365d``, and ``is_missing_<bio>`` columns
            for each configured biomarker.
        """
        df_res = df_features

        if df_measurement is None or df_measurement.limit(1).count() == 0:
            for bio_name in self.config.biomarkers:
                df_res = (
                    df_res.withColumn(f"latest_{bio_name}", lit(0.0).cast(DoubleType()))
                    .withColumn(f"mean_{bio_name}_365d", lit(0.0).cast(DoubleType()))
                    .withColumn(f"min_{bio_name}_365d", lit(0.0).cast(DoubleType()))
                    .withColumn(f"max_{bio_name}_365d", lit(0.0).cast(DoubleType()))
                    .withColumn(f"is_missing_{bio_name}", lit(1).cast(IntegerType()))
                )
            return df_res

        # Measurement table filtered to values prior to or on index date
        episode_keys = ["cohort_definition_id", "subject_id", "cohort_start_date"]
        meas_prior = (
            df_measurement.select(
                col("person_id").cast(LongType()).alias("subject_id"),
                col("measurement_concept_id").cast(IntegerType()),
                to_date(col("measurement_date")).alias("measurement_date"),
                col("value_as_number").cast(DoubleType()),
            )
            .join(
                df_features.select(*episode_keys).distinct(),
                on="subject_id",
                how="inner",
            )
            .filter(
                (col("measurement_date") <= col("cohort_start_date"))
                & col("value_as_number").isNotNull()
            )
            .withColumn(
                "days_prior",
                datediff(col("cohort_start_date"), col("measurement_date")),
            )
        )

        for bio_name, bio_concept_id in self.config.biomarkers.items():
            bio_df = meas_prior.filter(col("measurement_concept_id") == bio_concept_id)

            # Latest measurement prior to T0
            w_latest = Window.partitionBy(*episode_keys).orderBy(col("measurement_date").desc())
            df_latest = (
                bio_df.withColumn("rn", row_number().over(w_latest))
                .filter(col("rn") == 1)
                .select(
                    *episode_keys,
                    spark_round(col("value_as_number"), 2).alias(f"latest_{bio_name}"),
                )
            )

            # 365-day baseline statistics
            df_stats = (
                bio_df.filter(col("days_prior") <= 365)
                .groupBy(*episode_keys)
                .agg(
                    spark_round(spark_avg("value_as_number"), 2).alias(f"mean_{bio_name}_365d"),
                    spark_round(spark_min("value_as_number"), 2).alias(f"min_{bio_name}_365d"),
                    spark_round(spark_max("value_as_number"), 2).alias(f"max_{bio_name}_365d"),
                )
            )

            df_res = (
                df_res.join(df_latest, on=episode_keys, how="left")
                .join(df_stats, on=episode_keys, how="left")
                .withColumn(
                    f"is_missing_{bio_name}",
                    when(col(f"latest_{bio_name}").isNull(), lit(1))
                    .otherwise(lit(0))
                    .cast(IntegerType()),
                )
                .withColumn(
                    f"latest_{bio_name}",
                    coalesce(col(f"latest_{bio_name}"), lit(0.0)).cast(DoubleType()),
                )
                .withColumn(
                    f"mean_{bio_name}_365d",
                    coalesce(col(f"mean_{bio_name}_365d"), lit(0.0)).cast(DoubleType()),
                )
                .withColumn(
                    f"min_{bio_name}_365d",
                    coalesce(col(f"min_{bio_name}_365d"), lit(0.0)).cast(DoubleType()),
                )
                .withColumn(
                    f"max_{bio_name}_365d",
                    coalesce(col(f"max_{bio_name}_365d"), lit(0.0)).cast(DoubleType()),
                )
            )

        return df_res

    def _attach_genomic_features(
        self,
        df_features: DataFrame,
        df_measurement: DataFrame | None,
    ) -> DataFrame:
        """Extracts binary and count ClinVar pathogenic variant embeddings.

        Args:
            df_features: Feature DataFrame anchored at cohort_start_date (T0).
            df_measurement: Optional OMOP MEASUREMENT DataFrame.

        Returns:
            ``df_features`` extended with ``has_pathogenic_variant`` (binary 0/1 indicator)
            and ``num_pathogenic_variants`` (integer count of qualifying records).
        """
        if df_measurement is None or df_measurement.limit(1).count() == 0:
            return df_features.withColumn(
                "has_pathogenic_variant", lit(0).cast(IntegerType())
            ).withColumn("num_pathogenic_variants", lit(0).cast(IntegerType()))

        pathogenic_expr = (col("measurement_concept_id") == self.config.genomic_concept_id) & (
            upper(col("value_source_value")).contains("PATHOGENIC")
            | col("value_as_concept_id").isin([4181412, 36768280])
        )

        genomic_agg = (
            df_measurement.filter(pathogenic_expr)
            .groupBy(col("person_id").cast(LongType()).alias("subject_id"))
            .agg(
                # spark_max(lit(1)) is used instead of lit(1) to ensure this is a valid
                # aggregate expression across all Spark versions.
                spark_max(lit(1)).cast(IntegerType()).alias("has_pathogenic_variant"),
                spark_count(lit(1)).cast(IntegerType()).alias("num_pathogenic_variants"),
            )
        )

        return (
            df_features.join(genomic_agg, on="subject_id", how="left")
            .withColumn(
                "has_pathogenic_variant",
                coalesce(col("has_pathogenic_variant"), lit(0)).cast(IntegerType()),
            )
            .withColumn(
                "num_pathogenic_variants",
                coalesce(col("num_pathogenic_variants"), lit(0)).cast(IntegerType()),
            )
        )

    def _build_empty_feature_matrix(self) -> DataFrame:
        """Constructs an empty feature DataFrame with full typed schema."""
        fields = [
            StructField("cohort_definition_id", LongType(), False),
            StructField("subject_id", LongType(), False),
            StructField("cohort_start_date", DateType(), False),
            StructField("cohort_end_date", DateType(), False),
            StructField("gender_concept_id", LongType(), True),
            StructField("year_of_birth", IntegerType(), True),
            StructField("age_at_index", IntegerType(), True),
            StructField("is_female", IntegerType(), False),
            StructField("is_male", IntegerType(), False),
        ]
        for w in self.config.lookback_windows_days:
            fields.append(StructField(f"condition_count_{w}d", IntegerType(), False))
        fields.append(StructField("distinct_condition_count_365d", IntegerType(), False))
        fields.append(StructField("condition_count_lifetime", IntegerType(), False))
        for cat in CHARLSON_CATEGORIES:
            fields.append(StructField(f"cci_{cat}", IntegerType(), False))
        fields.append(StructField("charlson_comorbidity_index", IntegerType(), False))

        for bio in self.config.biomarkers:
            fields.append(StructField(f"latest_{bio}", DoubleType(), False))
            fields.append(StructField(f"mean_{bio}_365d", DoubleType(), False))
            fields.append(StructField(f"min_{bio}_365d", DoubleType(), False))
            fields.append(StructField(f"max_{bio}_365d", DoubleType(), False))
            fields.append(StructField(f"is_missing_{bio}", IntegerType(), False))

        fields.append(StructField("has_pathogenic_variant", IntegerType(), False))
        fields.append(StructField("num_pathogenic_variants", IntegerType(), False))

        return self.spark.createDataFrame([], StructType(fields))

    def save_feature_matrix(
        self,
        df_features: DataFrame,
        base_output_dir: str,
        mart_name: str = "patient_feature_store",
        mode: str = "overwrite",
    ) -> str:
        """Persists the Patient Feature Matrix to Delta Lake with Liquid Clustering.

        Args:
            df_features: Feature DataFrame.
            base_output_dir: Destination base directory.
            mart_name: Table/folder name (default 'patient_feature_store').
            mode: Write mode ('overwrite' or 'append').

        Returns:
            Saved directory path string.
        """
        target_path = os.path.join(base_output_dir, mart_name)

        if HAS_DELTA and DeltaMedallionWriter is not None:
            try:
                (
                    df_features.write.format("delta")
                    .mode(mode)
                    .option("mergeSchema", "true")
                    .clusterBy("cohort_definition_id", "subject_id")
                    .save(target_path)
                )
                logger.info(
                    "[FEATURE STORE] Persisted feature matrix to Delta Lake: %s", target_path
                )
                return target_path
            except (AnalysisException, OSError) as lc_err:
                logger.warning(
                    "[FEATURE STORE] Liquid Clustering write failed; falling back to standard Delta: %s",
                    lc_err,
                )
                df_features.write.format("delta").mode(mode).save(target_path)
                return target_path
        else:
            df_features.write.mode(mode).parquet(target_path)
            logger.info("[FEATURE STORE] Persisted feature matrix to Parquet: %s", target_path)
            return target_path
