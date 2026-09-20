"""
Unit tests for the ML-Ready Patient Feature Store and Charlson Comorbidity Index (cohorts/features.py).
"""

import os
import shutil
import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from cohorts.features import (
    FeatureStoreConfig,
    PatientFeatureStore,
)


@pytest.fixture
def feature_test_data(spark: SparkSession):
    """Provides synthetic Gold OMOP tables for feature store testing."""
    cohort_schema = StructType(
        [
            StructField("cohort_definition_id", LongType(), False),
            StructField("subject_id", LongType(), False),
            StructField("cohort_start_date", StringType(), False),
            StructField("cohort_end_date", StringType(), False),
        ]
    )
    cohort_rows = [
        (1001, 1, "2022-06-01", "2023-06-01"),  # Index: 2022-06-01
        (1001, 2, "2022-06-01", "2023-06-01"),
        (1001, 3, "2022-06-01", "2023-06-01"),
        (1001, 4, "2022-06-01", "2023-06-01"),
    ]
    df_cohort = spark.createDataFrame(cohort_rows, cohort_schema)

    person_schema = StructType(
        [
            StructField("person_id", LongType(), False),
            StructField("gender_concept_id", LongType(), False),
            StructField("year_of_birth", IntegerType(), False),
        ]
    )
    person_rows = [
        (1, 8507, 1972),  # Male, age 50 in 2022
        (2, 8532, 1962),  # Female, age 60 in 2022
        (3, 8507, 1982),  # Male, age 40 in 2022
        (4, 8532, 1952),  # Female, age 70 in 2022
    ]
    df_person = spark.createDataFrame(person_rows, person_schema)

    cond_schema = StructType(
        [
            StructField("person_id", LongType(), False),
            StructField("condition_concept_id", IntegerType(), False),
            StructField("condition_start_date", StringType(), False),
        ]
    )
    cond_rows = [
        # Patient 1: Uncomplicated diabetes (wt 1) on 2022-01-01 (151 days prior)
        (1, 201820, "2022-01-01"),
        # Patient 2: Both uncomplicated diabetes (201820) and complicated diabetes (443767, wt 2)
        # Expected: Hierarchical suppression of uncomplicated diabetes -> CCI = 2
        (2, 201820, "2021-06-01"),
        (2, 443767, "2022-03-01"),
        # Patient 3: MI (4329847, wt 1), Any Malignancy (254637, wt 2), Metastatic Tumor (4205430, wt 6)
        # Expected: Malignancy suppressed by metastatic tumor -> CCI = 1 + 6 = 7
        # Also test rolling lookback windows for Patient 3:
        # 15 days prior: 2022-05-17
        (3, 4329847, "2022-05-17"),
        # 100 days prior: 2022-02-21
        (3, 254637, "2022-02-21"),
        # 200 days prior: 2021-11-13
        (3, 4205430, "2021-11-13"),
        # Future condition (AFTER index date): should NOT be counted in baseline features
        (3, 316139, "2022-08-01"),
        # Patient 4: Mild liver (4212540, wt 1) + Severe liver (4245975, wt 3)
        # Expected: Mild liver suppressed -> CCI = 3
        (4, 4212540, "2021-01-01"),
        (4, 4245975, "2022-02-01"),
    ]
    df_cond = spark.createDataFrame(cond_rows, cond_schema)

    meas_schema = StructType(
        [
            StructField("person_id", LongType(), False),
            StructField("measurement_concept_id", IntegerType(), False),
            StructField("measurement_date", StringType(), False),
            StructField("value_as_number", DoubleType(), True),
            StructField("value_source_value", StringType(), True),
            StructField("value_as_concept_id", IntegerType(), True),
        ]
    )
    meas_rows = [
        # Patient 1: HbA1c baseline measurements (concept 3004410)
        (1, 3004410, "2022-01-15", 7.2, "HbA1c", 0),
        (1, 3004410, "2022-05-10", 8.4, "HbA1c", 0),  # Latest
        # Patient 1: Glucose (concept 3000483)
        (1, 3000483, "2022-05-10", 145.0, "Glucose", 0),
        # Patient 1: ClinVar pathogenic variant
        (1, 35917873, "2022-01-01", None, "Pathogenic", 4181412),
        (1, 35917873, "2022-03-01", None, "Pathogenic; Likely Pathogenic", 36768280),
        # Patient 2: Only Glucose
        (2, 3000483, "2022-04-12", 110.0, "Glucose", 0),
        # Patient 3: ClinVar Benign (not pathogenic)
        (3, 35917873, "2021-12-01", None, "Benign", 4049393),
    ]
    df_meas = spark.createDataFrame(meas_rows, meas_schema)

    return {
        "cohort": df_cohort,
        "person": df_person,
        "condition": df_cond,
        "measurement": df_meas,
    }


def test_charlson_comorbidity_index_calculation_and_hierarchical_suppression(
    spark: SparkSession, feature_test_data
):
    """Verifies weighted Charlson Comorbidity Index (CCI) and standard hierarchical rule exclusions."""
    store = PatientFeatureStore(spark)
    df_matrix = store.build_feature_matrix(
        df_cohort=feature_test_data["cohort"],
        df_person=feature_test_data["person"],
        df_condition_occurrence=feature_test_data["condition"],
        df_measurement=feature_test_data["measurement"],
    )

    rows = {r["subject_id"]: r for r in df_matrix.collect()}

    # Patient 1: Uncomplicated diabetes only -> CCI = 1
    p1 = rows[1]
    assert p1["cci_diabetes_uncomplicated"] == 1
    assert p1["cci_diabetes_complicated"] == 0
    assert p1["charlson_comorbidity_index"] == 1
    assert p1["age_at_index"] == 50
    assert p1["is_male"] == 1
    assert p1["is_female"] == 0

    # Patient 2: Complicated diabetes suppresses uncomplicated diabetes -> CCI = 2
    p2 = rows[2]
    assert p2["cci_diabetes_complicated"] == 1
    assert p2["cci_diabetes_uncomplicated"] == 0  # Suppressed!
    assert p2["charlson_comorbidity_index"] == 2
    assert p2["age_at_index"] == 60
    assert p2["is_female"] == 1

    # Patient 3: MI (1) + Metastatic Tumor (6), Malignancy (2) suppressed by Metastatic Tumor -> CCI = 7
    p3 = rows[3]
    assert p3["cci_myocardial_infarction"] == 1
    assert p3["cci_metastatic_tumor"] == 1
    assert p3["cci_any_malignancy"] == 0  # Suppressed!
    assert p3["charlson_comorbidity_index"] == 7

    # Patient 4: Severe liver (3) suppresses mild liver (1) -> CCI = 3
    p4 = rows[4]
    assert p4["cci_moderate_severe_liver"] == 1
    assert p4["cci_mild_liver"] == 0  # Suppressed!
    assert p4["charlson_comorbidity_index"] == 3


def test_rolling_condition_counts(spark: SparkSession, feature_test_data):
    """Verifies multi-window rolling condition lookbacks (30d, 180d, 365d, lifetime) and future filtering."""
    store = PatientFeatureStore(spark)
    df_matrix = store.build_feature_matrix(
        df_cohort=feature_test_data["cohort"],
        df_person=feature_test_data["person"],
        df_condition_occurrence=feature_test_data["condition"],
        df_measurement=feature_test_data["measurement"],
    )

    rows = {r["subject_id"]: r for r in df_matrix.collect()}

    # Patient 3 had events at:
    # 2022-05-17: 15 days prior (within 30d, 180d, 365d)
    # 2022-02-21: 100 days prior (within 180d, 365d)
    # 2021-11-13: 200 days prior (within 365d)
    # 2022-08-01: AFTER index (should NOT be counted)
    p3 = rows[3]
    assert p3["condition_count_30d"] == 1
    assert p3["condition_count_180d"] == 2
    assert p3["condition_count_365d"] == 3
    assert p3["distinct_condition_count_365d"] == 3
    assert p3["condition_count_lifetime"] == 3  # Excludes future event


def test_biomarker_aggregations_and_missingness(spark: SparkSession, feature_test_data):
    """Verifies baseline biomarker aggregations (latest, mean, min, max) and missingness imputation."""
    store = PatientFeatureStore(spark)
    df_matrix = store.build_feature_matrix(
        df_cohort=feature_test_data["cohort"],
        df_person=feature_test_data["person"],
        df_condition_occurrence=feature_test_data["condition"],
        df_measurement=feature_test_data["measurement"],
    )

    rows = {r["subject_id"]: r for r in df_matrix.collect()}

    # Patient 1 has HbA1c at 7.2 (Jan) and 8.4 (May)
    p1 = rows[1]
    assert p1["is_missing_hba1c"] == 0
    assert p1["latest_hba1c"] == 8.4
    assert p1["mean_hba1c_365d"] == 7.8
    assert p1["min_hba1c_365d"] == 7.2
    assert p1["max_hba1c_365d"] == 8.4
    assert p1["is_missing_glucose"] == 0
    assert p1["latest_glucose"] == 145.0
    # Patient 1 has no cholesterol measurement
    assert p1["is_missing_cholesterol"] == 1
    assert p1["latest_cholesterol"] == 0.0

    # Patient 2 has no HbA1c
    p2 = rows[2]
    assert p2["is_missing_hba1c"] == 1
    assert p2["latest_hba1c"] == 0.0
    assert p2["is_missing_glucose"] == 0
    assert p2["latest_glucose"] == 110.0


def test_genomic_variant_embeddings(spark: SparkSession, feature_test_data):
    """Verifies multi-omics ClinVar genomic variant embeddings (binary indicator and count)."""
    store = PatientFeatureStore(spark)
    df_matrix = store.build_feature_matrix(
        df_cohort=feature_test_data["cohort"],
        df_person=feature_test_data["person"],
        df_condition_occurrence=feature_test_data["condition"],
        df_measurement=feature_test_data["measurement"],
    )

    rows = {r["subject_id"]: r for r in df_matrix.collect()}

    # Patient 1 has 2 pathogenic variant records
    p1 = rows[1]
    assert p1["has_pathogenic_variant"] == 1
    assert p1["num_pathogenic_variants"] == 2

    # Patient 2 has no variant records
    assert rows[2]["has_pathogenic_variant"] == 0
    assert rows[2]["num_pathogenic_variants"] == 0

    # Patient 3 has only benign variants
    assert rows[3]["has_pathogenic_variant"] == 0
    assert rows[3]["num_pathogenic_variants"] == 0


def test_empty_cohort_handling(spark: SparkSession):
    """Verifies handling of empty cohort DataFrame returning fully typed empty feature store."""
    store = PatientFeatureStore(spark)
    cohort_schema = StructType(
        [
            StructField("cohort_definition_id", LongType(), False),
            StructField("subject_id", LongType(), False),
            StructField("cohort_start_date", StringType(), False),
            StructField("cohort_end_date", StringType(), False),
        ]
    )
    df_empty_cohort = spark.createDataFrame([], cohort_schema)
    df_person = spark.createDataFrame([], StructType([StructField("person_id", LongType())]))

    df_empty = store.build_feature_matrix(df_empty_cohort, df_person)
    assert df_empty.count() == 0
    assert "charlson_comorbidity_index" in df_empty.columns
    assert "condition_count_365d" in df_empty.columns
    assert "latest_hba1c" in df_empty.columns
    assert "has_pathogenic_variant" in df_empty.columns


def test_save_feature_matrix(spark: SparkSession, feature_test_data):
    """Verifies saving feature matrix table to storage."""
    store = PatientFeatureStore(spark)
    df_matrix = store.build_feature_matrix(
        df_cohort=feature_test_data["cohort"],
        df_person=feature_test_data["person"],
    )

    temp_dir = tempfile.mkdtemp(prefix="test_feature_matrix_")
    try:
        out_path = store.save_feature_matrix(df_matrix, temp_dir, mart_name="features")
        assert os.path.exists(out_path)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_empty_feature_matrix_custom_lookback_windows(spark: SparkSession):
    """Verifies that empty feature matrix matches schema with custom lookback windows."""
    config = FeatureStoreConfig(lookback_windows_days=[60, 90])
    store = PatientFeatureStore(spark, config=config)

    cohort_schema = StructType(
        [
            StructField("cohort_definition_id", LongType(), False),
            StructField("subject_id", LongType(), False),
            StructField("cohort_start_date", StringType(), False),
            StructField("cohort_end_date", StringType(), False),
        ]
    )
    df_empty_cohort = spark.createDataFrame([], cohort_schema)
    df_person = spark.createDataFrame([], StructType([StructField("person_id", LongType())]))

    df_empty = store.build_feature_matrix(df_empty_cohort, df_person)
    assert "condition_count_60d" in df_empty.columns
    assert "condition_count_90d" in df_empty.columns
    assert "condition_count_30d" not in df_empty.columns
