"""
Unit tests for the OHDSI Phenotyping Engine and Cohort Builder (cohorts/builder.py).
"""

import os
import shutil
import tempfile
from datetime import date

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

from cohorts.builder import (
    COHORT_SCHEMA,
    CohortCriteria,
    CohortDefinition,
    OHDSICohortBuilder,
    get_genomic_oncology_cohort_definition,
    get_hypertension_cohort_definition,
    get_type_2_diabetes_cohort_definition,
)


@pytest.fixture
def sample_omop_data(spark: SparkSession):
    """Provides synthetic Gold-tier PERSON, CONDITION_OCCURRENCE, and MEASUREMENT tables."""
    person_schema = StructType(
        [
            StructField("person_id", LongType(), False),
            StructField("gender_concept_id", IntegerType(), False),
            StructField("year_of_birth", IntegerType(), False),
            StructField("birth_datetime", StringType(), True),
        ]
    )
    # Patient 1: Born 1980 (Age ~46 in 2026), Male
    # Patient 2: Born 2015 (Age ~11 in 2026), Female (Pediatric)
    # Patient 3: Born 1965 (Age ~61 in 2026), Female
    # Patient 4: Born 1990 (Age ~36 in 2026), Male
    person_data = [
        (1, 8507, 1980, "1980-05-12T00:00:00Z"),
        (2, 8532, 2015, "2015-08-20T00:00:00Z"),
        (3, 8532, 1965, "1965-01-15T00:00:00Z"),
        (4, 8507, 1990, "1990-11-30T00:00:00Z"),
    ]
    df_person = spark.createDataFrame(person_data, person_schema)

    cond_schema = StructType(
        [
            StructField("condition_occurrence_id", LongType(), False),
            StructField("person_id", LongType(), False),
            StructField("condition_concept_id", IntegerType(), False),
            StructField("condition_start_date", StringType(), False),
        ]
    )
    # 201826: Type 2 Diabetes
    # 316866: Hypertension
    # 4329847: Acute Myocardial Infarction (Exclusion condition)
    # 254637: Malignant Neoplasm of Lung
    cond_data = [
        (101, 1, 201826, "2023-01-10"),
        (102, 2, 201826, "2023-02-15"),  # Underage for adult cohort
        (103, 3, 201826, "2022-05-01"),
        (104, 3, 4329847, "2021-04-10"),  # Prior MI
        (105, 4, 254637, "2023-06-01"),
    ]
    df_cond = spark.createDataFrame(cond_data, cond_schema)

    meas_schema = StructType(
        [
            StructField("measurement_id", LongType(), False),
            StructField("person_id", LongType(), False),
            StructField("measurement_concept_id", IntegerType(), False),
            StructField("measurement_date", StringType(), False),
            StructField("value_as_number", DoubleType(), True),
            StructField("value_as_concept_id", IntegerType(), True),
            StructField("value_source_value", StringType(), True),
        ]
    )
    # 3004410: HbA1c
    # 35917873: Genomic Variant
    meas_data = [
        (201, 1, 3004410, "2022-12-15", 8.2, None, "8.2%"),  # Elevated HbA1c >= 7.0
        (202, 3, 3004410, "2022-04-01", 6.1, None, "6.1%"),  # Normal HbA1c < 7.0
        (203, 4, 35917873, "2023-05-20", None, 35917873, "Pathogenic; rs121913529"),
    ]
    df_meas = spark.createDataFrame(meas_data, meas_schema)

    return df_person, df_cond, df_meas


def test_cohort_definition_factories():
    """Verifies factory helper cohort definitions."""
    t2d = get_type_2_diabetes_cohort_definition()
    assert t2d.cohort_definition_id == 1001
    assert 201826 in t2d.criteria.index_condition_concept_ids
    assert t2d.criteria.min_age_at_index == 18

    htn = get_hypertension_cohort_definition(exclude_prior_mi=True)
    assert htn.cohort_definition_id == 1002
    assert 316866 in htn.criteria.index_condition_concept_ids
    assert 4329847 in htn.criteria.exclusion_condition_concept_ids

    lung = get_genomic_oncology_cohort_definition()
    assert lung.cohort_definition_id == 1003
    assert lung.criteria.require_pathogenic_variant is True


def test_build_cohort_basic_and_age_filtering(spark: SparkSession, sample_omop_data):
    """Tests basic index event extraction and age exclusion filtering."""
    df_person, df_cond, df_meas = sample_omop_data
    builder = OHDSICohortBuilder(spark)

    # Adults (>=18) with Type 2 Diabetes
    t2d_def = get_type_2_diabetes_cohort_definition(min_age=18)
    df_cohort = builder.build_cohort(t2d_def, df_person, df_cond, df_meas)

    rows = df_cohort.collect()
    subjects = [r["subject_id"] for r in rows]

    # Patient 1 (Age 46) and Patient 3 (Age 61) qualify; Patient 2 (Age 11) excluded by min_age
    assert 1 in subjects
    assert 3 in subjects
    assert 2 not in subjects
    assert len(rows) == 2

    # Schema conformance
    assert set(df_cohort.columns) == {
        "cohort_definition_id",
        "subject_id",
        "cohort_start_date",
        "cohort_end_date",
    }
    first_row = rows[0]
    assert first_row["cohort_definition_id"] == 1001
    assert isinstance(first_row["cohort_start_date"], date)
    assert isinstance(first_row["cohort_end_date"], date)


def test_build_cohort_exclusion_conditions(spark: SparkSession, sample_omop_data):
    """Tests that clinical exclusion criteria properly filter candidate subjects."""
    df_person, df_cond, df_meas = sample_omop_data
    builder = OHDSICohortBuilder(spark)

    # Type 2 Diabetes excluding prior Myocardial Infarction (4329847)
    crit = CohortCriteria(
        index_condition_concept_ids=[201826],
        min_age_at_index=18,
        exclusion_condition_concept_ids=[4329847],
    )
    defn = CohortDefinition(1010, "T2D without prior MI", "Cohort", crit)

    df_cohort = builder.build_cohort(defn, df_person, df_cond, df_meas)
    subjects = [r["subject_id"] for r in df_cohort.collect()]

    # Patient 3 had prior MI in 2021 before index in 2022 -> excluded!
    assert 1 in subjects
    assert 3 not in subjects
    assert len(subjects) == 1


def test_build_cohort_biomarker_cutoff(spark: SparkSession, sample_omop_data):
    """Tests baseline lab biomarker inclusion filtering."""
    df_person, df_cond, df_meas = sample_omop_data
    builder = OHDSICohortBuilder(spark)

    # T2D with baseline HbA1c >= 7.0%
    t2d_hba1c_def = get_type_2_diabetes_cohort_definition(require_hba1c_elevated=True)
    df_cohort = builder.build_cohort(t2d_hba1c_def, df_person, df_cond, df_meas)
    subjects = [r["subject_id"] for r in df_cohort.collect()]

    # Patient 1 has HbA1c = 8.2% (qualified)
    # Patient 3 has HbA1c = 6.1% (excluded)
    assert subjects == [1]


def test_build_cohort_multi_omics_variant_criteria(spark: SparkSession, sample_omop_data):
    """Tests genomic multi-omics carrier filtering."""
    df_person, df_cond, df_meas = sample_omop_data
    builder = OHDSICohortBuilder(spark)

    onc_def = get_genomic_oncology_cohort_definition()
    df_cohort = builder.build_cohort(onc_def, df_person, df_cond, df_meas)
    subjects = [r["subject_id"] for r in df_cohort.collect()]

    # Patient 4 has Lung Neoplasm (254637) AND Pathogenic variant in MEASUREMENT
    assert subjects == [4]


def test_build_cohort_empty_input(spark: SparkSession):
    """Tests graceful handling when no input candidates exist."""
    empty_schema = StructType([StructField("dummy", StringType(), True)])
    df_empty = spark.createDataFrame([], empty_schema)

    builder = OHDSICohortBuilder(spark)
    t2d_def = get_type_2_diabetes_cohort_definition()
    df_cohort = builder.build_cohort(t2d_def, df_empty, df_empty, df_empty)

    assert df_cohort.count() == 0
    assert df_cohort.schema == COHORT_SCHEMA


def test_save_cohort_persistence(spark: SparkSession, sample_omop_data):
    """Tests persisting the OHDSI COHORT table."""
    import pyarrow.parquet as pq

    df_person, df_cond, df_meas = sample_omop_data
    builder = OHDSICohortBuilder(spark)
    t2d_def = get_type_2_diabetes_cohort_definition()
    df_cohort = builder.build_cohort(t2d_def, df_person, df_cond, df_meas)

    temp_dir = tempfile.mkdtemp(prefix="test_cohort_sink_")
    try:
        saved_path = builder.save_cohort(df_cohort, temp_dir, mode="overwrite")
        assert os.path.exists(saved_path)

        # Verify Delta commit log if Delta enabled, else parquet files
        delta_log_dir = os.path.join(saved_path, "_delta_log")
        if os.path.exists(delta_log_dir):
            assert os.path.exists(os.path.join(delta_log_dir, "00000000000000000000.json"))

        # Verify records written via PyArrow without triggering Windows Hadoop IO
        parquet_files = [f for f in os.listdir(saved_path) if f.endswith(".parquet")]
        assert len(parquet_files) > 0
        total_rows = sum(
            pq.read_table(os.path.join(saved_path, pf)).num_rows for pf in parquet_files
        )
        assert total_rows == 2
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
