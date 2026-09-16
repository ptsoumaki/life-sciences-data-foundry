"""
Unit tests for Time-to-Event and Survival Analysis Marts (cohorts/survival.py).
"""

import os
import shutil
import tempfile
from datetime import date

import pytest
from cohorts.survival import (
    KAPLAN_MEIER_SCHEMA,
    SURVIVAL_FRAME_SCHEMA,
    SurvivalConfig,
    SurvivalEndpoint,
    SurvivalMartBuilder,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture
def survival_test_data(spark: SparkSession):
    """Provides synthetic Gold OMOP tables for survival analysis testing."""
    cohort_schema = StructType(
        [
            StructField("cohort_definition_id", LongType(), False),
            StructField("subject_id", LongType(), False),
            StructField("cohort_start_date", StringType(), False),
            StructField("cohort_end_date", StringType(), False),
        ]
    )
    cohort_rows = [
        (1001, 1, "2020-01-01", "2022-01-01"),  # Follow-up: 731 days
        (1001, 2, "2020-01-01", "2022-01-01"),  # Follow-up: 731 days
        (1001, 3, "2020-01-01", "2022-01-01"),  # Follow-up: 731 days
        (1001, 4, "2020-01-01", "2022-01-01"),  # Follow-up: 731 days
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
        (1, 8507, 1970),  # Age at index 2020: 50
        (2, 8532, 1960),  # Age at index 2020: 60
        (3, 8507, 1985),  # Age at index 2020: 35
        (4, 8532, 1955),  # Age at index 2020: 65
    ]
    df_person = spark.createDataFrame(person_rows, person_schema)

    death_schema = StructType(
        [
            StructField("person_id", LongType(), False),
            StructField("death_date", StringType(), False),
        ]
    )
    # Patient 1 dies at 182 days; Patient 2 dies at 366 days
    death_rows = [
        (1, "2020-07-01"),
        (2, "2021-01-01"),
    ]
    df_death = spark.createDataFrame(death_rows, death_schema)

    cond_schema = StructType(
        [
            StructField("person_id", LongType(), False),
            StructField("condition_concept_id", IntegerType(), False),
            StructField("condition_start_date", StringType(), False),
        ]
    )
    # Patient 2 has progression at 91 days; Patient 3 has progression at 200 days
    cond_rows = [
        (2, 4329847, "2020-04-01"),
        (3, 4329847, "2020-07-19"),
    ]
    df_cond = spark.createDataFrame(cond_rows, cond_schema)

    meas_schema = StructType(
        [
            StructField("person_id", LongType(), False),
            StructField("measurement_concept_id", IntegerType(), False),
            StructField("value_source_value", StringType(), True),
            StructField("value_as_concept_id", IntegerType(), True),
        ]
    )
    # Patient 1 and Patient 3 have pathogenic ClinVar variants
    meas_rows = [
        (1, 35917873, "Pathogenic; Likely Pathogenic", 36768280),
        (2, 35917873, "Benign", 4049393),
        (3, 35917873, "PATHOGENIC", 4181412),
        (4, 35917873, "Uncertain Significance", 4078249),
    ]
    df_meas = spark.createDataFrame(meas_rows, meas_schema)

    return {
        "cohort": df_cohort,
        "person": df_person,
        "death": df_death,
        "condition": df_cond,
        "measurement": df_meas,
    }


def test_overall_survival_endpoints(spark: SparkSession, survival_test_data):
    """Verifies Overall Survival (OS) calculation with observed deaths and right-censoring."""
    config = SurvivalConfig(
        endpoint=SurvivalEndpoint.OVERALL_SURVIVAL,
        study_end_date="2021-06-30",  # Administrative censoring
    )
    builder = SurvivalMartBuilder(spark, config)
    df_surv = builder.build_survival_frame(
        df_cohort=survival_test_data["cohort"],
        df_person=survival_test_data["person"],
        df_death=survival_test_data["death"],
        df_measurement=survival_test_data["measurement"],
    )

    records = {r["subject_id"]: r for r in df_surv.collect()}
    assert len(records) == 4

    # Patient 1 died on 2020-07-01 (182 days post-index, prior to study_end_date)
    p1 = records[1]
    assert p1["event"] == 1
    assert p1["time_to_event_days"] == (date(2020, 7, 1) - date(2020, 1, 1)).days
    assert p1["censoring_reason"] == "DEATH_EVENT"
    assert p1["age_at_index"] == 50
    assert p1["has_pathogenic_variant"] == 1
    assert p1["stratum"] == "Pathogenic Variant"

    # Patient 2 died on 2021-01-01 (366 days post-index, prior to study_end_date 2021-06-30)
    p2 = records[2]
    assert p2["event"] == 1
    assert p2["time_to_event_days"] == (date(2021, 1, 1) - date(2020, 1, 1)).days
    assert p2["censoring_reason"] == "DEATH_EVENT"
    assert p2["has_pathogenic_variant"] == 0
    assert p2["stratum"] == "Wild-Type / VUS"

    # Patient 3 did not die; right-censored at administrative study_end_date (2021-06-30 = 546 days)
    p3 = records[3]
    assert p3["event"] == 0
    assert p3["time_to_event_days"] == (date(2021, 6, 30) - date(2020, 1, 1)).days
    assert p3["censoring_reason"] == "STUDY_END"

    # Patient 4 did not die; right-censored at study_end_date
    p4 = records[4]
    assert p4["event"] == 0
    assert p4["time_to_event_days"] == (date(2021, 6, 30) - date(2020, 1, 1)).days


def test_time_to_progression_and_death_censoring(spark: SparkSession, survival_test_data):
    """Verifies Time-to-Progression (TTP) with progression events and death right-censoring."""
    config = SurvivalConfig(
        endpoint=SurvivalEndpoint.TIME_TO_PROGRESSION,
        target_event_concept_ids=[4329847],
        censor_at_death=True,
    )
    builder = SurvivalMartBuilder(spark, config)
    df_surv = builder.build_survival_frame(
        df_cohort=survival_test_data["cohort"],
        df_person=survival_test_data["person"],
        df_condition_occurrence=survival_test_data["condition"],
        df_death=survival_test_data["death"],
        df_measurement=survival_test_data["measurement"],
    )

    records = {r["subject_id"]: r for r in df_surv.collect()}

    # Patient 1 has no progression, but died on 2020-07-01 -> censored at death
    p1 = records[1]
    assert p1["event"] == 0
    assert p1["censoring_reason"] == "DEATH_CENSORED"
    assert p1["time_to_event_days"] == (date(2020, 7, 1) - date(2020, 1, 1)).days

    # Patient 2 progressed on 2020-04-01 (before dying in 2021) -> progression event!
    p2 = records[2]
    assert p2["event"] == 1
    assert p2["censoring_reason"] == "PROGRESSION_EVENT"
    assert p2["time_to_event_days"] == (date(2020, 4, 1) - date(2020, 1, 1)).days

    # Patient 3 progressed on 2020-07-19 (no death) -> progression event
    p3 = records[3]
    assert p3["event"] == 1
    assert p3["censoring_reason"] == "PROGRESSION_EVENT"
    assert p3["time_to_event_days"] == (date(2020, 7, 19) - date(2020, 1, 1)).days

    # Patient 4 has neither event nor death -> censored at cohort end date
    p4 = records[4]
    assert p4["event"] == 0
    assert p4["censoring_reason"] == "OBSERVATION_END"


def test_event_free_survival_composite_endpoint(spark: SparkSession, survival_test_data):
    """Verifies Event-Free Survival (EFS) where either progression or death constitutes an event."""
    config = SurvivalConfig(
        endpoint=SurvivalEndpoint.EVENT_FREE_SURVIVAL,
        target_event_concept_ids=[4329847],
    )
    builder = SurvivalMartBuilder(spark, config)
    df_surv = builder.build_survival_frame(
        df_cohort=survival_test_data["cohort"],
        df_person=survival_test_data["person"],
        df_condition_occurrence=survival_test_data["condition"],
        df_death=survival_test_data["death"],
    )

    records = {r["subject_id"]: r for r in df_surv.collect()}

    # Patient 1: death is event
    assert records[1]["event"] == 1
    assert records[1]["censoring_reason"] == "DEATH_EVENT"

    # Patient 2: progression happened before death -> progression event
    assert records[2]["event"] == 1
    assert records[2]["censoring_reason"] == "PROGRESSION_EVENT"

    # Patient 3: progression is event
    assert records[3]["event"] == 1
    assert records[3]["censoring_reason"] == "PROGRESSION_EVENT"

    # Patient 4: neither -> censored
    assert records[4]["event"] == 0


def test_kaplan_meier_estimation_and_greenwood_se(spark: SparkSession):
    """Verifies distributed Kaplan-Meier product-limit estimation and Greenwood SE calculation."""
    # Build synthetic survival data with known event distribution
    # Stratum A: 4 patients, events at t=10 (1), t=20 (1), t=30 (censored), t=40 (1)
    # S(10) = 1 - 1/4 = 0.75
    # S(20) = 0.75 * (1 - 1/3) = 0.50
    # S(30) = 0.50 * (1 - 0/2) = 0.50
    # S(40) = 0.50 * (1 - 1/1) = 0.00
    rows = [
        (1001, 1, date(2020, 1, 1), 10, 1, "EVENT", 50, 8507, 0, "Stratum A"),
        (1001, 2, date(2020, 1, 1), 20, 1, "EVENT", 50, 8507, 0, "Stratum A"),
        (1001, 3, date(2020, 1, 1), 30, 0, "CENSORED", 50, 8507, 0, "Stratum A"),
        (1001, 4, date(2020, 1, 1), 40, 1, "EVENT", 50, 8507, 0, "Stratum A"),
    ]
    df_surv = spark.createDataFrame(rows, SURVIVAL_FRAME_SCHEMA)

    builder = SurvivalMartBuilder(spark)
    df_km = builder.compute_kaplan_meier_summary(df_surv, strata_col="stratum")

    km_records = df_km.collect()
    assert len(km_records) == 4

    # t=10: n_at_risk=4, n_events=1, S(10)=0.75
    r10 = km_records[0]
    assert r10["time_to_event_days"] == 10
    assert r10["n_at_risk"] == 4
    assert r10["n_events"] == 1
    assert r10["survival_probability"] == 0.75
    assert r10["standard_error"] > 0.0

    # t=20: n_at_risk=3, n_events=1, S(20)=0.50
    r20 = km_records[1]
    assert r20["time_to_event_days"] == 20
    assert r20["n_at_risk"] == 3
    assert r20["n_events"] == 1
    assert r20["survival_probability"] == 0.50

    # t=30: n_at_risk=2, n_events=0, n_censored=1, S(30)=0.50
    r30 = km_records[2]
    assert r30["time_to_event_days"] == 30
    assert r30["n_at_risk"] == 2
    assert r30["n_events"] == 0
    assert r30["n_censored"] == 1
    assert r30["survival_probability"] == 0.50

    # t=40: n_at_risk=1, n_events=1, S(40)=0.00
    r40 = km_records[3]
    assert r40["time_to_event_days"] == 40
    assert r40["n_at_risk"] == 1
    assert r40["n_events"] == 1
    assert r40["survival_probability"] == 0.00
    assert r40["standard_error"] == 0.0


def test_empty_cohort_and_survival_frame(spark: SparkSession):
    """Verifies edge case handling for empty cohorts and empty survival frames."""
    builder = SurvivalMartBuilder(spark)
    empty_cohort = spark.createDataFrame([], SURVIVAL_FRAME_SCHEMA)

    df_empty_surv = builder.build_survival_frame(
        df_cohort=empty_cohort,
        df_person=spark.createDataFrame([], StructType([StructField("person_id", LongType())])),
    )
    assert df_empty_surv.count() == 0
    assert set(df_empty_surv.columns) == set(SURVIVAL_FRAME_SCHEMA.fieldNames())

    df_empty_km = builder.compute_kaplan_meier_summary(df_empty_surv)
    assert df_empty_km.count() == 0
    assert set(df_empty_km.columns) == set(KAPLAN_MEIER_SCHEMA.fieldNames())


def test_survival_mart_persistence(spark: SparkSession):
    """Verifies persistence of survival mart tables to disk."""
    builder = SurvivalMartBuilder(spark)
    rows = [
        (1001, 1, date(2020, 1, 1), 10, 1, "EVENT", 50, 8507, 0, "All"),
    ]
    df_surv = spark.createDataFrame(rows, SURVIVAL_FRAME_SCHEMA)

    temp_dir = tempfile.mkdtemp(prefix="test_survival_mart_")
    try:
        out_path = builder.save_survival_mart(df_surv, temp_dir, mart_name="test_mart")
        assert os.path.exists(out_path)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
