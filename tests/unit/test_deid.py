"""
Unit tests for the HIPAA Safe Harbor De-Identification Transformer (cohorts/deid.py).
"""

import pytest
from cohorts.deid import HIPAADeIdentifier
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, to_date
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture
def sample_cohort_df(spark: SparkSession):
    """Provides a synthetic OHDSI COHORT table."""
    schema = StructType(
        [
            StructField("cohort_definition_id", LongType(), False),
            StructField("subject_id", LongType(), False),
            StructField("cohort_start_date", StringType(), False),
            StructField("cohort_end_date", StringType(), False),
        ]
    )
    # Patient 1: Duration = 365 days (2022-01-01 to 2023-01-01)
    # Patient 2: Duration = 180 days (2023-06-01 to 2023-11-28)
    data = [
        (1001, 101, "2022-01-01", "2023-01-01"),
        (1001, 102, "2023-06-01", "2023-11-28"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_person_df(spark: SparkSession):
    """Provides a synthetic OMOP PERSON table with ages across thresholds and ZIP codes."""
    schema = StructType(
        [
            StructField("person_id", LongType(), False),
            StructField("gender_concept_id", IntegerType(), False),
            StructField("year_of_birth", IntegerType(), False),
            StructField("birth_datetime", StringType(), True),
            StructField("zip", StringType(), True),
        ]
    )
    # Patient 101: Born 1980 (Age 46 in 2026) -> Standard adult
    # Patient 102: Born 1930 (Age 96 in 2026) -> Over 89, must be capped!
    # Patient 103: Born 1937 (Age 89 in 2026) -> Exactly 89, not capped
    data = [
        (101, 8507, 1980, "1980-05-12T08:30:00Z", "90210"),  # Regular ZIP
        (102, 8532, 1930, "1930-01-15T00:00:00Z", "03612"),  # Restricted ZIP3 (036)
        (103, 8507, 1937, "1937-11-20T12:00:00Z", "10001"),  # Regular ZIP
    ]
    return spark.createDataFrame(data, schema)


def test_pseudonymization_determinism_and_salt(spark: SparkSession, sample_cohort_df):
    """Verifies that pseudonymization is deterministic with identical salt, and distinct with different salt."""
    deid_a = HIPAADeIdentifier(salt="SALT_ALPHA", max_shift_days=100)
    deid_a2 = HIPAADeIdentifier(salt="SALT_ALPHA", max_shift_days=100)
    deid_b = HIPAADeIdentifier(salt="SALT_BETA", max_shift_days=100)

    res_a = deid_a.deidentify_cohort(sample_cohort_df).collect()
    res_a2 = deid_a2.deidentify_cohort(sample_cohort_df).collect()
    res_b = deid_b.deidentify_cohort(sample_cohort_df).collect()

    ids_a = [r["subject_id"] for r in res_a]
    ids_a2 = [r["subject_id"] for r in res_a2]
    ids_b = [r["subject_id"] for r in res_b]

    # Determinism with same salt
    assert ids_a == ids_a2

    # Different salt produces different pseudonymized IDs
    assert ids_a != ids_b

    # Pseudonymized IDs are distinct from original IDs (101, 102)
    assert 101 not in ids_a
    assert 102 not in ids_a


def test_date_shifting_preserves_longitudinal_intervals(spark: SparkSession, sample_cohort_df):
    """Verifies that patient-specific date shifting strictly preserves time-to-event intervals and cohort durations."""
    deid = HIPAADeIdentifier(salt="TEST_SALT_123", max_shift_days=180)
    df_deid = deid.deidentify_cohort(sample_cohort_df)

    # Compute original intervals
    df_orig_intervals = (
        sample_cohort_df.withColumn(
            "orig_duration",
            datediff(to_date(col("cohort_end_date")), to_date(col("cohort_start_date"))),
        )
        .select("orig_duration")
        .collect()
    )

    orig_duration_list = [r["orig_duration"] for r in df_orig_intervals]

    # Compute de-identified intervals
    df_deid_intervals = (
        df_deid.withColumn(
            "deid_duration",
            datediff(col("cohort_end_date"), col("cohort_start_date")),
        )
        .select("deid_duration")
        .collect()
    )

    deid_duration_list = [r["deid_duration"] for r in df_deid_intervals]

    # Invariant: Cohort duration is exactly preserved for each record
    assert sorted(orig_duration_list) == sorted(deid_duration_list)

    # Verify dates were actually shifted away from original
    orig_rows = sample_cohort_df.collect()
    deid_rows = df_deid.collect()
    orig_dates = {r["cohort_start_date"] for r in orig_rows}
    deid_dates = {r["cohort_start_date"].strftime("%Y-%m-%d") for r in deid_rows}
    # At least one date is shifted
    assert orig_dates != deid_dates


def test_age_capping_and_birth_datetime_clearing(spark: SparkSession, sample_person_df):
    """Verifies HIPAA Safe Harbor age capping at 89 for individuals aged 90 or older."""
    deid = HIPAADeIdentifier(salt="TEST_SALT_AGE")
    df_deid = deid.deidentify_person(sample_person_df, reference_year=2026)

    rows = df_deid.collect()

    # Patient 101 (Age 46) -> birth year 1980 preserved
    p101 = next(r for r in rows if r["year_of_birth"] == 1980)
    assert p101["birth_datetime"] is None

    # Patient 102 (Born 1930, Age 96 in 2026) -> Capped to age 89, so year_of_birth becomes 2026 - 89 = 1937
    p102 = next(r for r in rows if r["year_of_birth"] == 1937)
    assert p102["birth_datetime"] is None

    # Patient 103 (Born 1937, Age 89 in 2026) -> Exactly 89, remains 1937
    matching_1937 = [r for r in rows if r["year_of_birth"] == 1937]
    assert len(matching_1937) == 2  # p102 (capped to 1937) and p103 (originally 1937)


def test_geographic_masking_zip3(spark: SparkSession, sample_person_df):
    """Verifies that ZIP codes are truncated to ZIP3 and restricted prefixes masked to 000."""
    deid = HIPAADeIdentifier(salt="TEST_SALT_ZIP")
    df_deid = deid.deidentify_person(sample_person_df, reference_year=2026)

    zips = {r["zip"] for r in df_deid.collect()}

    # "90210" -> "902"
    assert "902" in zips
    # "10001" -> "100"
    assert "100" in zips
    # "03612" (036 is in RESTRICTED_ZIP3_PREFIXES) -> "000"
    assert "000" in zips
    assert "036" not in zips


def test_deidentify_longitudinal_table(spark: SparkSession):
    """Verifies consistent multi-column date shifting on clinical condition occurrences."""
    schema = StructType(
        [
            StructField("condition_occurrence_id", LongType(), False),
            StructField("person_id", LongType(), False),
            StructField("condition_concept_id", IntegerType(), False),
            StructField("condition_start_date", StringType(), False),
            StructField("condition_end_date", StringType(), False),
        ]
    )
    data = [
        (1, 555, 201826, "2023-01-01", "2023-01-15"),  # 14 days interval
    ]
    df_cond = spark.createDataFrame(data, schema)

    deid = HIPAADeIdentifier(salt="LONGITUDINAL_SALT", max_shift_days=100)
    df_deid = deid.deidentify_longitudinal_table(
        df_cond,
        person_id_col="person_id",
        date_cols=["condition_start_date", "condition_end_date"],
    )

    row = df_deid.collect()[0]

    # Person ID was pseudonymized
    assert row["person_id"] != 555

    # Duration between start and end date is strictly preserved (14 days)
    delta_days = (row["condition_end_date"] - row["condition_start_date"]).days
    assert delta_days == 14


def test_deid_empty_dataframe(spark: SparkSession):
    """Verifies graceful handling of empty DataFrames."""
    schema = StructType([StructField("subject_id", LongType(), False)])
    df_empty = spark.createDataFrame([], schema)

    deid = HIPAADeIdentifier()
    df_res = deid.deidentify_cohort(df_empty)
    assert df_res.count() == 0
