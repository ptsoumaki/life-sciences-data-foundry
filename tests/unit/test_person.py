"""
Unit tests for omop_cdm_v54.person domain transformer.
"""

from pyspark.sql.functions import to_date

from omop_cdm_v54.person import transform_person


def test_transform_person_gender_mapping(spark):
    """Verifies gender string normalization and standard OMOP concept ID resolution."""
    data = [
        ("P1", "MALE", "WHITE", "NOT_HISPANIC", "1990-01-01"),
        ("P2", "m", "WHITE", "NOT_HISPANIC", "1991-02-02"),
        ("P3", "FEMALE", "WHITE", "NOT_HISPANIC", "1992-03-03"),
        ("P4", "F", "WHITE", "NOT_HISPANIC", "1993-04-04"),
        ("P5", "UNKNOWN", "WHITE", "NOT_HISPANIC", "1994-05-05"),
        ("P6", None, "WHITE", "NOT_HISPANIC", "1995-06-06"),
    ]
    df = spark.createDataFrame(
        data, ["raw_patient_id", "gender", "race", "ethnicity", "parsed_birth_dt_str"]
    ).withColumn("parsed_birth_dt", to_date("parsed_birth_dt_str"))

    res = transform_person(df).collect()
    res_dict = {row["person_source_value"]: row["gender_concept_id"] for row in res}

    assert res_dict["P1"] == 8507  # Male
    assert res_dict["P2"] == 8507  # Male
    assert res_dict["P3"] == 8532  # Female
    assert res_dict["P4"] == 8532  # Female
    assert res_dict["P5"] == 0  # Unknown
    assert res_dict["P6"] == 0  # Unknown


def test_transform_person_race_ethnicity_mapping(spark):
    """Verifies race and ethnicity OMOP concept mappings."""
    data = [
        ("P1", "M", "WHITE", "HISPANIC", "1980-01-01"),
        ("P2", "F", "ASIAN", "NOT_HISPANIC", "1985-05-05"),
        ("P3", "M", "BLACK", "NON_HISPANIC", "1990-09-09"),
        ("P4", "F", "OTHER", "UNKNOWN", "1995-12-12"),
    ]
    df = spark.createDataFrame(
        data, ["raw_patient_id", "gender", "race", "ethnicity", "parsed_birth_dt_str"]
    ).withColumn("parsed_birth_dt", to_date("parsed_birth_dt_str"))

    res = transform_person(df).collect()
    race_dict = {row["person_source_value"]: row["race_concept_id"] for row in res}
    ethnicity_dict = {row["person_source_value"]: row["ethnicity_concept_id"] for row in res}

    assert race_dict["P1"] == 8527  # White
    assert race_dict["P2"] == 8515  # Asian
    assert race_dict["P3"] == 8516  # Black
    assert race_dict["P4"] == 0  # Unknown/Other

    assert ethnicity_dict["P1"] == 38003563  # Hispanic
    assert ethnicity_dict["P2"] == 38003564  # Not Hispanic
    assert ethnicity_dict["P3"] == 38003564  # Non-Hispanic
    assert ethnicity_dict["P4"] == 0  # Unknown


def test_transform_person_birth_date_decomposition(spark):
    """Verifies parsing and component extraction of birth dates into year, month, and day fields."""
    data = [("P100", "MALE", "WHITE", "NOT_HISPANIC", "1988-11-23")]
    df = spark.createDataFrame(
        data, ["raw_patient_id", "gender", "race", "ethnicity", "parsed_birth_dt_str"]
    ).withColumn("parsed_birth_dt", to_date("parsed_birth_dt_str"))

    row = transform_person(df).first()
    assert row is not None

    assert row["year_of_birth"] == 1988
    assert row["month_of_birth"] == 11
    assert row["day_of_birth"] == 23


def test_transform_person_hash_id_and_source_values(spark):
    """Verifies deterministic xxhash64 person_id generation and source value preservation."""
    data = [("PAT_999", "Female", "Asian", "Non_Hispanic", "1999-09-09")]
    df = spark.createDataFrame(
        data, ["raw_patient_id", "gender", "race", "ethnicity", "parsed_birth_dt_str"]
    ).withColumn("parsed_birth_dt", to_date("parsed_birth_dt_str"))

    row = transform_person(df).first()
    assert row is not None

    assert isinstance(row["person_id"], int)
    assert row["person_id"] > 0
    assert row["person_source_value"] == "PAT_999"
    assert row["gender_source_value"] == "Female"
    assert row["race_source_value"] == "Asian"
    assert row["ethnicity_source_value"] == "Non_Hispanic"
