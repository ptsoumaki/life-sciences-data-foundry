"""
Unit tests for omop_cdm_v54.measurement domain transformer.
"""

from pyspark.sql.functions import to_date

from omop_cdm_v54.measurement import transform_measurement


def test_transform_measurement_loinc_mapping(spark):
    """Verifies LOINC code mapping to standard OMOP laboratory concept IDs."""
    data = [
        ("LAB1", "P1", "4548-4", "HbA1c Blood Panel", 6.5, "%", "2023-01-01"),
        ("LAB2", "P2", "2345-7", "Glucose in Serum/Plasma", 99.0, "mg/dL", "2023-01-02"),
        ("LAB3", "P3", "2093-3", "Cholesterol in Serum/Plasma", 185.0, "mg/dL", "2023-01-03"),
        ("LAB4", "P4", "2160-0", "Creatinine in Serum/Plasma", 0.9, "mg/dL", "2023-01-04"),
        ("LAB5", "P5", "33959-8", "ALT Serum", 25.0, "U/L", "2023-01-05"),
        ("LAB6", "P6", "9999-9", "Unknown Custom Test", 1.0, "unit", "2023-01-06"),
    ]
    df = spark.createDataFrame(
        data,
        [
            "lab_event_id",
            "raw_patient_id",
            "loinc_code",
            "test_name",
            "numeric_value",
            "unit_value",
            "parsed_lab_dt_str",
        ],
    ).withColumn("parsed_lab_dt", to_date("parsed_lab_dt_str"))

    res = transform_measurement(df).collect()
    concept_map = {
        row["measurement_source_value"].split(":")[0]: row["measurement_concept_id"] for row in res
    }

    assert concept_map["4548-4"] == 3004410
    assert concept_map["2345-7"] == 3000483
    assert concept_map["2093-3"] == 3004249
    assert concept_map["2160-0"] == 3016723
    assert concept_map["33959-8"] == 3006923
    assert concept_map["9999-9"] == 0


def test_transform_measurement_values_and_units(spark):
    """Verifies numeric value double casting, unit preservation, and lab result concept type ID."""
    data = [("LAB100", "PAT_12", "4548-4", "HbA1c Panel", 5.8, "%", "2023-04-10")]
    df = spark.createDataFrame(
        data,
        [
            "lab_event_id",
            "raw_patient_id",
            "loinc_code",
            "test_name",
            "numeric_value",
            "unit_value",
            "parsed_lab_dt_str",
        ],
    ).withColumn("parsed_lab_dt", to_date("parsed_lab_dt_str"))

    row = transform_measurement(df).first()
    assert row is not None

    assert row["measurement_type_concept_id"] == 45754907  # Lab Result Concept
    assert row["value_as_number"] == 5.8
    assert row["value_source_value"] == "5.8"
    assert row["unit_source_value"] == "%"
    assert row["measurement_source_value"] == "4548-4:HbA1c Panel"
    assert isinstance(row["measurement_id"], int)
    assert row["measurement_id"] > 0
    assert isinstance(row["person_id"], int)
    assert row["person_id"] > 0


def test_transform_measurement_datetime_preservation(spark):
    """Verifies that measurement_datetime preserves full timestamp precision when provided."""
    data = [
        ("LAB200", "PAT_15", "4548-4", "HbA1c Panel", 6.1, "%", "2023-03-15 11:30:00"),
    ]
    df = spark.createDataFrame(
        data,
        [
            "lab_event_id",
            "raw_patient_id",
            "loinc_code",
            "test_name",
            "numeric_value",
            "unit_value",
            "lab_datetime",
        ],
    )

    row = transform_measurement(df).first()
    assert row is not None

    assert row["measurement_date"] is not None
    assert str(row["measurement_date"]) == "2023-03-15"
    assert row["measurement_datetime"] is not None
    assert "2023-03-15 11:30:00" in str(row["measurement_datetime"])


def test_transform_measurement_qualitative_observations(spark):
    """Verifies that qualitative/categorical observation strings are preserved in value_source_value."""
    data = [
        ("LAB300", "PAT_20", "2345-7", "Rapid Covid Ag", "Positive", "qual", "2023-08-01"),
    ]
    df = spark.createDataFrame(
        data,
        [
            "lab_event_id",
            "raw_patient_id",
            "loinc_code",
            "test_name",
            "numeric_value",
            "unit_value",
            "parsed_lab_dt_str",
        ],
    ).withColumn("parsed_lab_dt", to_date("parsed_diag_dt_str" if False else "parsed_lab_dt_str"))

    row = transform_measurement(df).first()
    assert row is not None
    assert row["value_as_number"] is None  # Non-numeric string casts to NULL in value_as_number
    assert row["value_source_value"] == "Positive"  # Verbatim qualitative payload preserved

