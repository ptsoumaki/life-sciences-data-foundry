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
        ["lab_event_id", "raw_patient_id", "loinc_code", "test_name", "numeric_value", "unit_value", "parsed_lab_dt_str"]
    ).withColumn("parsed_lab_dt", to_date("parsed_lab_dt_str"))

    res = transform_measurement(df).collect()
    concept_map = {row["measurement_source_value"].split(":")[0]: row["measurement_concept_id"] for row in res}

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
        ["lab_event_id", "raw_patient_id", "loinc_code", "test_name", "numeric_value", "unit_value", "parsed_lab_dt_str"]
    ).withColumn("parsed_lab_dt", to_date("parsed_lab_dt_str"))

    row = transform_measurement(df).first()

    assert row["measurement_type_concept_id"] == 45754907  # Lab Result Concept
    assert row["value_as_number"] == 5.8
    assert row["unit_source_value"] == "%"
    assert row["measurement_source_value"] == "4548-4:HbA1c Panel"
    assert isinstance(row["measurement_id"], int)
    assert row["measurement_id"] > 0
    assert isinstance(row["person_id"], int)
    assert row["person_id"] > 0
