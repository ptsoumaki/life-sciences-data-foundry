"""
Unit tests for omop_cdm_v54.condition_occurrence domain transformer.
"""

from pyspark.sql.functions import to_date
from omop_cdm_v54.condition_occurrence import transform_condition_occurrence


def test_transform_condition_occurrence_icd10_mapping(spark):
    """Verifies ICD-10-CM code mapping to standard SNOMED CT concept IDs."""
    data = [
        ("ENC1", "P1", "E11.9", "Type 2 Diabetes", "2023-01-01"),
        ("ENC2", "P2", "I10", "Essential Hypertension", "2023-01-02"),
        ("ENC3", "P3", "J45.909", "Unspecified Asthma", "2023-01-03"),
        ("ENC4", "P4", "I21.9", "Acute Myocardial Infarction", "2023-01-04"),
        ("ENC5", "P5", "C34.90", "Malignant Neoplasm of Lung", "2023-01-05"),
        ("ENC6", "P6", "Z00.00", "General Medical Exam", "2023-01-06"),
    ]
    df = spark.createDataFrame(
        data,
        ["encounter_id", "raw_patient_id", "icd10_code", "diagnosis_description", "parsed_diag_dt_str"]
    ).withColumn("parsed_diag_dt", to_date("parsed_diag_dt_str"))

    res = transform_condition_occurrence(df).collect()
    concept_map = {row["condition_source_value"].split(":")[0]: row["condition_concept_id"] for row in res}

    assert concept_map["E11.9"] == 201826
    assert concept_map["I10"] == 316866
    assert concept_map["J45.909"] == 195080
    assert concept_map["I21.9"] == 4329847
    assert concept_map["C34.90"] == 254637
    assert concept_map["Z00.00"] == 0


def test_transform_condition_occurrence_null_dates(spark):
    """Verifies OMOP CDM v5.4 compliance for null dates when source carries date only."""
    data = [("ENC100", "P100", "E11.9", "Type 2 Diabetes", "2023-05-10")]
    df = spark.createDataFrame(
        data,
        ["encounter_id", "raw_patient_id", "icd10_code", "diagnosis_description", "parsed_diag_dt_str"]
    ).withColumn("parsed_diag_dt", to_date("parsed_diag_dt_str"))

    row = transform_condition_occurrence(df).first()

    assert row["condition_start_datetime"] is None
    assert row["condition_end_date"] is None
    assert row["condition_end_datetime"] is None
    assert row["stop_reason"] is None


def test_transform_condition_occurrence_types_and_source_values(spark):
    """Verifies concept type 32817 (EHR Primary Diagnosis) and source value formatting."""
    data = [("ENC200", "PAT_55", "I10", "Primary Hypertension", "2023-06-15")]
    df = spark.createDataFrame(
        data,
        ["encounter_id", "raw_patient_id", "icd10_code", "diagnosis_description", "parsed_diag_dt_str"]
    ).withColumn("parsed_diag_dt", to_date("parsed_diag_dt_str"))

    row = transform_condition_occurrence(df).first()

    assert row["condition_type_concept_id"] == 32817
    assert row["condition_source_value"] == "I10:Primary Hypertension"
    assert isinstance(row["condition_occurrence_id"], int)
    assert row["condition_occurrence_id"] > 0
    assert isinstance(row["person_id"], int)
    assert row["person_id"] > 0
