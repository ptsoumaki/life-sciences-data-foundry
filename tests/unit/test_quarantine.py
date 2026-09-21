"""
Unit tests for GxP Dead-Letter Delta Lake Quarantine Sinks and Clinical Failure Taxonomy.
"""

import json
import os
import tempfile
import uuid

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from medallion.quarantine import (
    QUARANTINE_RECORD_SCHEMA,
    QUARANTINE_TABLE_CONDITIONS,
    QUARANTINE_TABLE_MEASUREMENTS,
    QUARANTINE_TABLE_PATIENTS,
    ClinicalFailureCode,
    GxPBreachError,
    QuarantineDeltaWriter,
    QuarantineRemediationEngine,
    evaluate_batch_quarantine_threshold,
    format_quarantine_dataframe,
)


def test_clinical_failure_code_taxonomy(spark: SparkSession):
    """Verifies that all standard GxP clinical failure codes are correctly configured."""
    expected_codes = {
        "SCHEMA_VIOLATION",
        "UNMAPPED_TERMINOLOGY",
        "OUT_OF_BOUNDS_LAB",
        "TEMPORAL_ANOMALY",
        "ORPHAN_FOREIGN_KEY",
        "TARGET_CONTRACT_VIOLATION",
    }
    actual_codes = {code.value for code in ClinicalFailureCode}
    assert actual_codes == expected_codes
    for code in ClinicalFailureCode:
        assert isinstance(code.value, str)
        assert code == ClinicalFailureCode(code.value)


def test_format_quarantine_dataframe_schema_and_payload(spark: SparkSession):
    """Verifies that non-compliant rows are converted to dead-letter rows with verbatim raw JSON payloads."""
    raw_schema = StructType(
        [
            StructField("patient_id", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("birth_date", StringType(), True),
            StructField("arbitrary_val", IntegerType(), True),
        ]
    )
    raw_data = [
        ("P001", "INVALID_GENDER", "1990-01-01", 42),
        ("P002", "MALE", "INVALID_DATE", 99),
    ]
    df_raw = spark.createDataFrame(raw_data, raw_schema)

    test_run_id = f"test-run-{uuid.uuid4()}"
    df_quarantine = format_quarantine_dataframe(
        df_raw,
        table_name="quarantine_patients",
        failure_code=ClinicalFailureCode.SCHEMA_VIOLATION,
        failure_reason="Invalid gender or birth_date format",
        mlflow_run_id=test_run_id,
    )

    # Check schema column names
    assert df_quarantine.columns == [field.name for field in QUARANTINE_RECORD_SCHEMA.fields]

    rows = df_quarantine.collect()
    assert len(rows) == 2

    # Verify first row
    r1 = rows[0]
    assert r1["table_name"] == "quarantine_patients"
    assert r1["failure_code"] == "SCHEMA_VIOLATION"
    assert r1["failure_reason"] == "Invalid gender or birth_date format"
    assert r1["mlflow_run_id"] == test_run_id
    assert r1["status"] == "QUARANTINED"
    assert r1["remediation_timestamp"] is None
    assert r1["quarantine_id"] is not None and len(r1["quarantine_id"]) > 10

    # Verify verbatim JSON payload matches original row data
    payload1 = json.loads(r1["raw_payload"])
    assert payload1["patient_id"] == "P001"
    assert payload1["gender"] == "INVALID_GENDER"
    assert payload1["birth_date"] == "1990-01-01"
    assert payload1["arbitrary_val"] == 42

    # Verify second row
    payload2 = json.loads(rows[1]["raw_payload"])
    assert payload2["patient_id"] == "P002"
    assert payload2["gender"] == "MALE"
    assert payload2["birth_date"] == "INVALID_DATE"


def test_format_quarantine_dataframe_empty(spark: SparkSession):
    """Verifies that formatting an empty DataFrame produces an empty DataFrame matching QUARANTINE_RECORD_SCHEMA."""
    raw_schema = StructType([StructField("code", StringType(), True)])
    df_empty = spark.createDataFrame([], raw_schema)

    df_q = format_quarantine_dataframe(
        df_empty,
        table_name="quarantine_conditions",
        failure_code=ClinicalFailureCode.UNMAPPED_TERMINOLOGY,
        failure_reason="No records",
    )

    assert df_q.count() == 0
    assert df_q.columns == [field.name for field in QUARANTINE_RECORD_SCHEMA.fields]


def test_quarantine_delta_writer_path_resolution(spark: SparkSession):
    """Verifies canonical path resolution for quarantine tables."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        writer = QuarantineDeltaWriter(spark, base_output_dir=tmp_dir)

        path_patients = writer.get_quarantine_table_path(QUARANTINE_TABLE_PATIENTS)
        assert path_patients.endswith("/quarantine/quarantine_patients")

        path_conditions = writer.get_quarantine_table_path("conditions")
        assert path_conditions.endswith("/quarantine/quarantine_conditions")

        path_measurements = writer.get_quarantine_table_path("measurements")
        assert path_measurements.endswith("/quarantine/quarantine_measurements")


def test_quarantine_delta_writer_persist_and_verify(spark: SparkSession):
    """Verifies persisting dead-letter records to Delta Lake and validating Delta transaction log."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        writer = QuarantineDeltaWriter(spark, base_output_dir=tmp_dir)

        raw_schema = StructType([StructField("test_val", StringType(), True)])
        df_raw = spark.createDataFrame([("BAD_DATA",)], raw_schema)

        df_q = format_quarantine_dataframe(
            df_raw,
            table_name=QUARANTINE_TABLE_MEASUREMENTS,
            failure_code=ClinicalFailureCode.OUT_OF_BOUNDS_LAB,
            failure_reason="Measurement value -999.0 is negative and biologically implausible",
            mlflow_run_id="run-12345",
        )

        saved_path = writer.write_quarantine_measurements(df_q, mode="overwrite")
        assert os.path.exists(saved_path)

        # Verify Delta Lake storage structure
        delta_log_path = os.path.join(saved_path, "_delta_log")
        assert os.path.exists(delta_log_path)

        commit_0 = os.path.join(delta_log_path, "00000000000000000000.json")
        assert os.path.exists(commit_0)

        # Parse Delta transaction log commit entries
        commit_actions = []
        with open(commit_0, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    commit_actions.append(json.loads(line))

        # Check commitInfo and add actions
        commit_info = next((a["commitInfo"] for a in commit_actions if "commitInfo" in a), None)
        assert commit_info is not None
        assert commit_info.get("operation") == "WRITE"

        add_action = next((a["add"] for a in commit_actions if "add" in a), None)
        assert add_action is not None
        assert add_action.get("path").endswith(".parquet")
        stats = json.loads(add_action.get("stats", "{}"))
        assert stats.get("numRecords") == 1


def test_evaluate_batch_quarantine_threshold_compliant():
    """Verifies that batch with quarantine ratio within threshold is marked COMPLIANT."""
    res = evaluate_batch_quarantine_threshold(
        total_ingested=1000,
        total_quarantined=15,
        threshold=0.02,
        failure_counts_by_code={"SCHEMA_VIOLATION": 10, "OUT_OF_BOUNDS_LAB": 5},
        abort_on_breach=False,
    )

    assert res["status"] == "COMPLIANT"
    assert res["is_breach"] is False
    assert res["rejection_ratio"] == 0.015
    assert res["threshold"] == 0.02
    assert res["total_quarantined"] == 15
    assert res["total_ingested"] == 1000


def test_evaluate_batch_quarantine_threshold_breach_warning():
    """Verifies that batch exceeding threshold is marked BREACH and does not raise if abort_on_breach=False."""
    res = evaluate_batch_quarantine_threshold(
        total_ingested=1000,
        total_quarantined=35,
        threshold=0.02,
        failure_counts_by_code={"SCHEMA_VIOLATION": 25, "OUT_OF_BOUNDS_LAB": 10},
        abort_on_breach=False,
    )

    assert res["status"] == "BREACH"
    assert res["is_breach"] is True
    assert res["rejection_ratio"] == 0.035
    assert res["threshold"] == 0.02


def test_evaluate_batch_quarantine_threshold_abort_on_breach():
    """Verifies that exceeding threshold raises GxPBreachError when abort_on_breach=True."""
    with pytest.raises(GxPBreachError, match="GxP Batch Quality Breach"):
        evaluate_batch_quarantine_threshold(
            total_ingested=500,
            total_quarantined=50,  # 10% >> 2%
            threshold=0.02,
            abort_on_breach=True,
        )


def test_evaluate_batch_quarantine_threshold_empty_ingest():
    """Verifies safe handling of 0 ingested records."""
    res = evaluate_batch_quarantine_threshold(
        total_ingested=0,
        total_quarantined=0,
        threshold=0.02,
    )
    assert res["status"] == "COMPLIANT"
    assert res["rejection_ratio"] == 0.0


def test_quarantine_remediation_engine_no_records(spark: SparkSession):
    """Verifies remediation engine handles empty tables gracefully."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        engine = QuarantineRemediationEngine(spark, base_output_dir=tmp_dir)
        res = engine.remediate_conditions()
        assert res["status"] == "NO_RECORDS"
        assert res["total_evaluated"] == 0
        assert res["remediated_count"] == 0


def test_quarantine_remediation_engine_conditions_with_updated_mapping(spark: SparkSession):
    """Verifies remediation of quarantined conditions after expanding vocabulary mappings."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        writer = QuarantineDeltaWriter(spark, base_output_dir=tmp_dir)

        # Create two quarantined records:
        # Row 1: Valid date '2022-03-10', unmapped code 'E11.9_EXP' (remediable)
        # Row 2: Corrupt date 'NOT_A_DATE', unmapped code 'OTHER' (irremediable)
        schema = StructType(
            [
                StructField("patient_id", StringType(), True),
                StructField("diagnosis_date", StringType(), True),
                StructField("code", StringType(), True),
                StructField("encounter_id", StringType(), True),
            ]
        )
        raw_rows = [
            ("P100", "2022-03-10", "E11.9_EXP", "ENC1"),
            ("P200", "NOT_A_DATE", "OTHER", "ENC2"),
        ]
        df_raw = spark.createDataFrame(raw_rows, schema)

        df_q = format_quarantine_dataframe(
            df_raw,
            table_name=QUARANTINE_TABLE_CONDITIONS,
            failure_code=ClinicalFailureCode.UNMAPPED_TERMINOLOGY,
            failure_reason="Unmapped diagnosis code",
            mlflow_run_id="run-rem-test",
        )

        writer.write_quarantine_conditions(df_q, mode="overwrite")

        engine = QuarantineRemediationEngine(spark, base_output_dir=tmp_dir)

        # Remediate with updated concept mapping resolving 'E11.9_EXP' to SNOMED 201826
        res = engine.remediate_conditions(updated_concept_mappings={"E11.9_EXP": 201826})

        assert res["status"] == "SUCCESS"
        assert res["total_evaluated"] == 2
        assert res["remediated_count"] == 1
        assert res["unresolved_count"] == 1

        # Check quarantine table status updates
        df_q_after = writer.read_quarantine_table(QUARANTINE_TABLE_CONDITIONS)
        rows_after = df_q_after.collect()
        remediated_row = next((r for r in rows_after if "E11.9_EXP" in r["raw_payload"]), None)
        unresolved_row = next((r for r in rows_after if "NOT_A_DATE" in r["raw_payload"]), None)

        assert remediated_row is not None
        assert remediated_row["status"] == "REMEDIATED"
        assert remediated_row["remediation_timestamp"] is not None

        assert unresolved_row is not None
        assert unresolved_row["status"] == "QUARANTINED"
        assert unresolved_row["remediation_timestamp"] is None


def test_quarantine_remediation_engine_idempotency(spark: SparkSession):
    """Verifies running remediation twice is idempotent and does not re-process remediated rows."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        writer = QuarantineDeltaWriter(spark, base_output_dir=tmp_dir)

        schema = StructType(
            [
                StructField("patient_id", StringType(), True),
                StructField("diagnosis_date", StringType(), True),
                StructField("code", StringType(), True),
            ]
        )
        df_raw = spark.createDataFrame([("P100", "2021-05-20", "I10_EXP")], schema)

        df_q = format_quarantine_dataframe(
            df_raw,
            table_name=QUARANTINE_TABLE_CONDITIONS,
            failure_code=ClinicalFailureCode.UNMAPPED_TERMINOLOGY,
            failure_reason="Unmapped code",
        )
        writer.write_quarantine_conditions(df_q, mode="overwrite")

        engine = QuarantineRemediationEngine(spark, base_output_dir=tmp_dir)

        # First remediation run: resolves the row
        res1 = engine.remediate_conditions(updated_concept_mappings={"I10_EXP": 316866})
        assert res1["remediated_count"] == 1

        # Second remediation run: 0 unresolved records remain
        res2 = engine.remediate_conditions(updated_concept_mappings={"I10_EXP": 316866})
        assert res2["status"] == "NO_RECORDS"
        assert res2["total_evaluated"] == 0
        assert res2["remediated_count"] == 0


def test_quarantine_remediation_engine_measurements(spark: SparkSession):
    """Verifies remediation of lab measurements with updated LOINC mapping."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        writer = QuarantineDeltaWriter(spark, base_output_dir=tmp_dir)

        schema = StructType(
            [
                StructField("patient_id", StringType(), True),
                StructField("lab_datetime", StringType(), True),
                StructField("code", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )
        # Row 1: valid date and positive value, unmapped LOINC '99999-9' (remediable)
        # Row 2: negative value '-10' (irremediable out-of-bounds)
        df_raw = spark.createDataFrame(
            [
                ("P1", "2023-01-01T12:00:00Z", "99999-9", "5.5"),
                ("P2", "2023-01-01T12:00:00Z", "99999-9", "-10.0"),
            ],
            schema,
        )

        df_q = format_quarantine_dataframe(
            df_raw,
            table_name=QUARANTINE_TABLE_MEASUREMENTS,
            failure_code=ClinicalFailureCode.UNMAPPED_TERMINOLOGY,
            failure_reason="Pending LOINC vocabulary review",
        )
        writer.write_quarantine_measurements(df_q, mode="overwrite")

        engine = QuarantineRemediationEngine(spark, base_output_dir=tmp_dir)
        res = engine.remediate_measurements(updated_loinc_mappings={"99999-9": 3004410})

        assert res["status"] == "SUCCESS"
        assert res["total_evaluated"] == 2
        assert res["remediated_count"] == 1
        assert res["unresolved_count"] == 1


def test_quarantine_remediation_with_pipeline_schema_columns(spark: SparkSession):
    """Verifies that remediation engine correctly handles real pipeline schema columns (icd10_code, loinc_code, numeric_value)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        writer = QuarantineDeltaWriter(spark, base_output_dir=tmp_dir)

        # 1. Test conditions with 'icd10_code' and 'raw_patient_id'
        cond_schema = StructType(
            [
                StructField("raw_patient_id", StringType(), True),
                StructField("encounter_id", StringType(), True),
                StructField("diagnosis_date", StringType(), True),
                StructField("icd10_code", StringType(), True),
            ]
        )
        cond_raw = spark.createDataFrame(
            [
                ("PAT_001", "ENC_001", "2023-05-20", "E11.9_NEW"),
                ("PAT_002", "ENC_002", "CORRUPT", "E11.9_NEW"),
            ],
            cond_schema,
        )
        df_q_cond = format_quarantine_dataframe(
            cond_raw,
            table_name=QUARANTINE_TABLE_CONDITIONS,
            failure_code=ClinicalFailureCode.UNMAPPED_TERMINOLOGY,
            failure_reason="Pipeline unmapped ICD-10",
        )
        writer.write_quarantine_conditions(df_q_cond, mode="overwrite")

        # 2. Test measurements with 'loinc_code', 'numeric_value', and 'raw_patient_id'
        meas_schema = StructType(
            [
                StructField("raw_patient_id", StringType(), True),
                StructField("lab_event_id", StringType(), True),
                StructField("lab_datetime", StringType(), True),
                StructField("loinc_code", StringType(), True),
                StructField("numeric_value", StringType(), True),
            ]
        )
        meas_raw = spark.createDataFrame(
            [
                ("PAT_001", "LAB_001", "2023-06-10T08:00:00Z", "88888-8", "12.5"),
                ("PAT_002", "LAB_002", "2023-06-10T08:00:00Z", "88888-8", "-5.0"),
            ],
            meas_schema,
        )
        df_q_meas = format_quarantine_dataframe(
            meas_raw,
            table_name=QUARANTINE_TABLE_MEASUREMENTS,
            failure_code=ClinicalFailureCode.UNMAPPED_TERMINOLOGY,
            failure_reason="Pipeline unmapped LOINC",
        )
        writer.write_quarantine_measurements(df_q_meas, mode="overwrite")

        engine = QuarantineRemediationEngine(spark, base_output_dir=tmp_dir)

        res_cond = engine.remediate_conditions(updated_concept_mappings={"E11.9_NEW": 201826})
        assert res_cond["status"] == "SUCCESS"
        assert res_cond["total_evaluated"] == 2
        assert res_cond["remediated_count"] == 1
        assert res_cond["unresolved_count"] == 1

        res_meas = engine.remediate_measurements(updated_loinc_mappings={"88888-8": 3004410})
        assert res_meas["status"] == "SUCCESS"
        assert res_meas["total_evaluated"] == 2
        assert res_meas["remediated_count"] == 1
        assert res_meas["unresolved_count"] == 1


def test_quarantine_remediation_patients_with_raw_patient_id(spark: SparkSession):
    """Verifies patient remediation and promotion to Silver when raw_patient_id is the identifier column."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        writer = QuarantineDeltaWriter(spark, base_output_dir=tmp_dir)

        patient_schema = StructType(
            [
                StructField("raw_patient_id", StringType(), True),
                StructField("birth_datetime", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("race", StringType(), True),
                StructField("ethnicity", StringType(), True),
            ]
        )
        # Row 1: valid date and gender (remediable)
        # Row 2: invalid gender 'INVALID_GENDER' (irremediable)
        patient_raw = spark.createDataFrame(
            [
                ("PAT_RAW_001", "1980-05-12", "MALE", "White", "Not Hispanic"),
                ("PAT_RAW_002", "1992-08-20", "INVALID_GENDER", "Asian", "Not Hispanic"),
            ],
            patient_schema,
        )
        df_q_pat = format_quarantine_dataframe(
            patient_raw,
            table_name=QUARANTINE_TABLE_PATIENTS,
            failure_code=ClinicalFailureCode.SCHEMA_VIOLATION,
            failure_reason="Raw demographics validation failure",
        )
        writer.write_quarantine_patients(df_q_pat, mode="overwrite")

        engine = QuarantineRemediationEngine(spark, base_output_dir=tmp_dir)
        res = engine.remediate_patients()

        assert res["status"] == "SUCCESS"
        assert res["total_evaluated"] == 2
        assert res["remediated_count"] == 1
        assert res["unresolved_count"] == 1
        assert res["promoted_path"] is not None

        assert "clinical_demographics" in res["promoted_path"]
        assert os.path.exists(res["promoted_path"])
