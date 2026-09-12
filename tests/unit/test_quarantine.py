"""
Unit tests for GxP Dead-Letter Delta Lake Quarantine Sinks and Clinical Failure Taxonomy.
"""

import json
import os
import tempfile
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from medallion.quarantine import (
    QUARANTINE_RECORD_SCHEMA,
    QUARANTINE_TABLE_MEASUREMENTS,
    QUARANTINE_TABLE_PATIENTS,
    ClinicalFailureCode,
    QuarantineDeltaWriter,
    format_quarantine_dataframe,
)


def test_clinical_failure_code_taxonomy():
    """Verifies that all 5 standard GxP clinical failure codes are correctly configured."""
    expected_codes = {
        "SCHEMA_VIOLATION",
        "UNMAPPED_TERMINOLOGY",
        "OUT_OF_BOUNDS_LAB",
        "TEMPORAL_ANOMALY",
        "ORPHAN_FOREIGN_KEY",
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
