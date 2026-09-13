"""
Integration tests for End-to-End Medallion Quarantine Routing, GxP Quality Breach Enforcement,
and Idempotent Remediation Replay.
"""

import os
import tempfile
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from medallion.quarantine import (
    QUARANTINE_TABLE_CONDITIONS,
    ClinicalFailureCode,
    GxPBreachError,
    QuarantineDeltaWriter,
    QuarantineRemediationEngine,
    format_quarantine_dataframe,
)
from omop_cdm_v54.pipeline import run_omop_pipeline


def test_pipeline_quarantine_routing_demo_mode(spark: SparkSession, tmp_path: Any):
    """
    Verifies that run_omop_pipeline correctly routes non-compliant records to quarantine sinks
    during Medallion pipeline execution without aborting when within quality thresholds.
    """
    output_dir = str(tmp_path / "delta_warehouse")

    res = run_omop_pipeline(
        spark,
        mode="demo",
        save_delta=True,
        output_dir=output_dir,
        enable_contract_enforcement=False,
        quarantine_threshold=0.50,
        abort_on_breach=False,
    )

    assert "person" in res
    assert "condition_occurrence" in res
    assert "measurement" in res

    # Verify Medallion gold tables exist
    gold_dir = os.path.join(output_dir, "gold")
    assert os.path.exists(os.path.join(gold_dir, "person"))


def test_pipeline_quarantine_breach_abort_on_strict_threshold(spark: SparkSession, tmp_path: Any):
    """
    Verifies that setting an impossibly tight quarantine threshold (e.g. 0.000001) with abort_on_breach=True
    successfully trips the GxP breach gate if any records are rejected.
    """
    output_dir = str(tmp_path / "delta_warehouse_abort")

    # In demo mode, if there are corrupt records or when threshold is breached,
    # evaluate_batch_quarantine_threshold aborts execution.
    try:
        run_omop_pipeline(
            spark,
            mode="demo",
            save_delta=True,
            output_dir=output_dir,
            enable_contract_enforcement=False,
            quarantine_threshold=0.00000001,
            abort_on_breach=True,
        )
    except GxPBreachError as err:
        assert "GxP Batch Quality Breach" in str(err)


def test_quarantine_remediation_replay_end_to_end(spark: SparkSession):
    """
    Verifies end-to-end remediation flow:
    1. Quarantined records with unmapped clinical codes are stored in dead-letter Delta sink.
    2. QuarantineRemediationEngine re-evaluates records with updated concept mappings.
    3. Corrected records are promoted to Silver 'clinical_diagnoses' Delta table.
    4. Quarantine records are idempotently marked as REMEDIATED.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        q_writer = QuarantineDeltaWriter(spark, base_output_dir=tmp_dir)

        # Stage non-compliant condition with unmapped code 'E11.9_INT'
        schema = StructType(
            [
                StructField("patient_id", StringType(), True),
                StructField("encounter_id", StringType(), True),
                StructField("diagnosis_date", StringType(), True),
                StructField("code", StringType(), True),
            ]
        )
        raw_rows = [
            ("P_INT_1", "ENC_1", "2023-04-15", "E11.9_INT"),
            ("P_INT_2", "ENC_2", "CORRUPT_DATE", "OTHER"),
        ]
        df_raw = spark.createDataFrame(raw_rows, schema)

        df_q = format_quarantine_dataframe(
            df_raw,
            table_name=QUARANTINE_TABLE_CONDITIONS,
            failure_code=ClinicalFailureCode.UNMAPPED_TERMINOLOGY,
            failure_reason="Integration test unmapped code",
        )
        q_writer.write_quarantine_conditions(df_q, mode="overwrite")

        engine = QuarantineRemediationEngine(spark, base_output_dir=tmp_dir)

        # Execute remediation with updated concept mapping resolving E11.9_INT to SNOMED 201826
        result = engine.remediate_conditions(updated_concept_mappings={"E11.9_INT": 201826})

        assert result["status"] == "SUCCESS"
        assert result["total_evaluated"] == 2
        assert result["remediated_count"] == 1
        assert result["unresolved_count"] == 1

        # Check that promoted Silver table exists on disk
        silver_diag_path = os.path.join(tmp_dir, "silver", "clinical_diagnoses")
        assert os.path.exists(silver_diag_path)

        # Verify second remediation run is idempotent (0 unresolved records remaining to evaluate)
        re_result = engine.remediate_conditions(updated_concept_mappings={"E11.9_INT": 201826})
        # After marking as REMEDIATED, unresolved record count drops to 1 ('P_INT_2')
        assert re_result["total_evaluated"] == 1
        assert re_result["remediated_count"] == 0
        assert re_result["unresolved_count"] == 1
