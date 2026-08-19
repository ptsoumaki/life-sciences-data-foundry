"""
Module: test_graph_auditor.py
Description: Unit tests for LangGraph GxP State Graph Auditor (Phase 7).
Author: Vivi Tsoumaki
"""

import json
import os
import sys

import mlflow

# Add repository root and agentic-ai directory to path
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
agentic_dir = os.path.join(base_dir, "agentic-ai")
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)
if agentic_dir not in sys.path:
    sys.path.insert(0, agentic_dir)

from graph_auditor import GxPGraphAuditor, compute_sha256_checksum, is_valid_sha256  # noqa: E402


def test_is_valid_sha256():
    """Validates SHA-256 string format checker."""
    valid_hash = "a" * 64
    invalid_hash_len = "a" * 63
    invalid_hash_char = "g" * 64
    assert is_valid_sha256(valid_hash) is True
    assert is_valid_sha256(invalid_hash_len) is False
    assert is_valid_sha256(invalid_hash_char) is False
    assert is_valid_sha256(None) is False
    assert is_valid_sha256("") is False


def test_graph_auditor_compilation():
    """Validates that LangGraph StateGraph builds and compiles with all required nodes."""
    auditor = GxPGraphAuditor()
    assert auditor.app is not None
    # LangGraph compiled graph should have the declared nodes
    nodes = auditor.app.get_graph().nodes
    assert "collect_mlflow_evidence" in nodes
    assert "collect_delta_log_evidence" in nodes
    assert "evaluate_cfr_part_11_compliance" in nodes
    assert "evaluate_schema_integrity" in nodes
    assert "generate_audit_findings" in nodes


def test_audit_run_lineage_compliant_mlflow(tmp_path):
    """Validates GxP audit of an MLflow run with valid 21 CFR Part 11 parameters."""
    db_path = (tmp_path / "mlflow.db").as_posix()
    mlflow_uri = f"sqlite:///{db_path}"
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("test_gxp_compliant_audit")

    valid_data_hash = compute_sha256_checksum("sample_clinical_data")
    valid_rules_hash = compute_sha256_checksum("sample_rules_contract")

    with mlflow.start_run(run_name="compliant_run") as run:
        run_id = run.info.run_id
        mlflow.log_param("data_input_path", "s3://clinical-bucket/person.parquet")
        mlflow.log_param("data_sha256", valid_data_hash)
        mlflow.log_param("rules_sha256", valid_rules_hash)
        mlflow.log_param("compliance_standard", "FDA_21_CFR_Part_11")
        mlflow.log_param("target_schema", "OMOP_CDM_v5.4")
        mlflow.log_param("execution_environment", "prod")

        mlflow.log_metric("total_records_ingested", 1000)
        mlflow.log_metric("evaluated_expectations", 4)
        mlflow.log_metric("successful_expectations", 4)
        mlflow.log_metric("unsuccessful_expectations", 0)
        mlflow.log_metric("expectation_success_rate", 100.0)
        mlflow.log_metric("gxp_gate_passed", 1.0)

    auditor = GxPGraphAuditor()
    report = auditor.audit_run_lineage(run_id=run_id, tracking_uri=mlflow_uri)

    assert report["compliance_status"] == "COMPLIANT"
    assert report["compliance_score"] >= 95.0
    assert is_valid_sha256(report["audit_receipt_sha256"]) is True
    assert report["summary"]["critical_failures"] == 0
    assert report["summary"]["error_failures"] == 0


def test_audit_run_lineage_non_compliant_mlflow(tmp_path):
    """Validates that missing checksums and failed validation trigger NON_COMPLIANT status."""
    db_path = (tmp_path / "mlflow_non_compliant.db").as_posix()
    mlflow_uri = f"sqlite:///{db_path}"
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("test_gxp_non_compliant_audit")

    with mlflow.start_run(run_name="non_compliant_run") as run:
        run_id = run.info.run_id
        mlflow.log_param("data_input_path", "unverified_data.csv")
        mlflow.log_param("data_sha256", "invalid_checksum")
        mlflow.log_param("rules_sha256", "invalid_rules_hash")

        mlflow.log_metric("total_records_ingested", 100)
        mlflow.log_metric("evaluated_expectations", 4)
        mlflow.log_metric("successful_expectations", 2)
        mlflow.log_metric("unsuccessful_expectations", 2)
        mlflow.log_metric("expectation_success_rate", 50.0)
        mlflow.log_metric("gxp_gate_passed", 0.0)

    auditor = GxPGraphAuditor()
    report = auditor.audit_run_lineage(run_id=run_id, tracking_uri=mlflow_uri)

    assert report["compliance_status"] == "NON_COMPLIANT"
    assert report["compliance_score"] < 75.0
    assert report["summary"]["critical_failures"] >= 2


def test_audit_delta_table_transaction_log(tmp_path):
    """Validates Delta Lake transaction log audit with continuous commits and schema metadata."""
    delta_dir = tmp_path / "delta_person"
    delta_log = delta_dir / "_delta_log"
    delta_log.mkdir(parents=True)

    commit_0 = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": "table-id-1",
                "format": {"provider": "parquet"},
                "schemaString": json.dumps(
                    {
                        "type": "struct",
                        "fields": [
                            {"name": "person_id", "type": "long", "nullable": False},
                            {"name": "gender_concept_id", "type": "integer", "nullable": True},
                            {"name": "year_of_birth", "type": "integer", "nullable": True},
                            {"name": "birth_datetime", "type": "string", "nullable": True},
                            {"name": "race_concept_id", "type": "integer", "nullable": True},
                            {"name": "ethnicity_concept_id", "type": "integer", "nullable": True},
                        ],
                    }
                ),
                "partitionColumns": ["person_id"],
                "configuration": {
                    "delta.enableChangeDataFeed": "true",
                    "delta.enableDeletionVectors": "true",
                },
                "createdTime": 1700000000000,
            }
        },
        {
            "commitInfo": {
                "timestamp": 1700000000000,
                "operation": "CREATE TABLE",
                "engineInfo": "Apache-Spark/3.5.0 Delta-Lake/3.1.0",
                "userMetadata": "Initial Gold OMOP CDM Table Creation",
            }
        },
        {
            "add": {
                "path": "part-0000.parquet",
                "size": 1024,
                "modificationTime": 1700000000000,
                "dataChange": True,
            }
        },
    ]

    commit_1 = [
        {
            "commitInfo": {
                "timestamp": 1700000100000,
                "operation": "MERGE",
                "engineInfo": "Apache-Spark/3.5.0 Delta-Lake/3.1.0",
                "userMetadata": "Incremental Upsert (SCD Type 1)",
            }
        },
        {
            "add": {
                "path": "part-0001.parquet",
                "size": 2048,
                "modificationTime": 1700000100000,
                "dataChange": True,
            }
        },
    ]

    with open(delta_log / "00000000000000000000.json", "w", encoding="utf-8") as f:
        f.writelines(json.dumps(entry) + "\n" for entry in commit_0)

    with open(delta_log / "00000000000000000001.json", "w", encoding="utf-8") as f:
        f.writelines(json.dumps(entry) + "\n" for entry in commit_1)

    auditor = GxPGraphAuditor()
    report = auditor.audit_delta_table(delta_table_path=str(delta_dir))

    assert report["evidence_summary"]["delta_log"] == "COLLECTED"
    seq_finding = next((f for f in report["findings"] if f["code"] == "DLT_SEQ_001"), None)
    time_finding = next((f for f in report["findings"] if f["code"] == "DLT_TIME_002"), None)
    schema_finding = next((f for f in report["findings"] if f["code"] == "SCH_001"), None)

    assert seq_finding is not None and seq_finding["passed"] is True
    assert time_finding is not None and time_finding["passed"] is True
    assert schema_finding is not None and schema_finding["passed"] is True


def test_audit_delta_table_discontinuous_commits(tmp_path):
    """Validates that a missing commit in the sequence triggers a critical failure."""
    delta_dir = tmp_path / "delta_broken"
    delta_log = delta_dir / "_delta_log"
    delta_log.mkdir(parents=True)

    # Commit 0 and Commit 2 (skipping 1)
    with open(delta_log / "00000000000000000000.json", "w", encoding="utf-8") as f:
        f.write(json.dumps({"commitInfo": {"timestamp": 1000, "operation": "WRITE"}}) + "\n")

    with open(delta_log / "00000000000000000002.json", "w", encoding="utf-8") as f:
        f.write(json.dumps({"commitInfo": {"timestamp": 2000, "operation": "WRITE"}}) + "\n")

    auditor = GxPGraphAuditor()
    report = auditor.audit_delta_table(delta_table_path=str(delta_dir))

    seq_finding = next((f for f in report["findings"] if f["code"] == "DLT_SEQ_001"), None)
    assert seq_finding is not None
    assert seq_finding["passed"] is False
    assert seq_finding["severity"] == "CRITICAL_FATAL"
    assert report["compliance_status"] == "NON_COMPLIANT"


def test_audit_delta_table_missing_directory(tmp_path):
    """Validates behavior when Delta table path does not have _delta_log/."""
    non_existent = tmp_path / "non_existent_table"
    auditor = GxPGraphAuditor()
    report = auditor.audit_delta_table(delta_table_path=str(non_existent))

    missing_finding = next((f for f in report["findings"] if f["code"] == "DLT_001"), None)
    assert missing_finding is not None
    assert missing_finding["passed"] is False
    assert report["compliance_status"] == "NON_COMPLIANT"


def test_audit_hitl_interruption_and_approval(tmp_path):
    """Validates that HITL interrupts on non-compliant runs and resumes upon electronic signature."""
    db_path = (tmp_path / "mlflow_hitl.db").as_posix()
    mlflow_uri = f"sqlite:///{db_path}"
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("test_gxp_hitl")

    with mlflow.start_run(run_name="flagged_run") as run:
        run_id = run.info.run_id
        mlflow.log_param("data_input_path", "unverified_data.csv")
        mlflow.log_param("data_sha256", "invalid_checksum")
        mlflow.log_param("rules_sha256", "invalid_rules_hash")
        mlflow.log_metric("total_records_ingested", 100)
        mlflow.log_metric("evaluated_expectations", 4)
        mlflow.log_metric("successful_expectations", 3)
        mlflow.log_metric("unsuccessful_expectations", 1)
        mlflow.log_metric("expectation_success_rate", 75.0)
        mlflow.log_metric("gxp_gate_passed", 0.0)

    auditor = GxPGraphAuditor()
    thread_id = "test_hitl_approval_thread"

    # Step 1: Initial audit execution with HITL enabled -> should interrupt
    interrupted_result = auditor.audit_run_lineage(
        run_id=run_id,
        tracking_uri=mlflow_uri,
        enable_hitl=True,
        thread_id=thread_id,
    )

    assert interrupted_result.get("hitl_interrupted") is True
    assert interrupted_result.get("thread_id") == thread_id
    review_req = interrupted_result.get("review_request", {})
    assert review_req.get("action") == "QA_SIGNOFF_REQUIRED"
    assert len(review_req.get("unpassed_findings", [])) > 0

    # Step 2: Human QA Lead reviews and provides Electronic Signature (21 CFR §11.50)
    signoff_payload = {
        "operator_id": "QA_LEAD_01",
        "decision": "APPROVED_WITH_JUSTIFICATION",
        "justification": "Deviation accepted for controlled sandbox testing.",
    }

    final_report = auditor.resume_audit_with_signoff(
        thread_id=thread_id,
        signoff_payload=signoff_payload,
    )

    assert final_report["compliance_status"] == "APPROVED_BY_QA"
    assert final_report["qa_signoff"]["operator_id"] == "QA_LEAD_01"
    assert final_report["qa_signoff"]["decision"] == "APPROVED_WITH_JUSTIFICATION"
    assert is_valid_sha256(final_report["qa_signoff"]["signature_checksum"]) is True
    assert is_valid_sha256(final_report["audit_receipt_sha256"]) is True


def test_audit_hitl_rejection(tmp_path):
    """Validates that a human QA rejection updates status to REJECTED_BY_QA."""
    db_path = (tmp_path / "mlflow_hitl_rej.db").as_posix()
    mlflow_uri = f"sqlite:///{db_path}"
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("test_gxp_hitl_rej")

    with mlflow.start_run(run_name="flagged_run_rej") as run:
        run_id = run.info.run_id
        mlflow.log_param("data_input_path", "unverified_data.csv")
        mlflow.log_param("data_sha256", "invalid_checksum")
        mlflow.log_param("rules_sha256", "invalid_rules_hash")
        mlflow.log_metric("gxp_gate_passed", 0.0)

    auditor = GxPGraphAuditor()
    thread_id = "test_hitl_rejection_thread"

    # Interrupt
    interrupted_result = auditor.audit_run_lineage(
        run_id=run_id,
        tracking_uri=mlflow_uri,
        enable_hitl=True,
        thread_id=thread_id,
    )
    assert interrupted_result.get("hitl_interrupted") is True

    # Reject
    signoff_payload = {
        "operator_id": "QA_LEAD_02",
        "decision": "REJECTED",
        "justification": "Integrity checksum missing. Batch rejected for production release.",
    }

    final_report = auditor.resume_audit_with_signoff(
        thread_id=thread_id,
        signoff_payload=signoff_payload,
    )

    assert final_report["compliance_status"] == "REJECTED_BY_QA"
    assert final_report["qa_signoff"]["operator_id"] == "QA_LEAD_02"
    assert final_report["qa_signoff"]["decision"] == "REJECTED"
