"""
Module: graph_auditor.py
Description: LangGraph state graph auditor for agentic GxP compliance, Delta Lake transaction
             log auditing, MLflow lineage verification, and Human-in-the-Loop (HITL) review
             with FDA 21 CFR §11.50 / §11.200 Electronic Signatures.

Dependencies:
    Requires `langgraph>=0.0.20`, `mlflow>=2.10.0`, `pydantic>=2.6.0`.
    Install with: `pip install -e .`

Author: Vivi Tsoumaki
"""

import argparse
import datetime
import glob
import hashlib
import json
import os
import re
from typing import Any, TypedDict

import mlflow
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
from mlflow.tracking import MlflowClient

# =====================================================================
# State & Finding Types
# =====================================================================


class AuditFinding(TypedDict, total=False):
    """Structured GxP finding produced during regulatory evaluation."""

    code: str
    category: str  # "MLFLOW_LINEAGE", "DELTA_TRANSACTION_LOG", "CFR_PART_11", "SCHEMA_INTEGRITY"
    severity: str  # "CRITICAL_FATAL", "ERROR", "WARNING", "INFO"
    title: str
    description: str
    passed: bool
    details: dict[str, Any]


class QASignoff(TypedDict, total=False):
    """FDA 21 CFR §11.50 compliant Electronic Signature & Deviation Sign-off record."""

    operator_id: str
    decision: str  # "APPROVED_WITH_JUSTIFICATION", "OVERRIDE", "REJECTED"
    justification: str
    timestamp: str
    signature_checksum: str


class AuditState(TypedDict, total=False):
    """Complete state container for LangGraph audit graph traversal."""

    run_id: str | None
    delta_table_path: str | None
    rules_path: str | None
    tracking_uri: str | None
    enable_hitl: bool
    mlflow_evidence: dict[str, Any]
    delta_evidence: dict[str, Any]
    schema_evidence: dict[str, Any]
    findings: list[AuditFinding]
    compliance_score: float
    compliance_status: str  # "COMPLIANT", "NON_COMPLIANT", "FLAGGED_FOR_REVIEW", "APPROVED_BY_QA", "REJECTED_BY_QA"
    qa_signoff: QASignoff | None
    audit_report: dict[str, Any]
    errors: list[str]


# =====================================================================
# Helper Utilities
# =====================================================================


def is_valid_sha256(hash_str: str | None) -> bool:
    """Validates whether a string is a standard 64-character hexadecimal SHA-256 hash."""
    if not hash_str or not isinstance(hash_str, str):
        return False
    return bool(re.match(r"^[a-fA-F0-9]{64}$", hash_str.strip()))


def compute_sha256_checksum(content: str | bytes) -> str:
    """Computes SHA-256 checksum for audit record hashing."""
    if isinstance(content, str):
        content = content.encode("utf-8")
    return hashlib.sha256(content).hexdigest()


# =====================================================================
# LangGraph Audit State Graph Nodes
# =====================================================================


def collect_mlflow_evidence(state: AuditState) -> dict[str, Any]:
    """Node: Queries MLflow for run parameters, metrics, tags, and audit artifacts."""
    run_id = state.get("run_id")
    tracking_uri = state.get("tracking_uri")
    findings: list[AuditFinding] = list(state.get("findings") or [])
    errors: list[str] = list(state.get("errors") or [])
    mlflow_evidence: dict[str, Any] = {"status": "NOT_REQUESTED"}

    if not run_id:
        return {
            "mlflow_evidence": {"status": "SKIPPED", "reason": "No run_id provided"},
            "findings": findings,
            "errors": errors,
        }

    try:
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        client = MlflowClient(tracking_uri=tracking_uri)
        run = client.get_run(run_id)

        params = run.data.params or {}
        metrics = run.data.metrics or {}
        tags = run.data.tags or {}
        run_info = {
            "run_id": run.info.run_id,
            "experiment_id": run.info.experiment_id,
            "status": run.info.status,
            "start_time": run.info.start_time,
            "end_time": run.info.end_time,
            "lifecycle_stage": run.info.lifecycle_stage,
            "artifact_uri": run.info.artifact_uri,
        }

        # Query logged artifacts
        artifact_list: list[str] = []
        try:
            artifacts = client.list_artifacts(run_id)
            artifact_list = [a.path for a in artifacts]
        except Exception as art_err:
            artifact_list = [f"ERROR_LISTING_ARTIFACTS: {art_err}"]

        mlflow_evidence = {
            "status": "COLLECTED",
            "run_info": run_info,
            "params": params,
            "metrics": metrics,
            "tags": tags,
            "artifacts": artifact_list,
        }

        findings.append(
            {
                "code": "MLF_001",
                "category": "MLFLOW_LINEAGE",
                "severity": "INFO",
                "title": "MLflow Run Provenance Retrieved",
                "description": f"Successfully retrieved MLflow run metadata for run '{run_id}'.",
                "passed": True,
                "details": {
                    "run_id": run_id,
                    "status": run.info.status,
                    "artifact_count": len(artifact_list),
                },
            }
        )

    except Exception as e:
        error_msg = f"Failed to retrieve MLflow run '{run_id}': {e}"
        errors.append(error_msg)
        mlflow_evidence = {"status": "ERROR", "error": str(e)}
        findings.append(
            {
                "code": "MLF_ERR",
                "category": "MLFLOW_LINEAGE",
                "severity": "CRITICAL_FATAL",
                "title": "MLflow Run Lookup Failure",
                "description": error_msg,
                "passed": False,
                "details": {"run_id": run_id, "error": str(e)},
            }
        )

    return {
        "mlflow_evidence": mlflow_evidence,
        "findings": findings,
        "errors": errors,
    }


def collect_delta_log_evidence(state: AuditState) -> dict[str, Any]:
    """Node: Inspects Delta Lake transaction logs (_delta_log/*.json) for commit integrity."""
    table_path = state.get("delta_table_path")
    findings: list[AuditFinding] = list(state.get("findings") or [])
    errors: list[str] = list(state.get("errors") or [])
    delta_evidence: dict[str, Any] = {"status": "NOT_REQUESTED"}

    if not table_path:
        return {
            "delta_evidence": {"status": "SKIPPED", "reason": "No delta_table_path provided"},
            "findings": findings,
            "errors": errors,
        }

    delta_log_dir = os.path.join(table_path, "_delta_log")
    if not os.path.isdir(delta_log_dir):
        error_msg = f"Delta Lake transaction log not found at '{delta_log_dir}'."
        findings.append(
            {
                "code": "DLT_001",
                "category": "DELTA_TRANSACTION_LOG",
                "severity": "CRITICAL_FATAL",
                "title": "Missing Delta Transaction Log",
                "description": error_msg,
                "passed": False,
                "details": {"table_path": table_path},
            }
        )
        return {
            "delta_evidence": {"status": "MISSING_LOG", "error": error_msg},
            "findings": findings,
            "errors": errors,
        }

    try:
        # Find and sort JSON commit files
        commit_files = sorted(
            glob.glob(os.path.join(delta_log_dir, "[0-9]*.json")),
            key=lambda p: int(os.path.splitext(os.path.basename(p))[0]),
        )

        if not commit_files:
            error_msg = f"Delta Lake transaction log at '{delta_log_dir}' contains 0 commit files."
            findings.append(
                {
                    "code": "DLT_002",
                    "category": "DELTA_TRANSACTION_LOG",
                    "severity": "CRITICAL_FATAL",
                    "title": "Empty Delta Transaction Log",
                    "description": error_msg,
                    "passed": False,
                    "details": {"table_path": table_path, "delta_log_dir": delta_log_dir},
                }
            )
            return {
                "delta_evidence": {"status": "EMPTY_LOG", "error": error_msg},
                "findings": findings,
                "errors": errors,
            }

        commits_summary: list[dict[str, Any]] = []
        parsed_schema: dict[str, Any] | None = None
        protocol_version: dict[str, Any] | None = None
        total_add_actions = 0
        total_remove_actions = 0
        clustering_columns: list[str] = []
        change_data_feed_enabled = False
        deletion_vectors_enabled = False

        for commit_file in commit_files:
            version_str = os.path.splitext(os.path.basename(commit_file))[0]
            version = int(version_str)

            commit_info = {}
            add_actions = 0
            remove_actions = 0

            with open(commit_file, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    if "commitInfo" in record:
                        commit_info = record["commitInfo"]
                    if "protocol" in record:
                        protocol_version = record["protocol"]
                    if "metaData" in record:
                        meta = record["metaData"]
                        if "schemaString" in meta:
                            try:
                                parsed_schema = json.loads(meta["schemaString"])
                            except (json.JSONDecodeError, TypeError):
                                parsed_schema = None
                        cfg = meta.get("configuration", {})
                        if cfg.get("delta.enableChangeDataFeed") == "true":
                            change_data_feed_enabled = True
                        if cfg.get("delta.enableDeletionVectors") == "true":
                            deletion_vectors_enabled = True
                        if "clusteringColumns" in meta:
                            clustering_columns = meta["clusteringColumns"]
                        elif "partitionColumns" in meta:
                            clustering_columns = meta["partitionColumns"]
                    if "add" in record:
                        add_actions += 1
                        total_add_actions += 1
                    if "remove" in record:
                        remove_actions += 1
                        total_remove_actions += 1

            commits_summary.append(
                {
                    "version": version,
                    "file": os.path.basename(commit_file),
                    "timestamp": commit_info.get("timestamp"),
                    "operation": commit_info.get("operation", "UNKNOWN"),
                    "operation_params": commit_info.get("operationParameters", {}),
                    "user_metadata": commit_info.get("userMetadata"),
                    "engine_info": commit_info.get("engineInfo"),
                    "add_actions": add_actions,
                    "remove_actions": remove_actions,
                }
            )

        # Check commit sequence continuity
        versions = [c["version"] for c in commits_summary]
        is_continuous = len(versions) > 0 and versions == list(
            range(min(versions), max(versions) + 1)
        )

        # Check monotonic timestamps
        timestamps = [c["timestamp"] for c in commits_summary if c.get("timestamp") is not None]
        is_monotonic_time = len(timestamps) <= 1 or all(
            timestamps[i] <= timestamps[i + 1] for i in range(len(timestamps) - 1)
        )

        delta_evidence = {
            "status": "COLLECTED",
            "table_path": table_path,
            "total_commits": len(commits_summary),
            "commit_versions": versions,
            "is_continuous": is_continuous,
            "is_monotonic_time": is_monotonic_time,
            "total_add_actions": total_add_actions,
            "total_remove_actions": total_remove_actions,
            "change_data_feed_enabled": change_data_feed_enabled,
            "deletion_vectors_enabled": deletion_vectors_enabled,
            "clustering_columns": clustering_columns,
            "protocol_version": protocol_version,
            "parsed_schema": parsed_schema,
            "commits": commits_summary,
        }

        # Sequence continuity finding
        findings.append(
            {
                "code": "DLT_SEQ_001",
                "category": "DELTA_TRANSACTION_LOG",
                "severity": "CRITICAL_FATAL" if not is_continuous else "INFO",
                "title": "Delta Log Commit Sequence Continuity",
                "description": "Validates that commit versions are consecutive and strictly uninterrupted.",
                "passed": is_continuous,
                "details": {"versions": versions, "is_continuous": is_continuous},
            }
        )

        # Timestamp monotonicity finding
        findings.append(
            {
                "code": "DLT_TIME_002",
                "category": "DELTA_TRANSACTION_LOG",
                "severity": "ERROR" if not is_monotonic_time else "INFO",
                "title": "Delta Log Monotonic Timestamp Consistency",
                "description": "Validates that transaction timestamps advance monotonically without clock skew.",
                "passed": is_monotonic_time,
                "details": {"timestamps": timestamps, "is_monotonic": is_monotonic_time},
            }
        )

    except Exception as e:
        error_msg = f"Failed to parse Delta Lake transaction log at '{delta_log_dir}': {e}"
        errors.append(error_msg)
        delta_evidence = {"status": "ERROR", "error": str(e)}
        findings.append(
            {
                "code": "DLT_ERR",
                "category": "DELTA_TRANSACTION_LOG",
                "severity": "ERROR",
                "title": "Delta Transaction Log Parse Error",
                "description": error_msg,
                "passed": False,
                "details": {"error": str(e)},
            }
        )

    return {
        "delta_evidence": delta_evidence,
        "findings": findings,
        "errors": errors,
    }


def evaluate_cfr_part_11_compliance(state: AuditState) -> dict[str, Any]:
    """Node: Evaluates evidence against FDA 21 CFR Part 11 parameters (Audit Trails, SHA-256)."""
    findings: list[AuditFinding] = list(state.get("findings") or [])
    mlflow_evidence = state.get("mlflow_evidence") or {}
    delta_evidence = state.get("delta_evidence") or {}
    mlflow_status = mlflow_evidence.get("status")

    if mlflow_status == "COLLECTED":
        params = mlflow_evidence.get("params", {})
        metrics = mlflow_evidence.get("metrics", {})

        # 1. 21 CFR §11.10(e) - SHA-256 Immutable Dataset Hash
        data_sha256 = params.get("data_sha256")
        is_data_hash_valid = is_valid_sha256(data_sha256) or data_sha256 == "in_memory_dataframe"
        findings.append(
            {
                "code": "CFR_11_10_E_DATA",
                "category": "CFR_PART_11",
                "severity": "CRITICAL_FATAL",
                "title": "21 CFR §11.10(e) Data Provenance SHA-256 Checksum",
                "description": "Requires cryptographic SHA-256 hashing of source datasets for immutable audit tracking.",
                "passed": bool(is_data_hash_valid),
                "details": {"data_sha256": data_sha256, "valid_hash": is_data_hash_valid},
            }
        )

        # 2. 21 CFR §11.10(e) - SHA-256 Rule Specification Hash
        rules_sha256 = params.get("rules_sha256")
        is_rules_hash_valid = is_valid_sha256(rules_sha256)
        findings.append(
            {
                "code": "CFR_11_10_E_RULES",
                "category": "CFR_PART_11",
                "severity": "CRITICAL_FATAL",
                "title": "21 CFR §11.10(e) Contract Specification SHA-256 Checksum",
                "description": "Requires cryptographic SHA-256 hashing of Great Expectations contract rules.",
                "passed": bool(is_rules_hash_valid),
                "details": {"rules_sha256": rules_sha256, "valid_hash": is_rules_hash_valid},
            }
        )

        # 3. 21 CFR §11.10(a) - Validation of System Integrity (GxP Gate Passed)
        gxp_gate_passed = metrics.get("gxp_gate_passed")
        unsuccessful_expectations = metrics.get("unsuccessful_expectations", 0)
        validation_passed = (gxp_gate_passed == 1.0) and (unsuccessful_expectations == 0)
        findings.append(
            {
                "code": "CFR_11_10_A_VALIDATION",
                "category": "CFR_PART_11",
                "severity": "CRITICAL_FATAL",
                "title": "21 CFR §11.10(a) Automated Data Contract Gate",
                "description": "Requires 100% adherence to GxP validation expectations prior to persistence.",
                "passed": bool(validation_passed),
                "details": {
                    "gxp_gate_passed": gxp_gate_passed,
                    "unsuccessful_expectations": unsuccessful_expectations,
                    "success_rate": metrics.get("expectation_success_rate"),
                },
            }
        )

        # 4. 21 CFR §11.10(k) - Execution Environment & Compliance Standard Configuration
        compliance_standard = params.get("compliance_standard")
        target_schema = params.get("target_schema")
        execution_env = params.get("execution_environment")
        is_env_configured = bool(compliance_standard and target_schema and execution_env)
        findings.append(
            {
                "code": "CFR_11_10_K_CONFIG",
                "category": "CFR_PART_11",
                "severity": "WARNING",
                "title": "21 CFR §11.10(k) Operational Environment & Standards Tagging",
                "description": "Requires explicit declaration of regulatory standard, target schema, and execution environment.",
                "passed": bool(is_env_configured),
                "details": {
                    "compliance_standard": compliance_standard,
                    "target_schema": target_schema,
                    "execution_environment": execution_env,
                },
            }
        )
    elif mlflow_status == "SKIPPED":
        findings.append(
            {
                "code": "CFR_11_10_MLF_SKIPPED",
                "category": "CFR_PART_11",
                "severity": "INFO",
                "title": "MLflow Lineage Scope Skipped",
                "description": "MLflow run ID not requested; evaluating Delta Lake transaction log audit trail directly.",
                "passed": True,
                "details": {"reason": mlflow_evidence.get("reason", "No run_id provided")},
            }
        )

    # 5. Delta Lake User Metadata Audit Trail (if Delta evidence is available)
    if delta_evidence.get("status") == "COLLECTED":
        commits = delta_evidence.get("commits", [])
        has_operations = len(commits) > 0 and all(c.get("operation") for c in commits)
        findings.append(
            {
                "code": "CFR_11_10_E_DELTA",
                "category": "CFR_PART_11",
                "severity": "WARNING",
                "title": "21 CFR §11.10(e) Delta Lake Transaction Audit Trail",
                "description": "Validates that all Delta Lake operations are recorded with full operation metadata.",
                "passed": bool(has_operations),
                "details": {
                    "total_commits": len(commits),
                    "operations": [c.get("operation") for c in commits],
                },
            }
        )

    return {
        "findings": findings,
    }


def evaluate_schema_integrity(state: AuditState) -> dict[str, Any]:
    """Node: Evaluates schema and data contract alignment against OMOP CDM v5.4 and rules."""
    findings: list[AuditFinding] = list(state.get("findings") or [])
    rules_path = state.get("rules_path") or "governance/rules.json"
    delta_evidence = state.get("delta_evidence") or {}
    schema_evidence: dict[str, Any] = {"status": "NOT_EVALUATED"}

    # Resolve rules path
    resolved_rules_path = rules_path
    if not os.path.exists(resolved_rules_path):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        candidate = os.path.join(base_dir, rules_path)
        if os.path.exists(candidate):
            resolved_rules_path = candidate

    rules_loaded = False
    expected_columns: list[str] = []
    if os.path.exists(resolved_rules_path):
        try:
            with open(resolved_rules_path, encoding="utf-8") as f:
                rules_json = json.load(f)
            rules_loaded = True
            for exp in rules_json.get("expectations", []):
                if exp.get("expectation_type") == "expect_table_columns_to_match_set":
                    expected_columns = exp.get("kwargs", {}).get("column_set", [])
        except Exception as e:
            schema_evidence = {"status": "RULES_PARSE_ERROR", "error": str(e)}

    # Check Delta Schema if available
    delta_schema = delta_evidence.get("parsed_schema")
    if delta_schema and isinstance(delta_schema, dict):
        delta_fields = [
            f.get("name") for f in delta_schema.get("fields", []) if isinstance(f, dict)
        ]
        missing_columns = [c for c in expected_columns if c not in delta_fields]
        is_schema_compliant = len(missing_columns) == 0

        findings.append(
            {
                "code": "SCH_001",
                "category": "SCHEMA_INTEGRITY",
                "severity": "CRITICAL_FATAL" if not is_schema_compliant else "INFO",
                "title": "OMOP CDM v5.4 Schema Column Conformance",
                "description": "Validates that the physical Delta table contains all mandatory clinical columns.",
                "passed": is_schema_compliant,
                "details": {
                    "expected_columns": expected_columns,
                    "delta_columns": delta_fields,
                    "missing_columns": missing_columns,
                },
            }
        )
        schema_evidence = {
            "status": "EVALUATED",
            "delta_columns": delta_fields,
            "expected_columns": expected_columns,
            "missing_columns": missing_columns,
            "is_compliant": is_schema_compliant,
        }
    elif rules_loaded:
        findings.append(
            {
                "code": "SCH_002",
                "category": "SCHEMA_INTEGRITY",
                "severity": "INFO",
                "title": "Data Contract Rules Specification Available",
                "description": f"Loaded {len(expected_columns)} expected schema columns from '{os.path.basename(resolved_rules_path)}'.",
                "passed": True,
                "details": {
                    "rules_path": resolved_rules_path,
                    "expected_columns": expected_columns,
                },
            }
        )
        schema_evidence = {
            "status": "RULES_LOADED",
            "expected_columns": expected_columns,
        }

    return {
        "schema_evidence": schema_evidence,
        "findings": findings,
    }


def generate_audit_findings(state: AuditState) -> dict[str, Any]:
    """Node: Aggregates findings, computes GxP compliance score, and generates preliminary report."""
    findings = state.get("findings") or []
    mlflow_evidence = state.get("mlflow_evidence") or {}
    delta_evidence = state.get("delta_evidence") or {}
    schema_evidence = state.get("schema_evidence") or {}

    # Scoring weights
    penalty_weights = {
        "CRITICAL_FATAL": 40.0,
        "ERROR": 20.0,
        "WARNING": 5.0,
        "INFO": 0.0,
    }

    total_penalty = 0.0
    critical_failures = 0
    error_failures = 0
    warning_failures = 0

    for finding in findings:
        if not finding.get("passed", False):
            sev = finding.get("severity", "ERROR")
            total_penalty += penalty_weights.get(sev, 10.0)
            if sev == "CRITICAL_FATAL":
                critical_failures += 1
            elif sev == "ERROR":
                error_failures += 1
            elif sev == "WARNING":
                warning_failures += 1

    compliance_score = max(0.0, min(100.0, 100.0 - total_penalty))

    # Determine Compliance Status
    if critical_failures > 0 or error_failures > 0 or compliance_score < 75.0:
        compliance_status = "NON_COMPLIANT"
    elif warning_failures > 0 or compliance_score < 95.0:
        compliance_status = "FLAGGED_FOR_REVIEW"
    else:
        compliance_status = "COMPLIANT"

    # Audit timestamp (ISO 8601 UTC)
    audit_timestamp = datetime.datetime.now(datetime.UTC).isoformat()

    # Base report payload
    report_payload = {
        "audit_timestamp": audit_timestamp,
        "auditor": "LangGraph GxP State Auditor (Phase 7)",
        "regulatory_standard": "FDA 21 CFR Part 11 & GxP Quality Systems",
        "target_framework": "OMOP CDM v5.4 & Medallion Delta Lake",
        "compliance_status": compliance_status,
        "compliance_score": round(compliance_score, 2),
        "summary": {
            "total_evaluations": len(findings),
            "passed_evaluations": sum(1 for f in findings if f.get("passed", False)),
            "critical_failures": critical_failures,
            "error_failures": error_failures,
            "warning_failures": warning_failures,
        },
        "target_identifiers": {
            "run_id": state.get("run_id"),
            "delta_table_path": state.get("delta_table_path"),
            "rules_path": state.get("rules_path"),
        },
        "findings": findings,
        "evidence_summary": {
            "mlflow": mlflow_evidence.get("status"),
            "delta_log": delta_evidence.get("status"),
            "schema": schema_evidence.get("status"),
        },
        "qa_signoff": None,
    }

    # Cryptographic Audit Receipt (SHA-256)
    serialized_report = json.dumps(report_payload, sort_keys=True)
    audit_signature = compute_sha256_checksum(serialized_report)
    report_payload["audit_receipt_sha256"] = audit_signature

    return {
        "compliance_score": compliance_score,
        "compliance_status": compliance_status,
        "audit_report": report_payload,
    }


def human_qa_review(state: AuditState) -> dict[str, Any]:
    """Node: Human-in-the-Loop (HITL) GxP Review & 21 CFR §11.50 Electronic Signature Gate.

    When enable_hitl is True and status is FLAGGED_FOR_REVIEW or NON_COMPLIANT,
    interrupts state execution for qualified human approval/deviation sign-off.
    """
    enable_hitl = state.get("enable_hitl", False)
    status = state.get("compliance_status")

    if not enable_hitl or status not in ("FLAGGED_FOR_REVIEW", "NON_COMPLIANT"):
        return {}

    # Yield control to Human QA Reviewer via LangGraph interrupt
    review_request = {
        "action": "QA_SIGNOFF_REQUIRED",
        "compliance_status": status,
        "compliance_score": state.get("compliance_score"),
        "unpassed_findings": [f for f in state.get("findings", []) if not f.get("passed")],
        "message": "Regulatory review and electronic signature (21 CFR §11.50) required prior to persistence.",
    }

    # Pauses graph execution until human submits sign-off payload
    human_input = interrupt(review_request)

    if isinstance(human_input, dict):
        decision = human_input.get("decision", "REJECTED")
        operator_id = human_input.get("operator_id", "UNKNOWN_OPERATOR")
        justification = human_input.get("justification", "No justification provided")
        timestamp = human_input.get("timestamp") or datetime.datetime.now(datetime.UTC).isoformat()

        # Compute electronic signature checksum (21 CFR §11.50)
        sig_raw = f"{operator_id}:{decision}:{justification}:{timestamp}"
        signature_checksum = human_input.get("signature_checksum") or compute_sha256_checksum(
            sig_raw
        )

        qa_signoff: QASignoff = {
            "operator_id": operator_id,
            "decision": decision,
            "justification": justification,
            "timestamp": timestamp,
            "signature_checksum": signature_checksum,
        }

        new_status = (
            "APPROVED_BY_QA"
            if decision in ("APPROVED_WITH_JUSTIFICATION", "OVERRIDE", "APPROVED")
            else "REJECTED_BY_QA"
        )

        # Re-sign audit report with attached electronic signature
        report = dict(state.get("audit_report") or {})
        report["compliance_status"] = new_status
        report["qa_signoff"] = qa_signoff
        serialized_report = json.dumps(report, sort_keys=True)
        report["audit_receipt_sha256"] = compute_sha256_checksum(serialized_report)

        return {
            "qa_signoff": qa_signoff,
            "compliance_status": new_status,
            "audit_report": report,
        }

    return {}


# =====================================================================
# Public GxPGraphAuditor Class
# =====================================================================


class GxPGraphAuditor:
    """State graph evaluator for autonomous GxP audit trail, lineage verification,

    and Human-in-the-Loop regulatory review (FDA 21 CFR Part 11).
    """

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        checkpointer: BaseCheckpointSaver | None = None,
    ) -> None:
        """Initializes the GxPGraphAuditor with execution parameters and checkpointer."""
        self.config = config or {}
        self.checkpointer = checkpointer or MemorySaver()
        self.app = self.build_graph(checkpointer=self.checkpointer)

    def build_graph(self, checkpointer: BaseCheckpointSaver | None = None):
        """Builds and compiles the LangGraph StateGraph for GxP auditing with checkpointer."""
        workflow = StateGraph(AuditState)

        # Add Nodes
        workflow.add_node("collect_mlflow_evidence", collect_mlflow_evidence)
        workflow.add_node("collect_delta_log_evidence", collect_delta_log_evidence)
        workflow.add_node("evaluate_cfr_part_11_compliance", evaluate_cfr_part_11_compliance)
        workflow.add_node("evaluate_schema_integrity", evaluate_schema_integrity)
        workflow.add_node("generate_audit_findings", generate_audit_findings)
        workflow.add_node("human_qa_review", human_qa_review)

        # Add Linear Directed Edges
        workflow.add_edge(START, "collect_mlflow_evidence")
        workflow.add_edge("collect_mlflow_evidence", "collect_delta_log_evidence")
        workflow.add_edge("collect_delta_log_evidence", "evaluate_cfr_part_11_compliance")
        workflow.add_edge("evaluate_cfr_part_11_compliance", "evaluate_schema_integrity")
        workflow.add_edge("evaluate_schema_integrity", "generate_audit_findings")
        workflow.add_edge("generate_audit_findings", "human_qa_review")
        workflow.add_edge("human_qa_review", END)

        return workflow.compile(checkpointer=checkpointer)

    def audit_run_lineage(
        self,
        run_id: str | None = None,
        delta_table_path: str | None = None,
        rules_path: str | None = None,
        tracking_uri: str | None = None,
        enable_hitl: bool = False,
        thread_id: str = "default_thread",
    ) -> dict[str, Any]:
        """Audits the provenance graph for a specific MLflow / Medallion pipeline run."""
        initial_state: AuditState = {
            "run_id": run_id,
            "delta_table_path": delta_table_path,
            "rules_path": rules_path,
            "tracking_uri": tracking_uri,
            "enable_hitl": enable_hitl,
            "findings": [],
            "errors": [],
            "qa_signoff": None,
        }

        call_config = {"configurable": {"thread_id": thread_id}}
        final_state = self.app.invoke(initial_state, config=call_config)

        # If interrupted by HITL, retrieve current state snapshot
        state_snapshot = self.app.get_state(call_config)
        if state_snapshot.tasks and any(t.interrupts for t in state_snapshot.tasks):
            interrupt_val = state_snapshot.tasks[0].interrupts[0].value
            return {
                "hitl_interrupted": True,
                "thread_id": thread_id,
                "review_request": interrupt_val,
                "state_values": state_snapshot.values,
            }

        return final_state.get("audit_report", {})

    def resume_audit_with_signoff(
        self,
        thread_id: str,
        signoff_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Resumes a paused HITL audit thread with human approval and electronic signature."""
        call_config = {"configurable": {"thread_id": thread_id}}
        final_state = self.app.invoke(Command(resume=signoff_payload), config=call_config)
        return final_state.get("audit_report", {})

    def audit_delta_table(
        self,
        delta_table_path: str,
        rules_path: str | None = None,
        enable_hitl: bool = False,
        thread_id: str = "delta_audit_thread",
    ) -> dict[str, Any]:
        """Audits the transaction log of a physical Delta Lake table without an MLflow run."""
        return self.audit_run_lineage(
            run_id=None,
            delta_table_path=delta_table_path,
            rules_path=rules_path,
            enable_hitl=enable_hitl,
            thread_id=thread_id,
        )


# =====================================================================
# CLI Entry Point
# =====================================================================


def main() -> None:
    """CLI entry point for executing GxP LangGraph compliance audits."""
    parser = argparse.ArgumentParser(
        description="LangGraph Delta Lake Lineage & GxP Compliance Auditor (FDA 21 CFR Part 11)"
    )
    parser.add_argument("--run-id", type=str, default=None, help="MLflow run ID to audit")
    parser.add_argument("--delta-path", type=str, default=None, help="Path to Delta Lake table")
    parser.add_argument(
        "--rules", type=str, default="governance/rules.json", help="Path to rules JSON"
    )
    parser.add_argument("--tracking-uri", type=str, default=None, help="MLflow tracking URI")
    parser.add_argument(
        "--enable-hitl", action="store_true", help="Enable Human-in-the-Loop review"
    )
    parser.add_argument("--thread-id", type=str, default="cli_audit", help="Checkpointer thread ID")
    parser.add_argument(
        "--output", type=str, default=None, help="Path to save output JSON audit report"
    )

    args = parser.parse_args()

    auditor = GxPGraphAuditor()
    report = auditor.audit_run_lineage(
        run_id=args.run_id,
        delta_table_path=args.delta_path,
        rules_path=args.rules,
        tracking_uri=args.tracking_uri,
        enable_hitl=args.enable_hitl,
        thread_id=args.thread_id,
    )

    if report.get("hitl_interrupted"):
        print("\n" + "!" * 70)
        print(" [HITL INTERRUPT] Regulatory Review & Electronic Signature Required (21 CFR §11.50)")
        print(f" Thread ID: {report.get('thread_id')}")
        print("!" * 70)
        req = report.get("review_request", {})
        print(f" Message: {req.get('message')}")
        print(
            f" Current Status: {req.get('compliance_status')} | Score: {req.get('compliance_score')}"
        )
        print("\n Unpassed Findings:")
        for f in req.get("unpassed_findings", []):
            print(f"  - [{f.get('severity')}] {f.get('code')}: {f.get('title')}")
        return

    print("\n" + "=" * 70)
    print(f" GxP LINEAGE AUDIT REPORT — Status: {report.get('compliance_status')}")
    print(
        f" Score: {report.get('compliance_score')}/100.0 | Receipt: {report.get('audit_receipt_sha256', '')[:16]}..."
    )
    if report.get("qa_signoff"):
        so = report["qa_signoff"]
        print(
            f" QA Sign-off: {so.get('decision')} by {so.get('operator_id')} ({so.get('signature_checksum')[:12]}...)"
        )
    print("=" * 70)
    print(
        f"Evaluations: {report.get('summary', {}).get('total_evaluations')} total | "
        f"{report.get('summary', {}).get('passed_evaluations')} passed | "
        f"{report.get('summary', {}).get('critical_failures')} critical failures"
    )
    print("-" * 70)
    for finding in report.get("findings", []):
        icon = "✓" if finding.get("passed") else "✗"
        print(
            f" [{icon}] {finding.get('code')}: {finding.get('title')} ({finding.get('severity')})"
        )

    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"\n[INFO] Audit report saved to '{args.output}'")


if __name__ == "__main__":
    main()
