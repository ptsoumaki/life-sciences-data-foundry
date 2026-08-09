"""
Module: mlflow_tracker.py
Description: Native Great Expectations & MLflow lineage tracker for 21 CFR Part 11 compliance.
             Evaluates rules.json dynamically, computes SHA-256 cryptographic hashes,
             and logs complete audit metrology to MLflow.
"""

import hashlib
import json
import os
import sys
import great_expectations as ge
import mlflow
import pandas as pd


def compute_sha256(file_path: str) -> str:
    """Computes SHA-256 checksum for immutable audit tracking (21 CFR Part 11)."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def run_governance_pipeline(data_path: str, rules_path: str, experiment_name: str):
    """Executes dynamic Great Expectations suites and logs audit lineage to MLflow."""
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name="gxp_ingestion_integrity_gate"):
        # 1. Cryptographic Hash Provenance (21 CFR Part 11)
        data_checksum = compute_sha256(data_path)
        rules_checksum = compute_sha256(rules_path)

        mlflow.log_param("data_input_path", data_path)
        mlflow.log_param("data_sha256", data_checksum)
        mlflow.log_param("rules_sha256", rules_checksum)
        mlflow.log_param("execution_environment", os.getenv("ENVIRONMENT", "dev"))

        # 2. Parse Governance Metadata
        with open(rules_path, "r") as f:
            suite_config = json.load(f)

        meta = suite_config.get("meta", {})
        mlflow.log_param("compliance_standard", meta.get("compliance_standard", "FDA_21_CFR_Part_11"))
        mlflow.log_param("target_schema", meta.get("target_schema", "OMOP_CDM_v5.4"))

        # 3. Dynamic Great Expectations Suite Execution
        df = pd.read_csv(data_path) if data_path.endswith(".csv") else pd.DataFrame()
        ge_df = ge.from_pandas(df)
        
        validation_results = ge_df.validate(expectation_suite=suite_config)
        
        # Extract Summary Metrology
        statistics = validation_results.get("statistics", {})
        evaluated_expectations = statistics.get("evaluated_expectations", 0)
        successful_expectations = statistics.get("successful_expectations", 0)
        unsuccessful_expectations = statistics.get("unsuccessful_expectations", 0)
        success_rate = statistics.get("success_percent", 0.0)
        validation_passed = validation_results.get("success", False)

        # 4. Metric & Artifact Logging
        mlflow.log_metric("total_records_ingested", len(df))
        mlflow.log_metric("evaluated_expectations", evaluated_expectations)
        mlflow.log_metric("successful_expectations", successful_expectations)
        mlflow.log_metric("unsuccessful_expectations", unsuccessful_expectations)
        mlflow.log_metric("expectation_success_rate", success_rate)
        mlflow.log_metric("gxp_gate_passed", 1.0 if validation_passed else 0.0)

        # Log Contract and Audit Results as MLflow Artifacts
        mlflow.log_artifact(rules_path, artifact_path="governance_contracts")
        
        results_output_path = "validation_results.json"
        with open(results_output_path, "w") as f:
            json.dump(validation_results.to_json_dict(), f, indent=2)
        mlflow.log_artifact(results_output_path, artifact_path="audit_reports")

        # Cleanup local temporary audit report
        if os.path.exists(results_output_path):
            os.remove(results_output_path)

        if not validation_passed:
            raise ValueError(
                f"GxP Integrity Gate Failed: {unsuccessful_expectations} expectations violated. "
                f"Review audit_reports/validation_results.json in MLflow Run ID {mlflow.active_run().info.run_id}."
            )

        print(f"[SUCCESS] Lineage & Governance Audit Logged. Run ID: {mlflow.active_run().info.run_id}")


if __name__ == "__main__":
    data_target = sys.argv[1] if len(sys.argv) > 1 else "sample_clinical.csv"
    rules_target = sys.argv[2] if len(sys.argv) > 2 else "governance/rules.json"
    run_governance_pipeline(data_target, rules_target, "/Shared/gxp_clinical_governance")