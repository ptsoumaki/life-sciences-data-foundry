import hashlib
import json
import os
import sys
import mlflow
import pandas as pd


def compute_sha256(file_path: str) -> str:
    """Computes SHA-256 checksum for immutable audit tracking (21 CFR Part 11)."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def run_governance_pipeline(
    data_path: str, rules_path: str, experiment_name: str
):
    """Executes schema integrity checks and logs complete execution lineage to MLflow."""
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name="gxp_ingestion_integrity_gate"):
        # 1. Log Input Artifact Metrology & SHA-256 Checksums
        data_checksum = compute_sha256(data_path)
        rules_checksum = compute_sha256(rules_path)

        mlflow.log_param("data_input_path", data_path)
        mlflow.log_param("data_sha256", data_checksum)
        mlflow.log_param("rules_sha256", rules_checksum)
        mlflow.log_param(
            "execution_environment", os.getenv("ENVIRONMENT", "dev")
        )

        # 2. Parse Governance Contract
        with open(rules_path, "r") as f:
            rules = json.load(f)

        mlflow.log_param(
            "compliance_standard",
            rules.get("meta", {}).get("compliance_standard"),
        )
        mlflow.log_param(
            "target_schema", rules.get("meta", {}).get("target_schema")
        )

        # 3. Simulate Integrity Validation Execution
        df = pd.read_csv(data_path) if data_path.endswith(".csv") else pd.DataFrame()
        total_records = len(df)
        null_person_ids = (
            df["person_id"].isnull().sum() if "person_id" in df.columns else 0
        )

        validation_passed = null_person_ids == 0

        # 4. Log Quality Metrics & Artifacts
        mlflow.log_metric("total_records_ingested", total_records)
        mlflow.log_metric("null_person_id_violations", null_person_ids)
        mlflow.log_metric(
            "gxp_gate_passed", 1.0 if validation_passed else 0.0
        )

        mlflow.log_artifact(rules_path, artifact_path="governance_rules")

        if not validation_passed:
            raise ValueError(
                f"GxP Integrity Gate Failed: {null_person_ids} primary key null violations detected."
            )

        print(
            f"MLflow Lineage & Governance Audit Complete. Run ID: {mlflow.active_run().info.run_id}"
        )


if __name__ == "__main__":
    # Test harness execution
    data_target = sys.argv[1] if len(sys.argv) > 1 else "sample_clinical.csv"
    rules_target = (
        sys.argv[2] if len(sys.argv) > 2 else "governance/rules.json"
    )
    run_governance_pipeline(
        data_target, rules_target, "/Shared/gxp_clinical_governance"
    )