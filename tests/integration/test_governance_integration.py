"""
Integration tests for MLflow cryptographic lineage tracking and Great Expectations contract auditing.
"""

import os
import mlflow
import pandas as pd
from governance.mlflow_tracker import evaluate_data_contract, compute_sha256


def test_mlflow_lineage_and_contract_auditing(tmp_path):
    """
    Verifies that evaluate_data_contract evaluates GxP rules, logs metrics to MLflow,
    and calculates cryptographic SHA-256 checksums.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    rules_path = os.path.join(base_dir, "governance", "rules.json")

    # Sample data compliant with OMOP CDM v5.4 rules
    valid_data = {
        "person_id": [101, 102, 103],
        "gender_concept_id": [8507, 8532, 8507],
        "year_of_birth": [1990, 1988, 1995],
        "birth_datetime": ["1990-05-12T00:00:00Z", "1988-09-23T00:00:00Z", "1995-01-01T00:00:00Z"],
        "race_concept_id": [8527, 8527, 8515],
        "ethnicity_concept_id": [38003564, 38003564, 38003563],
    }
    df = pd.DataFrame(valid_data)

    # Create dummy data file to verify SHA-256 computation
    test_csv = str(tmp_path / "test_patient_data.csv")
    df.to_csv(test_csv, index=False)

    sha256_hash = compute_sha256(test_csv)
    assert len(sha256_hash) == 64

    # Run evaluate_data_contract
    exp_name = "test_integration_gxp_governance"
    res = evaluate_data_contract(
        df,
        rules_path=rules_path,
        experiment_name=exp_name,
        dataset_source_path=test_csv,
        strict=False,
    )

    assert res["success"] is True
    assert res["evaluated_expectations"] > 0
    assert res["unsuccessful_expectations"] == 0
    assert res["success_rate"] == 100.0

    # Verify MLflow recorded experiment and active/last run
    exp = mlflow.get_experiment_by_name(exp_name)
    assert exp is not None

    runs = mlflow.search_runs(experiment_ids=[exp.experiment_id])
    assert len(runs) > 0
    latest_run = runs.iloc[0]

    assert "metrics.expectation_success_rate" in latest_run
    assert latest_run["metrics.expectation_success_rate"] == 100.0
    assert "metrics.evaluated_expectations" in latest_run
    assert latest_run["metrics.evaluated_expectations"] > 0
