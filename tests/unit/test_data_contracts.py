"""
Unit tests for Great Expectations Data Contract Enforcement gate and MLflow lineage auditor.
"""

import os

import pandas as pd
import pytest

from governance.mlflow_tracker import evaluate_data_contract


def test_evaluate_data_contract_valid_data():
    """Verifies that compliant OMOP clinical DataFrames pass data contract validation."""
    valid_data = {
        "person_id": [101, 102],
        "gender_concept_id": [8507, 8532],
        "year_of_birth": [1990, 1988],
        "birth_datetime": ["1990-05-12T00:00:00Z", "1988-09-23T00:00:00Z"],
        "race_concept_id": [8527, 8527],
        "ethnicity_concept_id": [38003564, 38003564],
    }
    df = pd.DataFrame(valid_data)
    rules_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "governance", "rules.json"
    )

    res = evaluate_data_contract(
        df, rules_path=rules_path, experiment_name="test_gxp_governance", strict=False
    )

    assert res["success"] is True
    assert res["evaluated_expectations"] > 0
    assert res["unsuccessful_expectations"] == 0
    assert res["success_rate"] == 100.0


def test_evaluate_data_contract_invalid_data_strict_mode():
    """Verifies that non-compliant DataFrames raise RuntimeError when strict=True."""
    invalid_data = {
        "person_id": [None, 102],  # Null primary key violates expect_column_values_to_not_be_null
        "gender_concept_id": [8507, 8532],
        "year_of_birth": [1990, 1988],
        "birth_datetime": ["1990-05-12T00:00:00Z", "1988-09-23T00:00:00Z"],
        "race_concept_id": [8527, 8527],
        "ethnicity_concept_id": [38003564, 38003564],
    }
    df = pd.DataFrame(invalid_data)
    rules_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "governance", "rules.json"
    )

    with pytest.raises(RuntimeError, match="GxP Data Contract Validation failed"):
        evaluate_data_contract(
            df, rules_path=rules_path, experiment_name="test_gxp_governance", strict=True
        )
