"""
Integration tests for end-to-end Medallion OMOP CDM v5.4 pipeline execution.
"""

import os
from omop_cdm_v54.pipeline import run_omop_pipeline


def test_run_omop_pipeline_demo_mode(spark, tmp_path):
    """
    Verifies full Medallion Lakehouse pipeline execution in 'demo' mode with Delta Lake persistence.
    """
    output_dir = str(tmp_path / "delta_warehouse")

    res = run_omop_pipeline(
        spark,
        mode="demo",
        save_delta=True,
        output_dir=output_dir,
        enable_contract_enforcement=False,
    )

    assert "person" in res
    assert "condition_occurrence" in res
    assert "measurement" in res

    assert res["person"].count() > 0
    assert res["condition_occurrence"].count() > 0
    assert res["measurement"].count() > 0

    # Verify Delta Lake table persistence on disk
    silver_dir = os.path.join(output_dir, "silver")
    gold_dir = os.path.join(output_dir, "gold")

    assert os.path.exists(os.path.join(gold_dir, "person"))
    assert os.path.exists(os.path.join(gold_dir, "condition_occurrence"))
    assert os.path.exists(os.path.join(gold_dir, "measurement"))
    assert os.path.exists(os.path.join(silver_dir, "clinical_demographics"))


def test_run_omop_pipeline_remote_mode(spark):
    """
    Verifies full pipeline execution in 'remote' open data mode.
    """
    res = run_omop_pipeline(
        spark,
        mode="remote",
        save_delta=False,
        enable_contract_enforcement=False,
    )

    assert "person" in res
    assert "condition_occurrence" in res
    assert "measurement" in res

    assert res["person"].count() > 0
    assert res["condition_occurrence"].count() > 0
    assert res["measurement"].count() > 0


def test_run_omop_pipeline_data_contract_gate(spark, tmp_path):
    """
    Verifies pipeline execution with Great Expectations data contract enforcement gate active.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    rules_path = os.path.join(base_dir, "governance", "rules.json")

    res = run_omop_pipeline(
        spark,
        mode="demo",
        save_delta=False,
        enable_contract_enforcement=True,
        rules_path=rules_path,
    )

    assert res["person"].count() > 0
