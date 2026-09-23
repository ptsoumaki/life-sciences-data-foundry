"""
Unit and integration smoke tests for Phase 13 End-to-End Analytical Showcase Notebook.
Validates execution across Bronze ingestion, Silver GxP filtering, Gold OMOP transformation,
HIPAA de-identification, Kaplan-Meier biostatistical estimation, and LangGraph audit.
"""

import os
import shutil
import tempfile

import pytest
from pyspark.sql import SparkSession

from notebooks.clinical_multiomics_showcase import (
    REPO_ROOT,
    plot_kaplan_meier_curves,
    run_bronze_ingestion,
    run_cohort_phenotyping_and_deid,
    run_gold_omop_normalization,
    run_showcase_pipeline,
    run_silver_gxp_filtration,
    run_survival_analysis,
)


@pytest.fixture
def showcase_temp_dir():
    """Provides an isolated temporary directory for showcase test outputs."""
    temp_dir = tempfile.mkdtemp(prefix="lsdf_showcase_test_")
    yield temp_dir
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_showcase_bronze_and_silver_stages(spark: SparkSession, showcase_temp_dir: str):
    """Validates Bronze ingestion and Silver GxP dead-letter quarantine routing."""
    data_dir = os.path.join(REPO_ROOT, "analytical-layer", "data")
    bronze_dfs = run_bronze_ingestion(spark, data_dir)

    assert "demographics" in bronze_dfs
    assert "diagnoses" in bronze_dfs
    assert "labs" in bronze_dfs
    assert "genomics" in bronze_dfs
    assert bronze_dfs["demographics"].count() >= 7

    silver_dfs, quarantine_dfs = run_silver_gxp_filtration(
        spark=spark,
        bronze_dfs=bronze_dfs,
        warehouse_dir=showcase_temp_dir,
        mlflow_run_id="test_run_stage_eval",
    )

    assert silver_dfs["patients"].count() >= 6
    assert quarantine_dfs["patients"].count() >= 1  # PAT_ERR is quarantined
    assert silver_dfs["diagnoses"].count() >= 5
    assert silver_dfs["labs"].count() >= 5


def test_showcase_gold_cohort_and_survival(spark: SparkSession, showcase_temp_dir: str):
    """Validates Gold OMOP normalization, cohort phenotyping, and survival analysis."""
    data_dir = os.path.join(REPO_ROOT, "analytical-layer", "data")
    bronze_dfs = run_bronze_ingestion(spark, data_dir)
    silver_dfs, _ = run_silver_gxp_filtration(
        spark=spark,
        bronze_dfs=bronze_dfs,
        warehouse_dir=showcase_temp_dir,
        mlflow_run_id="test_run_gold_eval",
    )

    gold_dfs = run_gold_omop_normalization(spark, silver_dfs, showcase_temp_dir)
    assert gold_dfs["person"].count() >= 6
    assert gold_dfs["condition_occurrence"].count() >= 5
    assert gold_dfs["measurement"].count() >= 5

    df_cohort, df_cohort_deid = run_cohort_phenotyping_and_deid(spark, gold_dfs)
    assert df_cohort.count() >= 1
    assert df_cohort_deid.count() >= 1
    assert "subject_id" in df_cohort_deid.columns

    # 1. Expanded synthetic cohort branch
    df_surv, df_km = run_survival_analysis(
        spark, gold_dfs, df_cohort, simulate_expanded_cohort=True
    )
    assert df_surv.count() == 40
    assert df_km.count() > 0

    km_rows = df_km.collect()
    for row in km_rows:
        assert 0.0 <= row["survival_probability"] <= 1.0
        assert row["standard_error"] >= 0.0
        assert row["n_at_risk"] >= 0

    # 2. Production unexpanded cohort branch (simulate_expanded_cohort=False)
    df_surv_real, df_km_real = run_survival_analysis(
        spark, gold_dfs, df_cohort, simulate_expanded_cohort=False
    )
    assert df_surv_real.count() >= 1
    assert "cohort_start_date" in df_surv_real.columns
    assert "stratum" in df_surv_real.columns
    assert df_km_real.count() > 0


def test_showcase_plot_generation(spark: SparkSession, showcase_temp_dir: str):
    """Validates that publication-grade Kaplan-Meier curves are exported to disk."""
    data_dir = os.path.join(REPO_ROOT, "analytical-layer", "data")
    bronze_dfs = run_bronze_ingestion(spark, data_dir)
    silver_dfs, _ = run_silver_gxp_filtration(
        spark=spark,
        bronze_dfs=bronze_dfs,
        warehouse_dir=showcase_temp_dir,
        mlflow_run_id="test_run_plot_eval",
    )
    gold_dfs = run_gold_omop_normalization(spark, silver_dfs, showcase_temp_dir)
    df_cohort, _ = run_cohort_phenotyping_and_deid(spark, gold_dfs)
    _, df_km = run_survival_analysis(spark, gold_dfs, df_cohort, simulate_expanded_cohort=True)

    fig_path = os.path.join(showcase_temp_dir, "test_km_plot.png")
    fig = plot_kaplan_meier_curves(df_km, output_path=fig_path, show_plot=False)

    assert fig is not None
    assert os.path.exists(fig_path)
    assert os.path.getsize(fig_path) > 1000  # Valid non-empty PNG file


def test_showcase_full_end_to_end_pipeline(spark: SparkSession, showcase_temp_dir: str):
    """Validates full execution of run_showcase_pipeline end-to-end."""
    results = run_showcase_pipeline(
        output_dir=showcase_temp_dir,
        headless=True,
        simulate_expanded_cohort=True,
        spark_session=spark,
    )

    assert results is not None
    assert "gold" in results
    assert "cohort_deid" in results
    assert "survival_frame" in results
    assert "km_summary" in results
    assert "figure_path" in results
    assert os.path.exists(results["figure_path"])
    assert results["target_evidence"].count() > 0
    assert results["target_evidence"].select("mlflow_run_id").first()[0].startswith("showcase_run_")
    assert results["audit_report"] is not None
