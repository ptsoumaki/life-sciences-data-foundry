"""
Integration tests for Gold-tier analytical cohorts, OHDSI phenotyping,
HIPAA de-identification, survival marts, and patient feature stores.
"""

import os

from cohorts import (
    HIPAADeIdentifier,
    OHDSICohortBuilder,
    PatientFeatureStore,
    SurvivalConfig,
    SurvivalEndpoint,
    SurvivalMartBuilder,
    get_type_2_diabetes_cohort_definition,
)
from pyspark.sql.functions import col, datediff

from omop_cdm_v54.pipeline import run_omop_pipeline


def test_end_to_end_pipeline_with_cohort_building(spark, tmp_path):
    """Verifies pipeline execution in demo mode with --build_cohorts producing analytical cohorts."""
    output_dir = str(tmp_path / "delta_warehouse")

    res = run_omop_pipeline(
        spark,
        mode="demo",
        save_delta=True,
        output_dir=output_dir,
        enable_contract_enforcement=False,
        build_cohorts=True,
    )

    # Core OMOP tables
    assert "person" in res
    assert "condition_occurrence" in res
    assert "measurement" in res

    # Analytical Cohorts & Marts
    assert "cohort_t2d" in res
    assert "cohort_deid" in res
    assert "survival_mart" in res
    assert "patient_features" in res

    df_cohort = res["cohort_t2d"]
    df_deid = res["cohort_deid"]
    df_surv = res["survival_mart"]
    df_feat = res["patient_features"]

    assert df_deid.count() == df_cohort.count()
    assert df_surv.count() == df_cohort.count()
    assert df_feat.count() == df_cohort.count()

    # Check persistence
    gold_dir = os.path.join(output_dir, "gold")
    assert os.path.exists(os.path.join(gold_dir, "cohort"))
    assert os.path.exists(os.path.join(gold_dir, "survival_mart"))
    assert os.path.exists(os.path.join(gold_dir, "patient_feature_store"))


def test_end_to_end_translational_cohort_lifecycle(spark):
    """Verifies the complete analytical cohort lifecycle across all Phase 9 modules."""
    # 1. Run Gold OMOP pipeline to generate base tables
    res = run_omop_pipeline(
        spark,
        mode="demo",
        save_delta=False,
        enable_contract_enforcement=False,
        build_cohorts=False,
    )

    df_person = res["person"]
    df_cond = res["condition_occurrence"]
    df_meas = res["measurement"]

    # 2. Build T2D Cohort
    builder = OHDSICohortBuilder(spark)
    t2d_def = get_type_2_diabetes_cohort_definition(cohort_id=1001, min_age=18)
    df_cohort = builder.build_cohort(
        definition=t2d_def,
        df_condition_occurrence=df_cond,
        df_person=df_person,
        df_measurement=df_meas,
    )

    cohort_count = df_cohort.count()
    assert cohort_count > 0

    # Verify duration
    df_with_dur = df_cohort.withColumn(
        "duration", datediff(col("cohort_end_date"), col("cohort_start_date"))
    )
    durations = [r["duration"] for r in df_with_dur.select("duration").collect()]
    assert all(d > 0 for d in durations)

    # 3. Apply HIPAA Safe Harbor De-Identification
    deid = HIPAADeIdentifier(salt="INTEGRATION_TEST_SALT", max_shift_days=180)
    df_deid = deid.deidentify_cohort(df_cohort)

    assert df_deid.count() == cohort_count
    # Invariant: Cohort duration is exactly preserved under date shifting
    df_deid_dur = df_deid.withColumn(
        "duration", datediff(col("cohort_end_date"), col("cohort_start_date"))
    )
    deid_durations = [r["duration"] for r in df_deid_dur.select("duration").collect()]
    assert sorted(durations) == sorted(deid_durations)

    # 4. Extract Overall Survival Analytical Mart & Kaplan-Meier Curve
    surv_builder = SurvivalMartBuilder(
        spark, SurvivalConfig(endpoint=SurvivalEndpoint.OVERALL_SURVIVAL)
    )
    df_surv = surv_builder.build_survival_frame(
        df_cohort=df_cohort,
        df_person=df_person,
        df_condition_occurrence=df_cond,
        df_measurement=df_meas,
    )

    assert df_surv.count() == cohort_count
    surv_records = df_surv.collect()
    assert all(r["time_to_event_days"] >= 0 for r in surv_records)
    assert all(r["event"] in (0, 1) for r in surv_records)

    # Generate Kaplan-Meier summary
    df_km = surv_builder.compute_kaplan_meier_summary(df_surv, strata_col="stratum")
    km_records = df_km.collect()
    assert len(km_records) > 0
    assert all(0.0 <= r["survival_probability"] <= 1.0 for r in km_records)

    # 5. Build ML-Ready Patient Feature Store
    feat_store = PatientFeatureStore(spark)
    df_features = feat_store.build_feature_matrix(
        df_cohort=df_cohort,
        df_person=df_person,
        df_condition_occurrence=df_cond,
        df_measurement=df_meas,
    )

    assert df_features.count() == cohort_count
    assert "charlson_comorbidity_index" in df_features.columns
    assert "condition_count_365d" in df_features.columns
    assert "latest_hba1c" in df_features.columns
    assert "has_pathogenic_variant" in df_features.columns

    # Verify no nulls in key feature columns
    null_cci_count = df_features.filter(col("charlson_comorbidity_index").isNull()).count()
    assert null_cci_count == 0
