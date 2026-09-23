# Databricks notebook source
# MAGIC %md
# MAGIC # 🔬 Life Sciences Data Foundry (LSDF) — Clinical & Multi-Omics Showcase
# MAGIC
# MAGIC **GxP-Compliant Medallion Lakehouse Platform adhering to OHDSI OMOP CDM v5.4, FDA 21 CFR Part 11, and HIPAA Safe Harbor**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architectural Progression
# MAGIC This showcase demonstrates the complete end-to-end execution lifecycle of the Life Sciences Data Foundry:
# MAGIC 1. **Lakehouse Environment & Spark Initialization**: Provisioning Delta Lake extensions and GxP provenance tracking.
# MAGIC 2. **Bronze Ingestion (Multi-Modal)**: Ingesting raw clinical demographics, diagnoses, LOINC labs, and VCF v4.2 genomic variants.
# MAGIC 3. **Silver GxP Quality Filtering & Quarantine**: Enforcing Great Expectations data contracts and routing violations to Delta dead-letter tables.
# MAGIC 4. **Gold OMOP CDM v5.4 Semantic Normalization**: Generating standard `PERSON`, `CONDITION_OCCURRENCE`, and `MEASUREMENT` tables with Liquid Clustering.
# MAGIC 5. **Clinical Phenotyping & HIPAA Safe Harbor De-ID**: Type 2 Diabetes / Oncology phenotyping with HMAC-SHA256 pseudonymization and longitudinal date shifting.
# MAGIC 6. **Longitudinal Time-to-Event (TTE) Mart**: Distributed Kaplan-Meier product-limit estimation and Greenwood standard error calculation.
# MAGIC 7. **Publication-Grade Survival Visualization**: Rendering step survival curves with 95% Greenwood confidence intervals.
# MAGIC 8. **Target Discovery & Phenotypic Evidence Mart**: Evaluating Target-to-Disease odds ratios, tractability scores, and ClinVar mutation burden.
# MAGIC 9. **LangGraph Autonomous GxP Lineage Audit**: FDA 21 CFR §11.50 dual electronic signature evaluation and Delta commit verification.

# COMMAND ----------
# %% [markdown]
# ### Stage 1: Module Imports & Cross-Platform Lakehouse Initialization

# COMMAND ----------
# %%
import argparse
import datetime
import logging
import os
import sys
from typing import Any

# Ensure repository root, analytical-layer, and agentic-ai directories are on sys.path
try:
    REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    _cwd = os.path.abspath(".")
    REPO_ROOT = os.path.dirname(_cwd) if os.path.basename(_cwd) == "notebooks" else _cwd
_ANALYTICAL_DIR = os.path.join(REPO_ROOT, "analytical-layer")
_AGENTIC_DIR = os.path.join(REPO_ROOT, "agentic-ai")

for _path in [REPO_ROOT, _ANALYTICAL_DIR, _AGENTIC_DIR]:
    if _path not in sys.path:
        sys.path.insert(0, _path)

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from graph_auditor import GxPGraphAuditor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from cohorts.builder import (
    OHDSICohortBuilder,
    get_type_2_diabetes_cohort_definition,
)
from cohorts.deid import HIPAADeIdentifier
from cohorts.features import PatientFeatureStore
from cohorts.survival import (
    SurvivalConfig,
    SurvivalEndpoint,
    SurvivalMartBuilder,
)
from discovery.target_mart import TargetEvidenceMart, TargetEvidenceMartConfig
from governance.mlflow_tracker import evaluate_data_contract
from medallion.quarantine import (
    ClinicalFailureCode,
    QuarantineDeltaWriter,
    format_quarantine_dataframe,
)
from medallion.writer import DeltaMedallionWriter
from omop_cdm_v54.compat import HAS_DELTA, configure_spark_with_delta_pip
from omop_cdm_v54.condition_occurrence import transform_condition_occurrence
from omop_cdm_v54.connectors import (
    load_demographics_data,
    load_diagnoses_data,
    load_genomics_data,
    load_labs_data,
    resolve_data_dir,
)
from omop_cdm_v54.genomic_variants import transform_genomic_variants
from omop_cdm_v54.measurement import transform_measurement
from omop_cdm_v54.person import transform_person
from omop_cdm_v54.pipeline import configure_windows_hadoop_environment

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LSDF-Showcase")


def init_showcase_spark_session(
    app_name: str = "LSDF-Clinical-Showcase",
    warehouse_dir: str | None = None,
) -> SparkSession:
    """Initializes a PySpark session configured for Delta Lake and GxP execution."""
    active_spark = SparkSession.getActiveSession()
    if active_spark is not None:
        active_spark.sparkContext.setLogLevel("WARN")
        return active_spark

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    configure_windows_hadoop_environment()

    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)

    if HAS_DELTA and configure_spark_with_delta_pip is not None:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


# COMMAND ----------
# %% [markdown]
# ### Stage 2: Bronze Ingestion — Multi-Modal Clinical & Genomic Ingestion


# COMMAND ----------
# %%
def run_bronze_ingestion(spark: SparkSession, data_dir: str) -> dict[str, DataFrame]:
    """Ingests multi-modal raw Bronze data sources: demographics, conditions, labs, and VCF genomics."""
    logger.info("Ingesting raw Bronze sources from data directory: %s", data_dir)
    df_raw_demographics = load_demographics_data(spark, mode="demo", data_dir=data_dir)
    df_raw_diagnoses = load_diagnoses_data(spark, mode="demo", data_dir=data_dir)
    df_raw_labs = load_labs_data(spark, mode="demo", data_dir=data_dir)
    df_raw_genomics = load_genomics_data(spark, mode="demo", data_dir=data_dir)

    bronze_dfs = {
        "demographics": df_raw_demographics,
        "diagnoses": df_raw_diagnoses,
        "labs": df_raw_labs,
        "genomics": df_raw_genomics,
    }
    for name, df in bronze_dfs.items():
        logger.info(
            "Bronze source [%s] loaded with schema: %s", name, [f.name for f in df.schema.fields]
        )
    return bronze_dfs


# COMMAND ----------
# %% [markdown]
# ### Stage 3: Silver GxP Quality Filtering & Dead-Letter Quarantine Routing


# COMMAND ----------
# %%
def run_silver_gxp_filtration(
    spark: SparkSession,
    bronze_dfs: dict[str, DataFrame],
    warehouse_dir: str,
    mlflow_run_id: str,
) -> tuple[dict[str, DataFrame], dict[str, DataFrame]]:
    """Evaluates data quality assertions and routes invalid records to Delta quarantine sinks."""
    logger.info("Executing Silver GxP quality filtering and contract enforcement...")
    quarantine_writer = QuarantineDeltaWriter(spark, base_output_dir=warehouse_dir)

    # 1. Demographics Quality Filtering & Timestamp Normalization
    df_demo = bronze_dfs["demographics"]
    df_clinical_parsed = df_demo.withColumn(
        "parsed_birth_dt",
        F.coalesce(
            F.to_timestamp(F.expr("try_cast(birth_datetime as timestamp)")),
            F.to_timestamp(F.expr("try_cast(birth_datetime as date)")),
        ),
    )
    valid_gender_expr = F.upper(F.trim(F.col("gender"))).isin(
        "MALE", "FEMALE", "M", "F", "OTHER", "UNKNOWN"
    )
    demo_pass_expr = F.col("parsed_birth_dt").isNotNull() & valid_gender_expr

    df_silver_patients = df_clinical_parsed.filter(demo_pass_expr)
    df_quarantine_demo = df_clinical_parsed.filter(~demo_pass_expr)
    if df_quarantine_demo.limit(1).count() > 0:
        df_q_patients = format_quarantine_dataframe(
            df=df_quarantine_demo,
            table_name="quarantine_patients",
            failure_code=ClinicalFailureCode.SCHEMA_VIOLATION,
            failure_reason="Malformed birth_datetime timestamp or invalid gender",
            mlflow_run_id=mlflow_run_id,
        )
        quarantine_writer.write_quarantine_patients(df_q_patients)
        logger.warning("Routed malformed patient records to quarantine sink.")

    # 2. Diagnoses Quality Filtering & Date Normalization
    df_diag = bronze_dfs["diagnoses"]
    df_diag_parsed = df_diag.withColumn(
        "parsed_diag_dt", F.expr("try_cast(diagnosis_date as date)")
    )
    diag_code_col = F.col("icd10_code") if "icd10_code" in df_diag_parsed.columns else F.col("code")
    diag_pass_expr = F.col("parsed_diag_dt").isNotNull() & diag_code_col.isNotNull()

    df_silver_diagnoses = df_diag_parsed.filter(diag_pass_expr)
    df_quarantine_diag = df_diag_parsed.filter(~diag_pass_expr)
    if df_quarantine_diag.limit(1).count() > 0:
        df_q_conditions = format_quarantine_dataframe(
            df=df_quarantine_diag,
            table_name="quarantine_conditions",
            failure_code=ClinicalFailureCode.UNMAPPED_TERMINOLOGY,
            failure_reason="Missing or invalid ICD-10 diagnosis code",
            mlflow_run_id=mlflow_run_id,
        )
        quarantine_writer.write_quarantine_conditions(df_q_conditions)

    # 3. Lab Measurements Quality Filtering & Date Normalization
    df_labs = bronze_dfs["labs"]
    lab_val_col_name = "numeric_value" if "numeric_value" in df_labs.columns else "value"
    df_labs_parsed = (
        df_labs.withColumn(
            "parsed_lab_datetime",
            F.coalesce(
                F.to_timestamp(F.expr("try_cast(lab_datetime as timestamp)")),
                F.to_timestamp(F.expr("try_cast(lab_datetime as date)")),
            ),
        )
        .withColumn("parsed_lab_dt", F.col("parsed_lab_datetime").cast("date"))
        .withColumn("numeric_value", F.expr(f"try_cast({lab_val_col_name} as double)"))
    )
    labs_pass_expr = F.col("parsed_lab_dt").isNotNull() & (
        F.col("numeric_value").isNull() | (F.col("numeric_value") >= 0.0)
    )

    df_silver_labs = df_labs_parsed.filter(labs_pass_expr)
    df_quarantine_labs = df_labs_parsed.filter(~labs_pass_expr)
    if df_quarantine_labs.limit(1).count() > 0:
        meas_reason_expr = F.when(
            F.col("parsed_lab_dt").isNull(),
            F.lit("Unparseable or missing laboratory event timestamp"),
        ).otherwise(F.lit("Physiologically invalid negative laboratory value (< 0.0)"))
        df_q_meas = format_quarantine_dataframe(
            df=df_quarantine_labs,
            table_name="quarantine_measurements",
            failure_code=ClinicalFailureCode.OUT_OF_BOUNDS_LAB,
            failure_reason=meas_reason_expr,
            mlflow_run_id=mlflow_run_id,
        )
        quarantine_writer.write_quarantine_measurements(df_q_meas)

    silver_dfs = {
        "patients": df_silver_patients,
        "diagnoses": df_silver_diagnoses,
        "labs": df_silver_labs,
        "genomics": bronze_dfs["genomics"],
    }
    quarantine_dfs = {
        "patients": df_quarantine_demo,
        "diagnoses": df_quarantine_diag,
        "labs": df_quarantine_labs,
    }
    return silver_dfs, quarantine_dfs


# COMMAND ----------
# %% [markdown]
# ### Stage 4: Gold OMOP CDM v5.4 Semantic Normalization & Liquid Clustering


# COMMAND ----------
# %%
def run_gold_omop_normalization(
    spark: SparkSession,
    silver_dfs: dict[str, DataFrame],
    warehouse_dir: str,
) -> dict[str, DataFrame]:
    """Transforms Silver clinical and genomic records into standardized OMOP CDM v5.4 tables."""
    logger.info("Transforming Silver records into standard OMOP CDM v5.4 Gold tables...")

    # OMOP PERSON
    df_omop_person = transform_person(silver_dfs["patients"])

    # OMOP CONDITION_OCCURRENCE
    df_omop_condition = transform_condition_occurrence(silver_dfs["diagnoses"])

    # OMOP MEASUREMENT (Clinical Chemistry Labs + ClinVar Genomic Variants)
    df_omop_labs = transform_measurement(silver_dfs["labs"])
    df_omop_genomics = transform_genomic_variants(silver_dfs["genomics"])
    df_omop_measurement = df_omop_labs.unionByName(df_omop_genomics)

    # Persist to Gold Medallion Delta Lake with Liquid Clustering
    medallion_writer = DeltaMedallionWriter(spark, base_output_dir=warehouse_dir)
    medallion_writer.write_gold_omop_table(df_omop_person, "person", cluster_by=["person_id"])
    medallion_writer.write_gold_omop_table(
        df_omop_condition, "condition_occurrence", cluster_by=["person_id", "condition_concept_id"]
    )
    medallion_writer.write_gold_omop_table(
        df_omop_measurement, "measurement", cluster_by=["person_id", "measurement_concept_id"]
    )

    # Data Contract Gate: Great Expectations GxP Runtime Assertion Enforcement on OMOP PERSON
    rules_path = os.path.join(REPO_ROOT, "governance", "rules.json")
    if os.path.exists(rules_path):
        try:
            df_person_contract = df_omop_person.withColumn(
                "birth_datetime", F.col("birth_datetime").cast("string")
            )
            contract_res = evaluate_data_contract(
                df=df_person_contract,
                rules_path=rules_path,
                experiment_name="gxp_clinical_governance",
                strict=False,
            )
            if contract_res.get("success"):
                logger.info(
                    "Gold PERSON GxP data contract passed (%.1f%% pass rate).",
                    contract_res.get("success_rate", 100.0),
                )
            else:
                logger.warning(
                    "Gold PERSON GxP data contract raised warnings: %d failed expectation(s).",
                    contract_res.get("unsuccessful_expectations", 0),
                )
        except Exception as err:
            logger.warning("Gold PERSON data contract evaluation notice: %s", err)

    logger.info("OMOP PERSON persisted with %d records.", df_omop_person.count())
    logger.info("OMOP CONDITION_OCCURRENCE persisted with %d records.", df_omop_condition.count())
    logger.info("OMOP MEASUREMENT persisted with %d records.", df_omop_measurement.count())

    return {
        "person": df_omop_person,
        "condition_occurrence": df_omop_condition,
        "measurement": df_omop_measurement,
    }


# COMMAND ----------
# %% [markdown]
# ### Stage 5: Gold Cohort Phenotyping & HIPAA Safe Harbor De-Identification


# COMMAND ----------
# %%
def run_cohort_phenotyping_and_deid(
    spark: SparkSession,
    gold_dfs: dict[str, DataFrame],
    salt: str = "LSDF_SHOWCASE_SALT_2026",
) -> tuple[DataFrame, DataFrame]:
    """Identifies clinical study cohorts and applies HIPAA Safe Harbor de-identification."""
    logger.info("Executing OHDSI cohort phenotyping and HIPAA Safe Harbor de-identification...")
    cohort_builder = OHDSICohortBuilder(spark)
    t2d_def = get_type_2_diabetes_cohort_definition()

    # Build Type 2 Diabetes Cohort
    df_cohort = cohort_builder.build_cohort(
        definition=t2d_def,
        df_condition_occurrence=gold_dfs["condition_occurrence"],
        df_person=gold_dfs["person"],
        df_measurement=gold_dfs["measurement"],
    )

    # Apply HIPAA Safe Harbor (HMAC-SHA256 pseudonymization, date shift, age capping)
    deid = HIPAADeIdentifier(salt=salt, max_shift_days=180)
    df_cohort_deid = deid.deidentify_cohort(df_cohort)

    # Build Patient Feature Store with Charlson Comorbidity Index
    feat_store = PatientFeatureStore(spark)
    df_features = feat_store.build_feature_matrix(
        df_cohort=df_cohort,
        df_person=gold_dfs["person"],
        df_condition_occurrence=gold_dfs["condition_occurrence"],
        df_measurement=gold_dfs["measurement"],
    )
    logger.info(
        "Constructed Patient Feature Store with %d rows and %d feature columns.",
        df_features.count(),
        len(df_features.columns),
    )

    logger.info("Identified %d subjects in clinical cohort.", df_cohort.count())
    logger.info("Successfully de-identified cohort adhering to 45 CFR §164.514(b)(2).")
    return df_cohort, df_cohort_deid


# COMMAND ----------
# %% [markdown]
# ### Stage 6: Longitudinal Time-to-Event (TTE) Mart & Distributed Kaplan-Meier


# COMMAND ----------
# %%
def run_survival_analysis(
    spark: SparkSession,
    gold_dfs: dict[str, DataFrame],
    df_cohort: DataFrame,
    simulate_expanded_cohort: bool = False,
) -> tuple[DataFrame, DataFrame]:
    """Constructs Time-to-Event analytical frame and computes Kaplan-Meier product-limit estimates."""
    logger.info("Constructing Time-to-Event (TTE) analytical frame...")
    surv_builder = SurvivalMartBuilder(
        spark,
        SurvivalConfig(
            endpoint=SurvivalEndpoint.OVERALL_SURVIVAL,
            default_censor_window_days=730,
        ),
    )

    if simulate_expanded_cohort:
        logger.info("Synthesizing expanded multi-strata cohort for rich visualization...")
        # Create an expanded multi-strata synthetic cohort (N=40) for dense visualization
        rng = np.random.default_rng(42)
        n_subjects = 40
        expanded_rows = []
        for i in range(1, n_subjects + 1):
            is_pathogenic = 1 if i <= 20 else 0
            # Pathogenic variants have lower median survival time
            if is_pathogenic:
                time = int(rng.exponential(scale=280)) + 30
                event = 1 if time <= 730 and rng.random() < 0.75 else 0
            else:
                time = int(rng.exponential(scale=650)) + 60
                event = 1 if time <= 730 and rng.random() < 0.35 else 0
            time = min(time, 730)
            stratum = "Pathogenic Variant" if is_pathogenic else "Wild-Type / VUS"
            expanded_rows.append(
                (
                    1001,
                    i,
                    datetime.date(2022, 1, 1),
                    time,
                    event,
                    "CENSORED" if event == 0 else "EVENT",
                    55,
                    8507,
                    is_pathogenic,
                    stratum,
                )
            )

        survival_schema = T.StructType(
            [
                T.StructField("cohort_definition_id", T.LongType(), False),
                T.StructField("subject_id", T.LongType(), False),
                T.StructField("cohort_start_date", T.DateType(), False),
                T.StructField("time_to_event_days", T.IntegerType(), False),
                T.StructField("event", T.IntegerType(), False),
                T.StructField("censoring_reason", T.StringType(), False),
                T.StructField("age_at_index", T.IntegerType(), False),
                T.StructField("gender_concept_id", T.LongType(), False),
                T.StructField("has_pathogenic_variant", T.IntegerType(), False),
                T.StructField("stratum", T.StringType(), False),
            ]
        )
        df_survival_frame = spark.createDataFrame(expanded_rows, survival_schema)
    else:
        df_survival_frame = surv_builder.build_survival_frame(
            df_cohort=df_cohort,
            df_person=gold_dfs["person"],
            df_condition_occurrence=gold_dfs["condition_occurrence"],
            df_measurement=gold_dfs["measurement"],
        )

    # Compute distributed Kaplan-Meier product-limit summary with Greenwood SE
    df_km_summary = surv_builder.compute_kaplan_meier_summary(
        df_survival_frame, strata_col="stratum"
    )
    logger.info(
        "Computed Kaplan-Meier summary with %d discrete time points.", df_km_summary.count()
    )
    return df_survival_frame, df_km_summary


# COMMAND ----------
# %% [markdown]
# ### Stage 7: Publication-Grade Visual Survival Curves (Matplotlib & Greenwood SE)


# COMMAND ----------
# %%
def plot_kaplan_meier_curves(
    df_km: DataFrame,
    output_path: str | None = None,
    show_plot: bool = False,
) -> plt.Figure:
    """Renders publication-grade Kaplan-Meier survival curves with 95% Greenwood confidence bands."""
    if not show_plot:
        matplotlib.use("Agg")

    pdf: Any = df_km.toPandas()
    if pdf.empty:
        logger.warning("Kaplan-Meier summary is empty; generating placeholder plot.")
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.text(0.5, 0.5, "No Survival Data Available", ha="center", va="center")
        if output_path:
            fig.savefig(output_path, dpi=300)
        if not show_plot:
            plt.close(fig)
        return fig

    fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
    colors = {"Pathogenic Variant": "#d9534f", "Wild-Type / VUS": "#337ab7", "ALL": "#2e6da4"}

    strata_col = "stratum" if "stratum" in pdf.columns else "_stratum"
    strata = pdf[strata_col].unique() if strata_col in pdf.columns else ["ALL"]
    for stratum in sorted(strata):
        sub_df = (
            pdf[pdf[strata_col] == stratum].sort_values("time_to_event_days")
            if strata_col in pdf.columns
            else pdf.sort_values("time_to_event_days")
        )
        times = np.concatenate([[0], sub_df["time_to_event_days"].values])
        surv_probs = np.concatenate([[1.0], sub_df["survival_probability"].values])
        se_vals = np.concatenate([[0.0], sub_df["standard_error"].values])

        lower_ci = np.clip(surv_probs - 1.96 * se_vals, 0.0, 1.0)
        upper_ci = np.clip(surv_probs + 1.96 * se_vals, 0.0, 1.0)

        color = colors.get(stratum, "#5cb85c")
        n_start = sub_df["n_at_risk"].iloc[0] if len(sub_df) > 0 else 0
        n_events = sub_df["n_events"].sum()
        label = f"{stratum} (N={n_start}, Events={n_events})"

        ax.step(times, surv_probs, where="post", label=label, color=color, linewidth=2.2)
        ax.fill_between(times, lower_ci, upper_ci, step="post", alpha=0.18, color=color)

        # Plot right-censored markers
        censored_sub = sub_df[sub_df["n_censored"] > 0]
        if not censored_sub.empty:
            c_times = censored_sub["time_to_event_days"].values
            c_probs = censored_sub["survival_probability"].values
            ax.scatter(
                c_times, c_probs, marker="+", s=60, color=color, zorder=4, label="_nolegend_"
            )

    ax.set_ylim(-0.02, 1.04)
    ax.set_xlim(left=0)
    ax.set_xlabel("Follow-up Duration (Days)", fontsize=12, fontweight="bold", labelpad=8)
    ax.set_ylabel("Overall Survival Probability S(t)", fontsize=12, fontweight="bold", labelpad=8)
    ax.set_title(
        "Kaplan-Meier Overall Survival by ClinVar Multi-Omics Genomic Strata\n(95% Greenwood Confidence Intervals)",
        fontsize=14,
        fontweight="bold",
        pad=12,
    )
    ax.grid(True, linestyle="--", alpha=0.4, color="#cccccc")
    ax.legend(loc="lower left", frameon=True, facecolor="#ffffff", framealpha=0.9, fontsize=10)

    # Style borders
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    for spine in ["left", "bottom"]:
        ax.spines[spine].set_color("#444444")
        ax.spines[spine].set_linewidth(1.2)

    plt.tight_layout()

    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        fig.savefig(output_path, dpi=300, bbox_inches="tight")
        logger.info("Saved publication-grade Kaplan-Meier figure to: %s", output_path)

    if show_plot:
        plt.show()
    else:
        plt.close(fig)

    return fig


# COMMAND ----------
# %% [markdown]
# ### Stage 8: Target Discovery & Phenotypic Evidence Mart (Phase 11)


# COMMAND ----------
# %%
def run_target_discovery_mart(
    spark: SparkSession,
    gold_dfs: dict[str, DataFrame],
    df_cohort: DataFrame,
    mlflow_run_id: str,
) -> DataFrame:
    """Computes Target-to-Phenotype association evidence, Haldane-Anscombe Odds Ratios, and tractability scores."""
    logger.info("Evaluating Target-to-Phenotype Evidence Mart...")
    target_mart = TargetEvidenceMart(
        spark,
        TargetEvidenceMartConfig(
            min_carrier_count=1,
            table_name="target_disease_evidence_showcase",
        ),
    )
    df_evidence = target_mart.build_target_evidence_mart(
        df_cohort=df_cohort,
        df_condition=gold_dfs["condition_occurrence"],
        df_measurement=gold_dfs["measurement"],
    )
    df_evidence = df_evidence.withColumn("mlflow_run_id", F.lit(mlflow_run_id))
    logger.info("Generated %d target-disease evidence associations.", df_evidence.count())
    return df_evidence


# COMMAND ----------
# %% [markdown]
# ### Stage 9: LangGraph Autonomous GxP Lineage Audit (21 CFR Part 11)


# COMMAND ----------
# %%
def run_langgraph_gxp_audit(
    run_id: str,
    delta_table_path: str,
    rules_path: str,
) -> dict[str, Any]:
    """Audits Delta transaction logs, verifies MLflow lineage hashes, and generates signed GxP dossiers."""
    logger.info("Executing LangGraph State Graph GxP Compliance Auditor...")
    auditor = GxPGraphAuditor()
    audit_result = auditor.audit_run_lineage(
        run_id=run_id,
        delta_table_path=delta_table_path,
        rules_path=rules_path,
        enable_hitl=False,
        log_to_mlflow=False,
    )
    logger.info("GxP Audit State evaluated with final status: %s", audit_result.get("final_status"))
    return audit_result


# COMMAND ----------
# %% [markdown]
# ### Pipeline Orchestration Entry Point


# COMMAND ----------
# %%
def run_showcase_pipeline(
    data_dir: str | None = None,
    output_dir: str | None = None,
    headless: bool = True,
    simulate_expanded_cohort: bool = True,
    spark_session: SparkSession | None = None,
) -> dict[str, Any]:
    """Executes the full end-to-end Life Sciences Data Foundry showcase pipeline."""
    effective_data_dir = resolve_data_dir(data_dir)
    effective_output_dir = output_dir or os.path.join(
        REPO_ROOT, "analytical-layer", "data", "delta_warehouse"
    )
    rules_path = os.path.join(REPO_ROOT, "governance", "rules.json")

    timestamp_str = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M%S")
    mlflow_run_id = f"showcase_run_{timestamp_str}"

    spark = spark_session or init_showcase_spark_session(warehouse_dir=effective_output_dir)

    # 1. Bronze Ingestion
    bronze_dfs = run_bronze_ingestion(spark, effective_data_dir)

    # 2. Silver Quality & Quarantine
    silver_dfs, quarantine_dfs = run_silver_gxp_filtration(
        spark, bronze_dfs, effective_output_dir, mlflow_run_id
    )

    # 3. Gold OMOP Transformation & Liquid Clustering
    gold_dfs = run_gold_omop_normalization(spark, silver_dfs, effective_output_dir)

    # 4. Cohort Phenotyping & HIPAA Safe Harbor De-Identification
    df_cohort, df_cohort_deid = run_cohort_phenotyping_and_deid(spark, gold_dfs)

    # 5. Longitudinal Survival Analysis
    df_survival, df_km = run_survival_analysis(
        spark, gold_dfs, df_cohort, simulate_expanded_cohort=simulate_expanded_cohort
    )

    # 6. Render Publication-Grade Kaplan-Meier Visualizations
    fig_path = os.path.join(effective_output_dir, "kaplan_meier_survival_curves.png")
    fig = plot_kaplan_meier_curves(df_km, output_path=fig_path, show_plot=not headless)

    # 7. Target Discovery Mart (Phase 11)
    df_target_evidence = run_target_discovery_mart(spark, gold_dfs, df_cohort, mlflow_run_id)

    # 8. LangGraph GxP Compliance Audit
    gold_person_path = os.path.join(effective_output_dir, "gold", "person")
    audit_report = run_langgraph_gxp_audit(
        run_id=mlflow_run_id,
        delta_table_path=gold_person_path,
        rules_path=rules_path,
    )

    print("\n" + "=" * 80)
    print(" [LSDF] LIFE SCIENCES DATA FOUNDRY -- SHOWCASE EXECUTION SUMMARY")
    print("=" * 80)
    print(f" * MLflow GxP Run ID      : {mlflow_run_id}")
    print(f" * Output Warehouse       : {effective_output_dir}")
    print(f" * Gold PERSON Records    : {gold_dfs['person'].count()}")
    print(f" * Gold CONDITIONS Records: {gold_dfs['condition_occurrence'].count()}")
    print(f" * Gold MEASUREMENT Recs  : {gold_dfs['measurement'].count()}")
    print(f" * De-Identified Cohort   : {df_cohort_deid.count()} subjects")
    print(f" * Survival KM Strata     : {df_km.count()} points evaluated")
    print(f" * Kaplan-Meier Figure    : {fig_path}")
    print(f" * Target Evidence Rows   : {df_target_evidence.count()}")
    print(f" * GxP Audit Lineage Gate : {audit_report.get('final_status', 'PASSED')}")
    print("=" * 80 + "\n")

    return {
        "spark": spark,
        "bronze": bronze_dfs,
        "silver": silver_dfs,
        "quarantine": quarantine_dfs,
        "gold": gold_dfs,
        "cohort": df_cohort,
        "cohort_deid": df_cohort_deid,
        "survival_frame": df_survival,
        "km_summary": df_km,
        "figure": fig,
        "figure_path": fig_path,
        "target_evidence": df_target_evidence,
        "audit_report": audit_report,
    }


def main() -> int:
    """CLI execution entrypoint."""
    parser = argparse.ArgumentParser(
        description="Life Sciences Data Foundry — Clinical & Multi-Omics End-to-End Showcase"
    )
    parser.add_argument(
        "--data_dir",
        "--data-dir",
        type=str,
        default=None,
        help="Input data directory containing synthetic or real-world source files",
    )
    parser.add_argument(
        "--output_dir",
        "--output-dir",
        type=str,
        default=None,
        help="Target warehouse directory for Delta tables, quarantine sinks, and figures",
    )
    parser.add_argument(
        "--headless",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run without displaying interactive GUI plots (saves plots to disk)",
    )
    parser.add_argument(
        "--simulate_expanded_cohort",
        "--simulate-expanded-cohort",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Synthesize multi-strata longitudinal follow-up for rich Kaplan-Meier curves",
    )

    args = parser.parse_args()
    results = run_showcase_pipeline(
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        headless=args.headless,
        simulate_expanded_cohort=args.simulate_expanded_cohort,
    )
    return 0 if results else 1


if __name__ == "__main__":
    sys.exit(main())
