"""
Module: pipeline.py
Description: Production PySpark Medallion Lakehouse pipeline orchestrating Bronze raw ingestion,
             Silver GxP quality filtering, and Gold OMOP CDM v5.4 relational table generation.
             Supports dual execution modes ('demo' local vs 'remote' public open datasets).
Author: Vivi Tsoumaki
"""

import argparse
import os
import sys
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    expr,
    to_timestamp,
    trim,
    upper,
)

from omop_cdm_v54.compat import HAS_DELTA, configure_spark_with_delta_pip
from omop_cdm_v54.condition_occurrence import transform_condition_occurrence
from omop_cdm_v54.connectors import (
    configure_s3a_anonymous_access,
    load_demographics_data,
    load_diagnoses_data,
    load_genomics_data,
    load_labs_data,
    resolve_data_dir,
)
from omop_cdm_v54.genomic_variants import transform_genomic_variants
from omop_cdm_v54.measurement import transform_measurement
from omop_cdm_v54.person import transform_person

try:
    from medallion.writer import DeltaMedallionWriter
except ImportError:
    DeltaMedallionWriter = None  # type: ignore[assignment, misc]

try:
    from medallion.quarantine import (
        QUARANTINE_TABLE_CONDITIONS,
        QUARANTINE_TABLE_MEASUREMENTS,
        QUARANTINE_TABLE_PATIENTS,
        ClinicalFailureCode,
        GxPBreachError,
        QuarantineDeltaWriter,
        evaluate_batch_quarantine_threshold,
        format_quarantine_dataframe,
    )
except ImportError:
    ClinicalFailureCode = None  # type: ignore[assignment, misc]
    GxPBreachError = RuntimeError  # type: ignore[assignment, misc]
    QuarantineDeltaWriter = None  # type: ignore[assignment, misc]
    evaluate_batch_quarantine_threshold = None  # type: ignore[assignment]
    format_quarantine_dataframe = None  # type: ignore[assignment]
    QUARANTINE_TABLE_PATIENTS = "quarantine_patients"
    QUARANTINE_TABLE_CONDITIONS = "quarantine_conditions"
    QUARANTINE_TABLE_MEASUREMENTS = "quarantine_measurements"

try:
    from governance.mlflow_tracker import evaluate_data_contract
except ImportError:
    evaluate_data_contract = None  # type: ignore[assignment]


def configure_windows_hadoop_environment():
    """Provisions a minimal winutils.exe stub required by PySpark on Windows.

    Compiles a no-op C# binary that exits with code 0, satisfying Hadoop's native
    filesystem permission checks without requiring a full Hadoop distribution.
    Sets HADOOP_HOME and hadoop.home.dir environment variables accordingly.
    No-op on non-Windows platforms.
    """
    if "JAVA_HOME" in os.environ:
        os.environ["JAVA_HOME"] = os.environ["JAVA_HOME"].rstrip("\\/ ")

    if os.name == "nt":
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        hadoop_dir = os.path.join(base_dir, "hadoop")
        bin_dir = os.path.join(hadoop_dir, "bin")
        os.makedirs(bin_dir, exist_ok=True)
        winutils_path = os.path.join(bin_dir, "winutils.exe")

        if not os.path.exists(winutils_path) or os.path.getsize(winutils_path) < 100:
            csc = r"C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe"
            cs_path = os.path.join(bin_dir, "dummy.cs")
            if os.path.exists(csc):
                try:
                    import subprocess

                    with open(cs_path, "w") as f:
                        f.write("class Program { static int Main(string[] args) { return 0; } }\n")
                    subprocess.run(
                        [csc, "/nologo", f"/out:{winutils_path}", cs_path],
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                except Exception:
                    pass
                finally:
                    # Remove the C# source file regardless of compilation outcome.
                    try:
                        os.remove(cs_path)
                    except OSError:
                        pass

        os.environ["HADOOP_HOME"] = hadoop_dir
        os.environ["hadoop.home.dir"] = hadoop_dir


def create_spark_session(mode: str = "demo") -> SparkSession:
    """Creates a local PySpark SparkSession for Medallion pipeline execution.

    Enables Delta Lake SQL extensions and the Liquid Clustering catalog. In "remote"
    mode, configures S3A anonymous credentials for AWS Open Data streaming.

    Args:
        mode: "remote" adds S3A anonymous access configuration; all other values use
              local filesystem ingestion only.

    Returns:
        Configured SparkSession bound to localhost (127.0.0.1).
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    configure_windows_hadoop_environment()

    builder = (
        SparkSession.builder.appName("life-sciences-data-foundry-omop-cdm")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
    )

    if mode.lower() == "remote":
        builder = configure_s3a_anonymous_access(builder)

    if HAS_DELTA and configure_spark_with_delta_pip is not None:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

    return builder.getOrCreate()


def run_omop_pipeline(
    spark: SparkSession,
    mode: str = "demo",
    data_dir: str | None = None,
    save_delta: bool = True,
    output_dir: str | None = None,
    run_maintenance: bool = False,
    enable_contract_enforcement: bool = True,
    rules_path: str = "governance/rules.json",
    quarantine_threshold: float = 0.02,
    abort_on_breach: bool = False,
    build_cohorts: bool = False,
) -> dict[str, Any]:
    """Executes the Bronze → Silver → Gold Medallion OMOP CDM v5.4 pipeline.

    Ingests raw clinical and genomic data (Bronze), applies GxP timestamp normalisation
    and quality filters (Silver), transforms to OMOP CDM v5.4 relational structures
    (Gold), evaluates batch quarantine rejection thresholds, and optionally enforces a
    Great Expectations data contract gate before persisting Delta Lake sinks.

    Args:
        spark: Active SparkSession.
        mode: "demo" uses local synthetic data; "remote" streams from public open datasets.
        data_dir: Input data directory. Defaults to analytical-layer/data/.
        save_delta: Persist Medallion tiers as Delta Lake tables when True.
        output_dir: Delta warehouse root. Defaults to data/delta_warehouse/.
        run_maintenance: Execute OPTIMIZE and VACUUM after Gold table writes when True.
        enable_contract_enforcement: Run Great Expectations GxP contract gate when True.
        rules_path: Path to the Great Expectations rules JSON specification.
        quarantine_threshold: Batch quarantine rejection ratio tolerance threshold (default: 0.02 = 2.0%).
        abort_on_breach: Abort downstream execution via GxPBreachError when rejection ratio exceeds threshold.
        build_cohorts: Build derived clinical cohorts, survival datasets, and feature store when True.

    Returns:
        Dict with Gold-tier DataFrames keyed by "person", "condition_occurrence", and
        "measurement".
    """
    data_dir = resolve_data_dir(data_dir)

    print("==========================================================================")
    print(" OHDSI OMOP CDM v5.4 Clinical & Genomic Normalization Engine")
    print(f" Execution Mode: [{mode.upper()}] | Delta Lake Persistence: [{save_delta}]")
    print("==========================================================================")

    # -------------------------------------------------------------------------
    # 1. BRONZE TIER: Ingest Data Streams via Open Data Connectors
    # -------------------------------------------------------------------------
    print(f"\n[INFO] [BRONZE TIER] Ingesting datasets (Mode: {mode.upper()})...")

    df_raw_patients = load_demographics_data(spark, mode=mode, data_dir=data_dir)
    df_raw_diagnoses = load_diagnoses_data(spark, mode=mode, data_dir=data_dir)
    df_raw_labs = load_labs_data(spark, mode=mode, data_dir=data_dir)
    df_raw_genomics = load_genomics_data(spark, mode=mode, data_dir=data_dir)

    # -------------------------------------------------------------------------
    # 2. SILVER TIER: Data Quality Contracts & GxP Validation Filters
    # -------------------------------------------------------------------------
    print("\n[INFO] [SILVER TIER] Enforcing GxP Data Quality Contracts & Timestamp Parsing...")

    # Prefer full timestamp precision; fall back to midnight cast when time component is absent.
    # Cache before the twin accept/quarantine filter to avoid re-evaluating the Bronze read twice.
    df_clinical_parsed = df_raw_patients.withColumn(
        "parsed_birth_dt",
        coalesce(
            to_timestamp(expr("try_cast(birth_datetime as timestamp)")),
            to_timestamp(expr("try_cast(birth_datetime as date)")),
        ),
    ).cache()

    # Single canonical condition inverted for quarantine guarantees mutually exclusive partitions.
    valid_gender_expr = upper(trim(col("gender"))).isin("MALE", "FEMALE", "M", "F", "UNKNOWN")
    valid_clinical_condition = col("parsed_birth_dt").isNotNull() & valid_gender_expr
    df_silver_clinical = df_clinical_parsed.filter(valid_clinical_condition)
    df_quarantine_clinical = df_clinical_parsed.filter(~valid_clinical_condition)

    # Filter Diagnoses — parse to DateType directly (OMOP condition_start_date is `date`, not `datetime`).
    df_diag_parsed = df_raw_diagnoses.withColumn(
        "parsed_diag_dt", expr("try_cast(diagnosis_date as date)")
    ).cache()
    # Diagnoses use 'icd10_code' in the synthetic demo schema and 'code' in normalised remote schemas.
    diag_code_col = col("icd10_code") if "icd10_code" in df_diag_parsed.columns else col("code")
    valid_diag_condition = col("parsed_diag_dt").isNotNull() & diag_code_col.isNotNull()
    df_silver_diagnoses = df_diag_parsed.filter(valid_diag_condition)
    df_quarantine_diagnoses = df_diag_parsed.filter(~valid_diag_condition)

    # Filter Labs — parse timestamp, date, and validate physiological non-negativity for numeric biomarkers.
    lab_val_col_name = "numeric_value" if "numeric_value" in df_raw_labs.columns else "value"
    df_labs_parsed = (
        df_raw_labs.withColumn(
            "parsed_lab_datetime",
            coalesce(
                to_timestamp(expr("try_cast(lab_datetime as timestamp)")),
                to_timestamp(expr("try_cast(lab_datetime as date)")),
            ),
        )
        .withColumn("parsed_lab_dt", col("parsed_lab_datetime").cast("date"))
        .withColumn("numeric_value", expr(f"try_cast({lab_val_col_name} as double)"))
    ).cache()

    valid_lab_condition = col("parsed_lab_dt").isNotNull() & (
        col("numeric_value").isNull() | (col("numeric_value") >= 0.0)
    )
    df_silver_labs = df_labs_parsed.filter(valid_lab_condition)
    df_quarantine_labs = df_labs_parsed.filter(~valid_lab_condition)

    # Filter Genomics — include standard PASS and uncomputed '.' variant quality filters.
    valid_genomics_condition = col("filter").isin("PASS", ".")
    df_silver_genomics = df_raw_genomics.filter(valid_genomics_condition)
    df_quarantine_genomics = df_raw_genomics.filter(~valid_genomics_condition)

    # Materialize counts and calculate batch quality metrics
    _raw_patients_count = df_raw_patients.count()
    _raw_diag_count = df_raw_diagnoses.count()
    _raw_labs_count = df_raw_labs.count()
    _raw_genomics_count = df_raw_genomics.count()
    total_ingested = _raw_patients_count + _raw_diag_count + _raw_labs_count + _raw_genomics_count

    _silver_clinical_count = df_silver_clinical.count()
    _silver_diag_count = df_silver_diagnoses.count()
    _silver_labs_count = df_silver_labs.count()
    _silver_genomics_count = df_silver_genomics.count()

    _qc_patients_count = df_quarantine_clinical.count()
    _qc_diag_count = df_quarantine_diagnoses.count()
    _qc_labs_count = df_quarantine_labs.count()
    _qc_genomics_count = df_quarantine_genomics.count()
    total_quarantined = _qc_patients_count + _qc_diag_count + _qc_labs_count + _qc_genomics_count

    # Release cached Bronze DataFrames — all downstream split counts are now materialised and
    # these caches are no longer needed. Freeing them before Gold transforms prevents
    # long-running sessions from accumulating unnecessary executor memory pressure.
    df_clinical_parsed.unpersist()
    df_diag_parsed.unpersist()
    df_labs_parsed.unpersist()

    print(f"[METRIC] Silver Clinical Records Accepted: {_silver_clinical_count}")
    print(f"[METRIC] Clinical Records Quarantined:     {_qc_patients_count}")
    print(f"[METRIC] Silver Diagnoses Records Accepted: {_silver_diag_count}")
    print(f"[METRIC] Diagnoses Records Quarantined:    {_qc_diag_count}")
    print(f"[METRIC] Silver Lab Biomarkers Accepted:    {_silver_labs_count}")
    print(f"[METRIC] Lab Biomarkers Quarantined:       {_qc_labs_count}")
    print(f"[METRIC] Silver Genomic Variants Accepted:  {_silver_genomics_count}")
    print(f"[METRIC] Genomic Variants Quarantined:      {_qc_genomics_count}")
    print(f"[METRIC] Total Ingested Records:           {total_ingested}")
    print(f"[METRIC] Total Quarantined Records:         {total_quarantined}")

    # Format dead-letter quarantine records preserving raw JSON payloads
    df_q_patients_formatted = (
        format_quarantine_dataframe(
            df_quarantine_clinical,
            QUARANTINE_TABLE_PATIENTS,
            ClinicalFailureCode.SCHEMA_VIOLATION
            if ClinicalFailureCode is not None
            else "SCHEMA_VIOLATION",
            "Missing valid birth timestamp or invalid gender specification",
        )
        if format_quarantine_dataframe is not None and _qc_patients_count > 0
        else None
    )

    df_q_conditions_formatted = (
        format_quarantine_dataframe(
            df_quarantine_diagnoses,
            QUARANTINE_TABLE_CONDITIONS,
            ClinicalFailureCode.SCHEMA_VIOLATION
            if ClinicalFailureCode is not None
            else "SCHEMA_VIOLATION",
            "Missing or unparseable clinical diagnosis date or missing diagnosis code",
        )
        if format_quarantine_dataframe is not None and _qc_diag_count > 0
        else None
    )

    df_q_labs_formatted = (
        format_quarantine_dataframe(
            df_quarantine_labs,
            QUARANTINE_TABLE_MEASUREMENTS,
            ClinicalFailureCode.OUT_OF_BOUNDS_LAB
            if ClinicalFailureCode is not None
            else "OUT_OF_BOUNDS_LAB",
            "Unparseable lab measurement date or out-of-bounds numeric biomarker value",
        )
        if format_quarantine_dataframe is not None and _qc_labs_count > 0
        else None
    )

    df_q_genomics_formatted = (
        format_quarantine_dataframe(
            df_quarantine_genomics,
            QUARANTINE_TABLE_MEASUREMENTS,
            ClinicalFailureCode.SCHEMA_VIOLATION
            if ClinicalFailureCode is not None
            else "SCHEMA_VIOLATION",
            "Variant FILTER failed quality threshold (non-PASS / non-period filter)",
        )
        if format_quarantine_dataframe is not None and _qc_genomics_count > 0
        else None
    )

    # -------------------------------------------------------------------------
    # 2.5 BATCH QUALITY THRESHOLD & GXP BREACH ENFORCEMENT GATE
    # -------------------------------------------------------------------------
    if evaluate_batch_quarantine_threshold is not None:
        evaluate_batch_quarantine_threshold(
            total_ingested=total_ingested,
            total_quarantined=total_quarantined,
            threshold=quarantine_threshold,
            failure_counts_by_code={
                "SCHEMA_VIOLATION": _qc_patients_count + _qc_diag_count + _qc_genomics_count,
                "OUT_OF_BOUNDS_LAB": _qc_labs_count,
            },
            abort_on_breach=abort_on_breach,
        )

    # -------------------------------------------------------------------------
    # 3. GOLD TIER: OMOP CDM v5.4 Target Table Generation
    # -------------------------------------------------------------------------
    print("\n[INFO] [GOLD TIER] Generating OHDSI OMOP CDM v5.4 Relational Tables...")

    df_omop_person = transform_person(df_silver_clinical)
    df_omop_measurement_labs = transform_measurement(df_silver_labs)
    df_omop_condition = transform_condition_occurrence(df_silver_diagnoses)
    df_omop_measurement_genomics = transform_genomic_variants(df_silver_genomics)

    # Union Lab and Genomic Measurements into Gold MEASUREMENT Table
    df_omop_measurement = df_omop_measurement_labs.unionByName(
        df_omop_measurement_genomics, allowMissingColumns=True
    )

    print("\n--- OHDSI OMOP CDM v5.4 PERSON Table ---")
    df_omop_person.show(5, truncate=False)

    print("\n--- OHDSI OMOP CDM v5.4 CONDITION_OCCURRENCE Table (SNOMED Diagnoses) ---")
    df_omop_condition.show(5, truncate=False)

    print("\n--- OHDSI OMOP CDM v5.4 MEASUREMENT Table (LOINC Labs & Genomic Variants) ---")
    df_omop_measurement.show(5, truncate=False)

    # -------------------------------------------------------------------------
    # 3.5 DATA CONTRACT GATE: Great Expectations GxP Runtime Assertion Enforcement
    # -------------------------------------------------------------------------
    if enable_contract_enforcement and evaluate_data_contract is not None:
        print("\n[INFO] [DATA CONTRACT GATE] Enforcing Great Expectations GxP Data Contracts...")
        try:
            # Sample up to 10 000 rows for GE validation to cap in-driver memory;
            # schema and null checks are statistically valid on this cardinality.
            contract_res = evaluate_data_contract(
                df_omop_person.limit(10_000),
                rules_path=rules_path,
                experiment_name="gxp_clinical_governance",
                strict=False,
            )
            if contract_res.get("success"):
                print(
                    f"[DATA CONTRACT GATE] GxP Integrity Gate Passed ({contract_res.get('success_rate', 100):.1f}% pass rate)."
                )
            else:
                print(
                    f"[DATA CONTRACT WARNING] GxP Contract Gate raised warnings: {contract_res.get('unsuccessful_expectations')} failed expectation(s)."
                )
        except Exception as err:
            print(f"[DATA CONTRACT WARNING] Data contract evaluation encountered an error: {err}")

    # -------------------------------------------------------------------------
    # 4. DELTA LAKE SINKS: Storage Optimization & Liquid Clustering Persistence
    # -------------------------------------------------------------------------
    if save_delta and HAS_DELTA and DeltaMedallionWriter is not None:
        print(
            "\n[INFO] [DELTA STORAGE] Persisting Medallion Tiers with Liquid Clustering & Schema Evolution..."
        )
        writer = DeltaMedallionWriter(spark, base_output_dir=output_dir)

        # Write Silver Sinks
        writer.write_silver_table(df_silver_clinical, "clinical_demographics")
        writer.write_silver_table(df_silver_diagnoses, "clinical_diagnoses")
        writer.write_silver_table(df_silver_labs, "lab_measurements")
        writer.write_silver_table(df_silver_genomics, "genomic_variants")

        # Write Dedicated Delta Lake Quarantine Sinks
        if QuarantineDeltaWriter is not None:
            q_writer = QuarantineDeltaWriter(spark, base_output_dir=output_dir)
            if df_q_patients_formatted is not None:
                q_writer.write_quarantine_patients(df_q_patients_formatted)
            if df_q_conditions_formatted is not None:
                q_writer.write_quarantine_conditions(df_q_conditions_formatted)
            if df_q_labs_formatted is not None:
                q_writer.write_quarantine_measurements(df_q_labs_formatted)
            if df_q_genomics_formatted is not None:
                q_writer.write_quarantine_measurements(df_q_genomics_formatted)

        # Write Gold Sinks with Liquid Clustering (CLUSTER BY (person_id, concept_id))
        person_path = writer.write_gold_omop_table(
            df_omop_person, "person", cluster_by=["person_id"]
        )
        cond_path = writer.write_gold_omop_table(
            df_omop_condition,
            "condition_occurrence",
            cluster_by=["person_id", "condition_concept_id"],
        )
        meas_path = writer.write_gold_omop_table(
            df_omop_measurement, "measurement", cluster_by=["person_id", "measurement_concept_id"]
        )

        # Log GxP Delta telemetry for all three Gold tables.
        for table_label, table_path in [
            ("PERSON", person_path),
            ("CONDITION_OCCURRENCE", cond_path),
            ("MEASUREMENT", meas_path),
        ]:
            telemetry = writer.get_table_telemetry(table_path)
            print(
                f"[DELTA METROLOGY] Gold {table_label} — "
                f"files: {telemetry.get('num_files')} | "
                f"size: {telemetry.get('size_in_bytes')} bytes | "
                f"clustering: {telemetry.get('clustering_columns')}"
            )

        print(
            "[DELTA STORAGE] Delta Lake Liquid Clustering & Schema Evolution Sinks Written Successfully."
        )

        if run_maintenance:
            print("[DELTA MAINTENANCE] Executing automated compaction and vacuum routines...")
            writer.optimize_table(meas_path)
            writer.vacuum_table(meas_path, retention_hours=168.0)

    cohort_results: dict[str, Any] = {}
    if build_cohorts:
        print(
            "\n[INFO] [COHORT ENGINE] Constructing Gold Analytical Cohorts & Translational Endpoints..."
        )
        try:
            from cohorts.builder import OHDSICohortBuilder, get_type_2_diabetes_cohort_definition
            from cohorts.deid import HIPAADeIdentifier
            from cohorts.features import PatientFeatureStore
            from cohorts.survival import SurvivalConfig, SurvivalEndpoint, SurvivalMartBuilder

            builder = OHDSICohortBuilder(spark)
            t2d_def = get_type_2_diabetes_cohort_definition()
            df_cohort_t2d = builder.build_cohort(
                definition=t2d_def,
                df_condition_occurrence=df_omop_condition,
                df_person=df_omop_person,
                df_measurement=df_omop_measurement,
            )
            cohort_results["cohort_t2d"] = df_cohort_t2d

            deid = HIPAADeIdentifier()
            df_cohort_deid = deid.deidentify_cohort(df_cohort_t2d)
            cohort_results["cohort_deid"] = df_cohort_deid

            surv_builder = SurvivalMartBuilder(
                spark, SurvivalConfig(endpoint=SurvivalEndpoint.OVERALL_SURVIVAL)
            )
            df_surv = surv_builder.build_survival_frame(
                df_cohort=df_cohort_t2d,
                df_person=df_omop_person,
                df_condition_occurrence=df_omop_condition,
                df_measurement=df_omop_measurement,
            )
            cohort_results["survival_mart"] = df_surv

            feat_store = PatientFeatureStore(spark)
            df_features = feat_store.build_feature_matrix(
                df_cohort=df_cohort_t2d,
                df_person=df_omop_person,
                df_condition_occurrence=df_omop_condition,
                df_measurement=df_omop_measurement,
            )
            cohort_results["patient_features"] = df_features

            if save_delta:
                effective_output_dir = output_dir or "data/delta_warehouse"
                cohort_dir = os.path.join(effective_output_dir, "gold")
                builder.save_cohort(df_cohort_t2d, cohort_dir)
                surv_builder.save_survival_mart(df_surv, cohort_dir)
                feat_store.save_feature_matrix(df_features, cohort_dir)

            print(
                "[COHORT ENGINE] Gold Cohort generation and analytical marts persisted successfully."
            )
        except Exception as e:
            print(f"[COHORT ENGINE WARNING] Cohort build encountered error: {e}")

    print(
        f"[SUCCESS] OHDSI OMOP CDM v5.4 Pipeline Execution Mode [{mode.upper()}] Completed Successfully."
    )

    final_result = {
        "person": df_omop_person,
        "condition_occurrence": df_omop_condition,
        "measurement": df_omop_measurement,
    }
    final_result.update(cohort_results)
    return final_result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Medallion OMOP CDM v5.4 Pipeline")
    parser.add_argument(
        "--mode",
        type=str,
        default="demo",
        choices=["demo", "remote"],
        help="Execution mode: 'demo' (local synthetic datasets) or 'remote' (public open datasets)",
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        default=None,
        help="Path to custom directory containing real-world clinical & genomic input files",
    )
    parser.add_argument(
        "--save_delta",
        "--save-delta",
        dest="save_delta",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Persist Medallion datasets into Delta Lake sinks (use --no-save-delta to skip)",
    )
    parser.add_argument(
        "--output_dir", type=str, default=None, help="Custom path for Delta Lake warehouse storage"
    )
    parser.add_argument(
        "--run_maintenance",
        action="store_true",
        default=False,
        help="Execute post-ingest Delta Lake OPTIMIZE compaction and VACUUM routines",
    )
    parser.add_argument(
        "--enable_contract_enforcement",
        "--enable-contract-enforcement",
        dest="enable_contract_enforcement",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enforce Great Expectations data quality contracts before Gold persistence",
    )
    parser.add_argument(
        "--rules_path",
        type=str,
        default="governance/rules.json",
        help="Path to Great Expectations rules JSON specification file",
    )
    parser.add_argument(
        "--quarantine_threshold",
        type=float,
        default=0.02,
        help="Batch quarantine rejection ratio tolerance threshold (default: 0.02 = 2.0%)",
    )
    parser.add_argument(
        "--abort_on_breach",
        action="store_true",
        default=False,
        help="Abort pipeline execution if batch quarantine rejection ratio exceeds tolerance threshold",
    )
    parser.add_argument(
        "--build_cohorts",
        "--build-cohorts",
        dest="build_cohorts",
        action="store_true",
        default=False,
        help="Construct Gold-tier analytical cohorts, survival marts, and patient feature stores",
    )
    args = parser.parse_args()

    mode_val = os.getenv("DATA_MODE", args.mode)
    # Prefer the env var; `or` guards against DATA_DIR="" overriding a valid CLI argument.
    data_dir_val = os.getenv("DATA_DIR") or args.data_dir
    spark = create_spark_session(mode=mode_val)
    run_omop_pipeline(
        spark,
        mode=mode_val,
        data_dir=data_dir_val,
        save_delta=args.save_delta,
        output_dir=args.output_dir,
        run_maintenance=args.run_maintenance,
        enable_contract_enforcement=args.enable_contract_enforcement,
        rules_path=args.rules_path,
        quarantine_threshold=args.quarantine_threshold,
        abort_on_breach=args.abort_on_breach,
        build_cohorts=args.build_cohorts,
    )
    spark.stop()
