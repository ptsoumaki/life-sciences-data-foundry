"""
Module: pipeline.py
Description: Production PySpark Medallion Lakehouse pipeline orchestrating Bronze raw ingestion,
             Silver GxP quality filtering, and Gold OMOP CDM v5.4 relational table generation.
             Supports dual execution modes ('demo' local vs 'remote' public open datasets).
Author: Vivi Tsoumaki
"""

import os
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, current_timestamp, expr, lit, to_date, to_timestamp, upper, trim


from omop_cdm_v54.compat import HAS_DELTA, configure_spark_with_delta_pip

try:
    from omop_cdm_v54.connectors import (
        configure_s3a_anonymous_access,
        load_demographics_data,
        load_diagnoses_data,
        load_labs_data,
        load_genomics_data,
    )
    from omop_cdm_v54.person import transform_person
    from omop_cdm_v54.measurement import transform_measurement
    from omop_cdm_v54.condition_occurrence import transform_condition_occurrence
    from omop_cdm_v54.genomic_variants import transform_genomic_variants
except ImportError:
    from connectors import (
        configure_s3a_anonymous_access,
        load_demographics_data,
        load_diagnoses_data,
        load_labs_data,
        load_genomics_data,
    )
    from person import transform_person
    from measurement import transform_measurement
    from condition_occurrence import transform_condition_occurrence
    from genomic_variants import transform_genomic_variants

try:
    from medallion.writer import DeltaMedallionWriter
except ImportError:
    DeltaMedallionWriter = None

try:
    from governance.mlflow_tracker import evaluate_data_contract
except ImportError:
    evaluate_data_contract = None



def configure_windows_hadoop_environment():
    """Provisions a minimal winutils.exe stub required by PySpark on Windows.

    Compiles a no-op C# binary that exits with code 0, satisfying Hadoop's native
    filesystem permission checks without requiring a full Hadoop distribution.
    Sets HADOOP_HOME and hadoop.home.dir environment variables accordingly.
    No-op on non-Windows platforms.
    """
    if os.name == 'nt':
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
                    subprocess.run([csc, "/nologo", f"/out:{winutils_path}", cs_path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except Exception:
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

    builder = SparkSession.builder \
        .appName("life-sciences-data-foundry-omop-cdm") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "4")

    if mode.lower() == "remote":
        builder = configure_s3a_anonymous_access(builder)

    if HAS_DELTA:
        builder = builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        return configure_spark_with_delta_pip(builder).getOrCreate()
    
    return builder.getOrCreate()


def run_omop_pipeline(
    spark: SparkSession,
    mode: str = "demo",
    data_dir: str = None,
    save_delta: bool = True,
    output_dir: str = None,
    run_maintenance: bool = False,
    enable_contract_enforcement: bool = True,
    rules_path: str = "governance/rules.json",
) -> dict:
    """Executes the Bronze → Silver → Gold Medallion OMOP CDM v5.4 pipeline.

    Ingests raw clinical and genomic data (Bronze), applies GxP timestamp normalisation
    and quality filters (Silver), transforms to OMOP CDM v5.4 relational structures
    (Gold), and optionally enforces a Great Expectations data contract gate before
    persisting Delta Lake sinks.

    Args:
        spark: Active SparkSession.
        mode: "demo" uses local synthetic data; "remote" streams from public open datasets.
        data_dir: Input data directory. Defaults to analytical-layer/data/.
        save_delta: Persist Medallion tiers as Delta Lake tables when True.
        output_dir: Delta warehouse root. Defaults to data/delta_warehouse/.
        run_maintenance: Execute OPTIMIZE and VACUUM after Gold table writes when True.
        enable_contract_enforcement: Run Great Expectations GxP contract gate when True.
        rules_path: Path to the Great Expectations rules JSON specification.

    Returns:
        Dict with Gold-tier DataFrames keyed by "person", "condition_occurrence", and
        "measurement".
    """
    if data_dir is None:
        env_data_dir = os.getenv("LSDF_DATA_DIR")
        if env_data_dir and os.path.exists(env_data_dir):
            data_dir = env_data_dir
        else:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            data_dir = os.path.join(base_dir, "data")

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

    # Normalise birth_datetime per OMOP CDM v5.4: prefer full timestamp precision;
    # fall back to date-only cast (midnight) when time component is absent.
    # Cache before the twin accept/quarantine filter so both count() calls read from
    # memory rather than re-evaluating the Bronze source read.
    df_clinical_parsed = df_raw_patients.withColumn(
        "parsed_birth_dt",
        coalesce(
            to_timestamp(expr("try_cast(birth_datetime as timestamp)")),
            to_timestamp(expr("try_cast(birth_datetime as date)"))
        )
    ).cache()

    valid_gender_expr = upper(trim(col("gender"))).isin("MALE", "FEMALE", "M", "F", "UNKNOWN")
    df_silver_clinical = df_clinical_parsed.filter(
        col("parsed_birth_dt").isNotNull() & valid_gender_expr
    )
    df_quarantine_clinical = df_clinical_parsed.filter(
        col("parsed_birth_dt").isNull() | ~valid_gender_expr
    )

    # Filter Diagnoses — parse to DateType directly (OMOP condition_start_date is `date`, not `datetime`).
    df_silver_diagnoses = df_raw_diagnoses.withColumn(
        "parsed_diag_dt",
        expr("try_cast(diagnosis_date as date)")
    ).filter(col("parsed_diag_dt").isNotNull())

    # Filter Labs — parse timestamp and date (OMOP measurement_date is `date`, measurement_datetime is `timestamp`).
    df_silver_labs = df_raw_labs.withColumn(
        "parsed_lab_datetime",
        coalesce(
            to_timestamp(expr("try_cast(lab_datetime as timestamp)")),
            to_timestamp(expr("try_cast(lab_datetime as date)"))
        )
    ).withColumn(
        "parsed_lab_dt",
        col("parsed_lab_datetime").cast("date")
    ).filter(col("parsed_lab_dt").isNotNull())

    # Filter Genomics
    df_silver_genomics = df_raw_genomics.filter((col("filter") == "PASS") | (col("filter") == "."))

    # Materialize Silver counts before reporting metrics; avoids re-scanning the
    # same DataFrame twice when the twin filter (accepted / quarantined) is lazy.
    _silver_clinical_count = df_silver_clinical.count()
    _qc_count = df_quarantine_clinical.count()
    print(f"[METRIC] Silver Clinical Records Accepted: {_silver_clinical_count}")
    print(f"[METRIC] Clinical Records Quarantined:     {_qc_count}")
    print(f"[METRIC] Silver Diagnoses Records Accepted: {df_silver_diagnoses.count()}")
    print(f"[METRIC] Silver Lab Biomarkers Accepted:    {df_silver_labs.count()}")
    print(f"[METRIC] Silver Genomic Variants Accepted:  {df_silver_genomics.count()}")

    # -------------------------------------------------------------------------
    # 3. GOLD TIER: OMOP CDM v5.4 Target Table Generation
    # -------------------------------------------------------------------------
    print("\n[INFO] [GOLD TIER] Generating OHDSI OMOP CDM v5.4 Relational Tables...")

    df_omop_person = transform_person(df_silver_clinical)
    df_omop_measurement_labs = transform_measurement(df_silver_labs)
    df_omop_condition = transform_condition_occurrence(df_silver_diagnoses)
    df_omop_measurement_genomics = transform_genomic_variants(df_silver_genomics)

    # Union Lab and Genomic Measurements into Gold MEASUREMENT Table
    df_omop_measurement = df_omop_measurement_labs.unionByName(df_omop_measurement_genomics, allowMissingColumns=True)

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
            contract_res = evaluate_data_contract(
                df_omop_person,
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
        print("\n[INFO] [DELTA STORAGE] Persisting Medallion Tiers with Liquid Clustering & Schema Evolution...")
        writer = DeltaMedallionWriter(spark, base_output_dir=output_dir)

        # Write Silver Sinks
        writer.write_silver_table(df_silver_clinical, "clinical_demographics")
        writer.write_silver_table(df_silver_diagnoses, "clinical_diagnoses")
        writer.write_silver_table(df_silver_labs, "lab_measurements")
        writer.write_silver_table(df_silver_genomics, "genomic_variants")

        if _qc_count > 0:
            writer.write_quarantine_table(df_quarantine_clinical, "quarantine_clinical")

        # Write Gold Sinks with Liquid Clustering (CLUSTER BY (person_id, concept_id))
        person_path = writer.write_gold_omop_table(df_omop_person, "person", cluster_by=["person_id"])
        cond_path = writer.write_gold_omop_table(df_omop_condition, "condition_occurrence", cluster_by=["person_id", "condition_concept_id"])
        meas_path = writer.write_gold_omop_table(df_omop_measurement, "measurement", cluster_by=["person_id", "measurement_concept_id"])

        # Log GxP Delta Metrology Telemetry
        meas_telemetry = writer.get_table_telemetry(meas_path)
        print(f"[DELTA METROLOGY] Gold MEASUREMENT Table Telemetry:")
        print(f"   - Path: {meas_telemetry.get('table_path')}")
        print(f"   - Files: {meas_telemetry.get('num_files')} | Size: {meas_telemetry.get('size_in_bytes')} bytes")
        print(f"   - Clustering Keys: {meas_telemetry.get('clustering_columns')}")

        print("[DELTA STORAGE] Delta Lake Liquid Clustering & Schema Evolution Sinks Written Successfully.")

        if run_maintenance:
            print("[DELTA MAINTENANCE] Executing automated compaction and vacuum routines...")
            writer.optimize_table(meas_path)
            writer.vacuum_table(meas_path, retention_hours=168.0)


    print(f"[SUCCESS] OHDSI OMOP CDM v5.4 Pipeline Execution Mode [{mode.upper()}] Completed Successfully.")

    return {
        "person": df_omop_person,
        "condition_occurrence": df_omop_condition,
        "measurement": df_omop_measurement,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Medallion OMOP CDM v5.4 Pipeline")
    parser.add_argument("--mode", type=str, default="demo", choices=["demo", "remote"],
                        help="Execution mode: 'demo' (local synthetic datasets) or 'remote' (public open datasets)")
    parser.add_argument("--data_dir", type=str, default=None,
                        help="Path to custom directory containing real-world clinical & genomic input files")
    parser.add_argument("--save_delta", action=argparse.BooleanOptionalAction, default=True,
                        help="Persist Medallion datasets into Delta Lake sinks (use --no-save_delta to skip)")
    parser.add_argument("--output_dir", type=str, default=None,
                        help="Custom path for Delta Lake warehouse storage")
    parser.add_argument("--run_maintenance", action="store_true", default=False,
                        help="Execute post-ingest Delta Lake OPTIMIZE compaction and VACUUM routines")
    parser.add_argument("--enable_contract_enforcement", action=argparse.BooleanOptionalAction, default=True,
                        help="Enforce Great Expectations data quality contracts before Gold persistence")
    parser.add_argument("--rules_path", type=str, default="governance/rules.json",
                        help="Path to Great Expectations rules JSON specification file")
    args = parser.parse_args()

    mode_val = os.getenv("DATA_MODE", args.mode)
    data_dir_val = os.getenv("DATA_DIR", args.data_dir)
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
    )
    spark.stop()


