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
from pyspark.sql.functions import col, current_timestamp, expr, lit, to_date, to_timestamp

try:
    from delta import configure_spark_with_delta_pip
    HAS_DELTA = True
except ImportError:
    HAS_DELTA = False

try:
    from .connectors import (
        configure_s3a_anonymous_access,
        load_demographics_data,
        load_diagnoses_data,
        load_labs_data,
        load_genomics_data,
    )
    from .person import transform_person
    from .measurement import transform_measurement
    from .condition_occurrence import transform_condition_occurrence
    from .genomic_variants import transform_genomic_variants
except ImportError:
    # Fallback for direct script execution (e.g. python pipeline.py)
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
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from medallion.writer import DeltaMedallionWriter
    except ImportError:
        DeltaMedallionWriter = None



def configure_windows_hadoop_environment():
    """Sets valid dummy winutils binary on Windows returning exit code 0 for Hadoop checks."""
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
    """Initializes Spark session configured for Delta Lake extensions & S3A open data streaming."""
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
) -> dict:
    """
    Executes the end-to-end Medallion OMOP CDM v5.4 pipeline.
    """
    if data_dir is None:
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

    # Filter Demographics — apply OMOP CDM v5.4 midnight normalization convention.
    # Per OMOP §3.1 PERSON: "For data sources with date only, the time is defaulted to midnight."
    # to_date() handles any recognizable format (YYYY-MM-DD, YYYY-MM-DD HH:mm:ss, ISO 8601, etc.).
    # to_timestamp() promotes the date to 00:00:00, explicitly implementing the OMOP convention
    # rather than treating date-only values as a parse error.
    df_clinical_parsed = df_raw_patients.withColumn(
        "parsed_birth_dt",
        to_timestamp(to_date(col("birth_datetime")))
    )

    df_silver_clinical = df_clinical_parsed.filter(
        col("parsed_birth_dt").isNotNull() & col("gender").isin("MALE", "FEMALE", "UNKNOWN")
    )
    df_quarantine_clinical = df_clinical_parsed.filter(
        col("parsed_birth_dt").isNull() | ~col("gender").isin("MALE", "FEMALE", "UNKNOWN")
    )

    # Filter Diagnoses — parse to DateType directly (OMOP condition_start_date is `date`, not `datetime`).
    df_silver_diagnoses = df_raw_diagnoses.withColumn(
        "parsed_diag_dt",
        to_date(col("diagnosis_date"))
    ).filter(col("parsed_diag_dt").isNotNull())

    # Filter Labs — parse to DateType directly (OMOP measurement_date is `date`, not `datetime`).
    df_silver_labs = df_raw_labs.withColumn(
        "parsed_lab_dt",
        to_date(col("lab_datetime"))
    ).filter(col("parsed_lab_dt").isNotNull())

    # Filter Genomics
    df_silver_genomics = df_raw_genomics.filter((col("filter") == "PASS") | (col("filter") == "."))

    # Cache counts to avoid re-scanning the Silver DataFrame twice (Issue #6 fix).
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
    )
    spark.stop()

