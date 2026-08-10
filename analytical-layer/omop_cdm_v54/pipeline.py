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
from pyspark.sql.functions import col, current_timestamp, to_timestamp, lit

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


def configure_windows_hadoop_environment():
    """Sets dummy HADOOP_HOME on Windows to prevent PySpark winutils.exe missing crashes."""
    if os.name == 'nt' and "HADOOP_HOME" not in os.environ:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        hadoop_dir = os.path.join(base_dir, "hadoop")
        bin_dir = os.path.join(hadoop_dir, "bin")
        os.makedirs(bin_dir, exist_ok=True)
        winutils_path = os.path.join(bin_dir, "winutils.exe")
        if not os.path.exists(winutils_path):
            with open(winutils_path, "wb") as f:
                f.write(b"")
        os.environ["HADOOP_HOME"] = hadoop_dir
        os.environ["hadoop.home.dir"] = hadoop_dir


def create_spark_session(mode: str = "demo") -> SparkSession:
    """Initializes Spark session configured for Delta Lake extensions & S3A open data streaming."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    configure_windows_hadoop_environment()

    builder = SparkSession.builder \
        .appName("life-sciences-data-foundry-omop-cdm") \
        .config("spark.sql.shuffle.partitions", "4")

    if mode.lower() == "remote":
        builder = configure_s3a_anonymous_access(builder)

    if HAS_DELTA:
        builder = builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        return configure_spark_with_delta_pip(builder).getOrCreate()
    
    return builder.getOrCreate()


def run_omop_pipeline(spark: SparkSession, mode: str = "demo", data_dir: str = None) -> dict:
    """
    Executes the end-to-end Medallion OMOP CDM v5.4 pipeline.
    """
    if data_dir is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir = os.path.join(base_dir, "data")

    print("==========================================================================")
    print(" OHDSI OMOP CDM v5.4 Clinical & Genomic Normalization Engine")
    print(f" Execution Mode: [{mode.upper()}]")
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

    from pyspark.sql.functions import try_to_timestamp

    # Filter Demographics
    df_clinical_parsed = df_raw_patients.withColumn(
        "parsed_birth_dt", try_to_timestamp(col("birth_datetime"), lit("yyyy-MM-dd HH:mm:ss"))
    )

    df_silver_clinical = df_clinical_parsed.filter(
        col("parsed_birth_dt").isNotNull() & col("gender").isin("MALE", "FEMALE", "UNKNOWN")
    )
    df_quarantine_clinical = df_clinical_parsed.filter(
        col("parsed_birth_dt").isNull() | ~col("gender").isin("MALE", "FEMALE", "UNKNOWN")
    )

    # Filter Diagnoses
    df_silver_diagnoses = df_raw_diagnoses.withColumn(
        "parsed_diag_dt", try_to_timestamp(col("diagnosis_date"), lit("yyyy-MM-dd HH:mm:ss"))
    ).filter(col("parsed_diag_dt").isNotNull())

    # Filter Labs
    df_silver_labs = df_raw_labs.withColumn(
        "parsed_lab_dt", try_to_timestamp(col("lab_datetime"), lit("yyyy-MM-dd HH:mm:ss"))
    ).filter(col("parsed_lab_dt").isNotNull())

    # Filter Genomics
    df_silver_genomics = df_raw_genomics.filter((col("filter") == "PASS") | (col("filter") == "."))

    print(f"[METRIC] Silver Clinical Records Accepted: {df_silver_clinical.count()}")
    print(f"[METRIC] Clinical Records Quarantined:     {df_quarantine_clinical.count()}")
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
    args = parser.parse_args()

    mode_val = os.getenv("DATA_MODE", args.mode)
    data_dir_val = os.getenv("DATA_DIR", args.data_dir)
    spark = create_spark_session(mode=mode_val)
    run_omop_pipeline(spark, mode=mode_val, data_dir=data_dir_val)
    spark.stop()
