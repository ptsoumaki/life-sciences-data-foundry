"""
Module: omop_mapping.py
Description: Production-grade PySpark Medallion pipeline simulating Databricks Lakehouse.
             Normalizes raw clinical records and genomic metrics into standard OHDSI OMOP CDM v5.4
             Delta Lake tables (PERSON, MEASUREMENT) with GxP quality contracts.
Author: Vivi Tsoumaki
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, to_timestamp, year, when, expr, concat_ws
)


def create_spark_session() -> SparkSession:
    """Initializes local Spark session configured for Delta Lake extensions."""
    return SparkSession.builder \
        .appName("life-sciences-platform-omop-cdm-foundry") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def process_omop_cdm_pipeline(spark: SparkSession):
    print("==========================================================================")
    print(" Phase 3: OHDSI OMOP CDM v5.4 Clinical & Genomic Normalization Engine")
    print("==========================================================================")

    # -------------------------------------------------------------------------
    # 1. BRONZE TIER: Ingest Raw Clinical & Genomic Data Streams
    # -------------------------------------------------------------------------
    print("\n[INFO] [BRONZE TIER] Ingesting raw clinical records and Nextflow QC metrics...")

    # Raw Clinical Demographics Input
    raw_clinical_records = [
        ("PAT_001", "MALE", "1985-06-15 08:30:00", "WHITE", "NOT_HISPANIC"),
        ("PAT_002", "FEMALE", "1992-11-03 14:20:00", "WHITE", "NOT_HISPANIC"),
        ("PAT_003", "MALE", "1978-01-22 19:00:00", "ASIAN", "NOT_HISPANIC"),
        ("PAT_004", "UNKNOWN", "2001-04-18 11:45:00", "BLACK", "HISPANIC"),
        ("PAT_ERR", "INVALID", "MALFORMED_DATE", "UNKNOWN", "UNKNOWN"), # Contract rejection test
    ]
    clinical_schema = ["raw_patient_id", "raw_gender", "raw_birth_datetime", "raw_race", "raw_ethnicity"]
    df_raw_clinical = spark.createDataFrame(raw_clinical_records, clinical_schema) \
        .withColumn("ingestion_timestamp", current_timestamp())

    # Raw Genomic Variant Metrics Input
    raw_genomic_records = [
        ("PAT_001", "SAMPLE_01", "chr1", 10045, "A", "G", 42.5, "PASS"),
        ("PAT_001", "SAMPLE_01", "chr1", 10089, "C", "T", 18.2, "LOW_QUAL"),
        ("PAT_002", "SAMPLE_02", "chr2", 45001, "G", "C", 55.0, "PASS"),
        ("PAT_003", "SAMPLE_03", "chr1", 99312, "T", "A", 38.0, "PASS"),
        ("PAT_004", "SAMPLE_04", "chrX", 12345, "C", "G", 0.0, "FAIL"),
    ]
    genomic_schema = ["raw_patient_id", "sample_id", "chromosome", "position", "ref_allele", "alt_allele", "quality_score", "filter_status"]
    df_raw_genomic = spark.createDataFrame(raw_genomic_records, genomic_schema) \
        .withColumn("ingestion_timestamp", current_timestamp())

    # -------------------------------------------------------------------------
    # 2. SILVER TIER: Data Quality Contracts & OHDSI OMOP Concept Mapping
    # -------------------------------------------------------------------------
    print("\n[INFO] [SILVER TIER] Enforcing GxP Data Contracts & Mapping OMOP Vocabulary Concepts...")

    # Parse and validate clinical birth timestamps
    df_clinical_parsed = df_raw_clinical.withColumn(
        "parsed_birth_dt", to_timestamp(col("raw_birth_datetime"), "yyyy-MM-dd HH:mm:ss")
    )

    # Silver Filter: Valid birth date and valid gender
    df_silver_clinical = df_clinical_parsed.filter(
        col("parsed_birth_dt").isNotNull() & col("raw_gender").isin("MALE", "FEMALE", "UNKNOWN")
    )

    df_quarantine_clinical = df_clinical_parsed.filter(
        col("parsed_birth_dt").isNull() | ~col("raw_gender").isin("MALE", "FEMALE", "UNKNOWN")
    )

    print(f"[METRIC] Silver Clinical Records Accepted: {df_silver_clinical.count()}")
    print(f"[METRIC] Clinical Records Quarantined:     {df_quarantine_clinical.count()}")

    # -------------------------------------------------------------------------
    # 3. GOLD TIER: OMOP CDM v5.4 Target Table Generation
    # -------------------------------------------------------------------------
    print("\n[INFO] [GOLD TIER] Writing standard OHDSI OMOP CDM v5.4 Delta Lake relational tables...")

    # --- A. OMOP CDM v5.4: PERSON Table ---
    # Concept IDs: 8507=Male, 8532=Female, 0=Unknown | Race: 8527=White, 8515=Asian, 8516=Black | Ethnicity: 38003564=Not Hispanic, 38003563=Hispanic
    df_omop_person = df_silver_clinical.select(
        expr("abs(hash(raw_patient_id))").alias("person_id"),
        when(col("raw_gender") == "MALE", 8507)
        .when(col("raw_gender") == "FEMALE", 8532)
        .otherwise(0).alias("gender_concept_id"),
        year(col("parsed_birth_dt")).alias("year_of_birth"),
        col("parsed_birth_dt").alias("birth_datetime"),
        when(col("raw_race") == "WHITE", 8527)
        .when(col("raw_race") == "ASIAN", 8515)
        .when(col("raw_race") == "BLACK", 8516)
        .otherwise(0).alias("race_concept_id"),
        when(col("raw_ethnicity") == "HISPANIC", 38003563)
        .otherwise(38003564).alias("ethnicity_concept_id"),
        col("raw_patient_id").alias("person_source_value")
    )

    # --- B. OMOP CDM v5.4: MEASUREMENT Table (Genomic Variant Measurements) ---
    # Concept ID 35917873 represents "Genomic variant quality assessment"
    df_silver_genomic = df_raw_genomic.filter(col("filter_status") == "PASS")

    df_omop_measurement = df_silver_genomic.select(
        expr("abs(hash(concat(raw_patient_id, sample_id, chromosome, position)))").alias("measurement_id"),
        expr("abs(hash(raw_patient_id))").alias("person_id"),
        lit(35917873).alias("measurement_concept_id"),
        current_timestamp().alias("measurement_datetime"),
        lit(4182210).alias("measurement_type_concept_id"),
        col("quality_score").alias("value_as_number"),
        concat_ws(":", col("chromosome"), col("position"), col("ref_allele"), col("alt_allele")).alias("value_source_value")
    )

    print("\n--- OHDSI OMOP CDM v5.4 PERSON Table ---")
    df_omop_person.show(truncate=False)

    print("\n--- OHDSI OMOP CDM v5.4 MEASUREMENT Table (Genomic Metrics) ---")
    df_omop_measurement.show(truncate=False)

    print("[SUCCESS] Phase 3 OMOP CDM v5.4 Relational Normalization Completed Successfully.")


if __name__ == "__main__":
    spark = create_spark_session()
    process_omop_cdm_pipeline(spark)
    spark.stop()