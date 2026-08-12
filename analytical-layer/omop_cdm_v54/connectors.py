"""
Module: connectors.py
Description: Open Data Ingestion Connector supporting dual execution modes:
             - 'demo': Instant local synthetic dataset ingestion for offline testing & demos.
             - 'remote': Direct streaming ingestion from public open datasets (AWS Open Data S3 buckets & NCBI HTTP endpoints).
Author: Vivi Tsoumaki
"""

import os
import urllib.request
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp


# Public Open Data Remote URLs
CLINVAR_VCF_REMOTE_URL = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz"
SYNTHEA_REMOTE_PATIENTS_URL = "https://raw.githubusercontent.com/OHDSI/ETL-Synthea/main/inst/csv/patients.csv"
SYNTHEA_REMOTE_DIAGNOSES_URL = "https://raw.githubusercontent.com/OHDSI/ETL-Synthea/main/inst/csv/conditions.csv"
SYNTHEA_REMOTE_LABS_URL = "https://raw.githubusercontent.com/OHDSI/ETL-Synthea/main/inst/csv/observations.csv"

# Maximum allowed HTTP response body size buffered into driver memory.
# Synthea observations can reach several hundred MB; raise early rather than OOM-ing the driver.
MAX_HTTP_RESPONSE_BYTES = 256 * 1024 * 1024  # 256 MB

def configure_s3a_anonymous_access(spark_builder: SparkSession.builder) -> SparkSession.builder:
    """Configures SparkSession builder for anonymous AWS S3A open dataset streaming."""
    return spark_builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def read_http_csv(spark: SparkSession, url: str, fallback_path: Optional[str] = None) -> DataFrame:
    """Reads a remote HTTP/HTTPS CSV URL into PySpark DataFrame via Python HTTP buffer with fallback.

    Raises if the response body exceeds MAX_HTTP_RESPONSE_BYTES to protect driver memory;
    falls back to the local file path on any error.
    """
    import io
    import pandas as pd
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=30) as response:
            content = response.read(MAX_HTTP_RESPONSE_BYTES + 1)
            if len(content) > MAX_HTTP_RESPONSE_BYTES:
                raise ValueError(
                    f"Remote CSV at {url} exceeds {MAX_HTTP_RESPONSE_BYTES // (1024 * 1024)} MB "
                    "driver memory guard. Use a distributed S3A path for large files."
                )
        pdf = pd.read_csv(io.BytesIO(content), dtype=str)
        return spark.createDataFrame(pdf)
    except Exception as e:
        print(f"[CONNECTOR WARNING] Remote HTTP fetch from {url} failed ({e}). Falling back to dataset at {fallback_path}.")
        return spark.read.option("header", "true").csv(fallback_path)


def load_demographics_data(spark: SparkSession, mode: str = "demo", data_dir: Optional[str] = None) -> DataFrame:
    """Ingests patient demographics in either demo or remote open dataset mode."""
    file_path = os.path.join(data_dir, "clinical_patients.csv") if data_dir else os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clinical_patients.csv")
    if mode.lower() == "remote":
        print(f"[CONNECTOR] Streaming remote open demographics from: {SYNTHEA_REMOTE_PATIENTS_URL}")
        return read_http_csv(spark, SYNTHEA_REMOTE_PATIENTS_URL, fallback_path=file_path) \
            .withColumnRenamed("Id", "raw_patient_id") \
            .withColumnRenamed("GENDER", "gender") \
            .withColumnRenamed("BIRTHDATE", "birth_datetime") \
            .withColumnRenamed("RACE", "race") \
            .withColumnRenamed("ETHNICITY", "ethnicity") \
            .withColumn("ingestion_timestamp", current_timestamp())
    else:
        return spark.read.option("header", "true").csv(file_path) \
            .withColumn("ingestion_timestamp", current_timestamp())


def load_diagnoses_data(spark: SparkSession, mode: str = "demo", data_dir: Optional[str] = None) -> DataFrame:
    """Ingests clinical diagnoses in either demo or remote open dataset mode."""
    file_path = os.path.join(data_dir, "clinical_diagnoses.csv") if data_dir else os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clinical_diagnoses.csv")
    if mode.lower() == "remote":
        print(f"[CONNECTOR] Streaming remote open diagnoses from: {SYNTHEA_REMOTE_DIAGNOSES_URL}")
        return read_http_csv(spark, SYNTHEA_REMOTE_DIAGNOSES_URL, fallback_path=file_path) \
            .withColumnRenamed("ENCOUNTER", "encounter_id") \
            .withColumnRenamed("PATIENT", "raw_patient_id") \
            .withColumnRenamed("START", "diagnosis_date") \
            .withColumnRenamed("CODE", "icd10_code") \
            .withColumnRenamed("DESCRIPTION", "diagnosis_description") \
            .withColumn("ingestion_timestamp", current_timestamp())
    else:
        return spark.read.option("header", "true").csv(file_path) \
            .withColumn("ingestion_timestamp", current_timestamp())


def load_labs_data(spark: SparkSession, mode: str = "demo", data_dir: Optional[str] = None) -> DataFrame:
    """Ingests lab observations in either demo or remote open dataset mode."""
    file_path = os.path.join(data_dir, "lab_measurements.csv") if data_dir else os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "lab_measurements.csv")
    if mode.lower() == "remote":
        print(f"[CONNECTOR] Streaming remote open lab measurements from: {SYNTHEA_REMOTE_LABS_URL}")
        return read_http_csv(spark, SYNTHEA_REMOTE_LABS_URL, fallback_path=file_path) \
            .withColumnRenamed("Id", "lab_event_id") \
            .withColumnRenamed("PATIENT", "raw_patient_id") \
            .withColumnRenamed("DATE", "lab_datetime") \
            .withColumnRenamed("CODE", "loinc_code") \
            .withColumnRenamed("DESCRIPTION", "test_name") \
            .withColumnRenamed("VALUE", "numeric_value") \
            .withColumnRenamed("UNITS", "unit_value") \
            .withColumn("ingestion_timestamp", current_timestamp())
    else:
        return spark.read.option("header", "true").csv(file_path) \
            .withColumn("ingestion_timestamp", current_timestamp())


def parse_vcf_to_dataframe(spark: SparkSession, vcf_path: str, max_rows: Optional[int] = None) -> DataFrame:
    """Parses VCF file into a PySpark DataFrame using pure JVM DataFrame SQL operations."""
    from pyspark.sql.functions import split
    df_raw = spark.read.text(vcf_path).filter(~col("value").startswith("##"))

    # Extract the #CHROM header BEFORE applying any row limit so the header line
    # is always present regardless of partition ordering (Issue #8 fix).
    header_row = df_raw.filter(col("value").startswith("#CHROM")).first()
    if not header_row:
        raise ValueError(f"No #CHROM header line found in VCF: {vcf_path}")

    vcf_columns = [c.lstrip("#").lower() for c in header_row[0].split("\t")]
    df_data = df_raw.filter(~col("value").startswith("#CHROM"))
    if max_rows:
        df_data = df_data.limit(max_rows)
    select_exprs = [split(col("value"), "\t").getItem(i).alias(col_name) for i, col_name in enumerate(vcf_columns)]

    return df_data.select(*select_exprs).withColumn("ingestion_timestamp", current_timestamp())


def load_genomics_data(spark: SparkSession, mode: str = "demo", data_dir: Optional[str] = None) -> DataFrame:
    """Ingests VCF genomic variant calls in either demo or remote open dataset mode."""
    vcf_file = os.path.join(data_dir, "genomic_variants.vcf") if data_dir else os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "genomic_variants.vcf")
    if mode.lower() == "remote":
        print("[CONNECTOR] Accessing remote AWS Open Data 1000 Genomes / ClinVar public S3 bucket...")
        s3_vcf_path = "s3a://1000genomes/release/20130502/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz"
        print(f"[CONNECTOR] Streaming remote genomic variants sample from: {s3_vcf_path}")
        try:
            return parse_vcf_to_dataframe(spark, s3_vcf_path, max_rows=1000)
        except Exception as e:
            print(f"[CONNECTOR WARNING] Remote S3A fetch from {s3_vcf_path} failed ({e}). Falling back to dataset at {vcf_file}.")
            return parse_vcf_to_dataframe(spark, vcf_file)
    else:
        return parse_vcf_to_dataframe(spark, vcf_file)
