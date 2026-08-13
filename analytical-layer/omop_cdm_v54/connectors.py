"""
Module: connectors.py
Description: Data ingestion connectors for the Medallion OMOP CDM pipeline.
             Supports two execution modes:
               'demo'   -- reads local synthetic CSV/VCF files from analytical-layer/data/.
               'remote' -- streams from public open datasets (Synthea via GitHub HTTP,
                           1000 Genomes via AWS Open Data S3A).

Public API:
    configure_s3a_anonymous_access -- SparkSession builder config for anonymous S3A.
    read_http_csv                  -- Buffered HTTP CSV reader with local fallback.
    parse_vcf_to_dataframe         -- VCF v4.2 tab-delimited parser to PySpark DataFrame.
    load_demographics_data         -- Patient demographics ingestion (PERSON source).
    load_diagnoses_data            -- Clinical diagnosis ingestion (CONDITION_OCCURRENCE source).
    load_labs_data                 -- Lab observation ingestion (MEASUREMENT source).
    load_genomics_data             -- Genomic variant ingestion (VCF -> MEASUREMENT source).
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
    """Adds anonymous AWS S3A credentials to a SparkSession builder.

    Configures the Hadoop AWS and AWS SDK JARs, selects the anonymous
    credentials provider, and registers the S3AFileSystem implementation.
    Must be called before getOrCreate() to take effect.

    Args:
        spark_builder: An in-progress SparkSession.Builder instance.

    Returns:
        The same builder with S3A anonymous access configuration applied.
    """
    return spark_builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def read_http_csv(spark: SparkSession, url: str, fallback_path: Optional[str] = None) -> DataFrame:
    """Fetches a CSV from an HTTP/HTTPS URL and returns it as a PySpark DataFrame.

    Buffers the response body in driver memory via Pandas to avoid requiring
    distributed file access for small public datasets. Raises before buffering
    if the response would exceed MAX_HTTP_RESPONSE_BYTES to prevent OOM.
    Falls back to a local file when the remote fetch fails.

    Args:
        spark: Active SparkSession.
        url: HTTP/HTTPS URL of the CSV resource.
        fallback_path: Absolute path to a local CSV file used when the remote
            fetch fails. Must exist if provided; raises FileNotFoundError otherwise.

    Returns:
        PySpark DataFrame with all columns typed as string.

    Raises:
        ValueError: Response body exceeds MAX_HTTP_RESPONSE_BYTES.
        FileNotFoundError: Remote fetch failed and no usable fallback path was given.
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
        if not fallback_path or not os.path.exists(fallback_path):
            raise FileNotFoundError(
                f"Remote fetch from {url} failed and no local fallback is available: {e}"
            ) from e
        print(f"[CONNECTOR WARNING] Remote fetch from {url} failed ({e}). Using local fallback: {fallback_path}")
        return spark.read.option("header", "true").csv(fallback_path)


def load_demographics_data(spark: SparkSession, mode: str = "demo", data_dir: Optional[str] = None) -> DataFrame:
    """Loads patient demographics into a standardised PERSON-source DataFrame.

    Demo mode reads clinical_patients.csv (columns: raw_patient_id, gender,
    birth_datetime, race, ethnicity). Remote mode streams the Synthea ETL
    patients.csv from GitHub and renames Synthea columns to the pipeline schema.

    Args:
        spark: Active SparkSession.
        mode: "remote" streams from SYNTHEA_REMOTE_PATIENTS_URL; any other value
            reads the local demo file.
        data_dir: Directory containing input files. Defaults to analytical-layer/data/.

    Returns:
        DataFrame with columns: raw_patient_id, gender, birth_datetime, race,
        ethnicity, ingestion_timestamp.
    """
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
    """Loads clinical diagnoses into a standardised CONDITION_OCCURRENCE-source DataFrame.

    Demo mode reads clinical_diagnoses.csv (columns: encounter_id, raw_patient_id,
    diagnosis_date, icd10_code, diagnosis_description). Remote mode streams the
    Synthea ETL conditions.csv from GitHub and renames Synthea columns.

    Args:
        spark: Active SparkSession.
        mode: "remote" streams from SYNTHEA_REMOTE_DIAGNOSES_URL; any other value
            reads the local demo file.
        data_dir: Directory containing input files. Defaults to analytical-layer/data/.

    Returns:
        DataFrame with columns: encounter_id, raw_patient_id, diagnosis_date,
        icd10_code, diagnosis_description, ingestion_timestamp.
    """
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
    """Loads lab observations into a standardised MEASUREMENT-source DataFrame.

    Demo mode reads lab_measurements.csv (columns: lab_event_id, raw_patient_id,
    lab_datetime, loinc_code, test_name, numeric_value, unit_value). Remote mode
    streams the Synthea ETL observations.csv from GitHub and renames columns.

    Args:
        spark: Active SparkSession.
        mode: "remote" streams from SYNTHEA_REMOTE_LABS_URL; any other value
            reads the local demo file.
        data_dir: Directory containing input files. Defaults to analytical-layer/data/.

    Returns:
        DataFrame with columns: lab_event_id, raw_patient_id, lab_datetime,
        loinc_code, test_name, numeric_value, unit_value, ingestion_timestamp.
    """
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
    """Parses a VCF v4.2 file into a PySpark DataFrame with one row per variant call.

    Reads the file as plain text, strips meta-information lines (##), resolves
    the #CHROM column header independently of partition order, then splits each
    data row on tab boundaries and projects the resulting array into named columns.

    Args:
        spark: Active SparkSession.
        vcf_path: Local filesystem path or S3A URI to the VCF file.
        max_rows: Optional row cap applied after header extraction; useful for
            limiting large remote VCF files during development.

    Returns:
        DataFrame with columns derived from the VCF #CHROM header (lowercased,
        # stripped) plus ingestion_timestamp.

    Raises:
        ValueError: No #CHROM header line found in the file.
    """
    from pyspark.sql.functions import split
    df_raw = spark.read.text(vcf_path).filter(~col("value").startswith("##"))

    # Extract the #CHROM header before applying any row limit: Spark partition
    # ordering is non-deterministic, so the header must be resolved independently.
    header_row = df_raw.filter(col("value").startswith("#CHROM")).first()
    if not header_row:
        raise ValueError(f"No #CHROM header line found in VCF: {vcf_path}")

    vcf_columns = [c.lstrip("#").lower() for c in header_row[0].split("\t")]
    df_data = df_raw.filter(~col("value").startswith("#CHROM"))
    if max_rows:
        df_data = df_data.limit(max_rows)
    df_data = df_data.withColumn("_vcf_parts", split(col("value"), "\t"))
    select_exprs = [col("_vcf_parts").getItem(i).alias(col_name) for i, col_name in enumerate(vcf_columns)]

    return df_data.select(*select_exprs).withColumn("ingestion_timestamp", current_timestamp())


def load_genomics_data(spark: SparkSession, mode: str = "demo", data_dir: Optional[str] = None) -> DataFrame:
    """Loads genomic variant calls into a MEASUREMENT-source DataFrame from a VCF file.

    Demo mode reads genomic_variants.vcf from the local data directory. Remote
    mode streams up to 1 000 rows from the 1000 Genomes Phase 3 whole-genome
    sites VCF on AWS Open Data (s3a://1000genomes), falling back to the local
    file on any S3A error.

    Args:
        spark: Active SparkSession configured with S3A when mode="remote".
        mode: "remote" streams from the 1000 Genomes S3A bucket; any other value
            reads the local demo VCF file.
        data_dir: Directory containing genomic_variants.vcf. Defaults to analytical-layer/data/.

    Returns:
        DataFrame with VCF columns (chrom, pos, id, ref, alt, qual, filter, info, …)
        plus ingestion_timestamp.
    """
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
