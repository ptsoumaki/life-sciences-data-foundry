"""
Module: genomic_variants.py
Description: PySpark domain transformer mapping VCF genomic variant annotations into OMOP CDM v5.4 EPISODE and MEASUREMENT tables.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, lit, concat_ws, regexp_extract, when, coalesce


def transform_genomic_variants(df_silver_genomics: DataFrame) -> DataFrame:
    """
    Transforms VCF v4.2 genomic variant calls into OMOP CDM v5.4 MEASUREMENT table structures.

    Concept ID Mappings:
      measurement_concept_id:
        35917873 = Genomic variant quality assessment (OMOP Genomic vocabulary)

      measurement_type_concept_id:
        4182210  = Lab/EHR Record (OMOP Standard — measurement provenance)

      value_as_concept_id (ClinVar clinical significance -> OMOP Meas Value / SNOMED):
        4181412  = Pathogenic
        36768280 = Likely pathogenic
        4049393  = Benign
        36768279 = Likely benign
        4078249  = Uncertain significance
        0        = Not classified / not available

    Notes:
      - measurement_date is NULL because VCF fileDate is a global header field, not a per-variant
        attribute. Per OMOP CDM guidance, date fields must be NULL when the source date is unknown.
      - value_source_value preserves the raw ClinVar CLNSIG string for full source lineage.
    """
    # Safely handle patient_id and sample_id if present in input VCF DataFrame
    cols = df_silver_genomics.columns
    patient_col = col("patient_id") if "patient_id" in cols else lit("PAT_GENOMIC")
    sample_col = col("sample_id") if "sample_id" in cols else lit("VCF_SAMPLE")

    # Extract ClinVar clinical significance and gene symbol from VCF INFO field
    df_annotated = df_silver_genomics.withColumn(
        "gene_symbol", regexp_extract(col("info"), r"GENE=([^;]+)", 1)
    ).withColumn(
        "clinvar_sig", regexp_extract(col("info"), r"CLNSIG=([^;]+)", 1)
    ).withColumn(
        "_patient_id_ref", patient_col
    ).withColumn(
        "_sample_id_ref", sample_col
    )

    return df_annotated.select(
        expr("abs(hash(concat(_patient_id_ref, _sample_id_ref, chrom, pos, ref, alt)))").cast("long").alias("measurement_id"),
        expr("abs(hash(_patient_id_ref))").cast("long").alias("person_id"),
        lit(35917873).cast("integer").alias("measurement_concept_id"),
        lit(None).cast("date").alias("measurement_date"),       # NULL: VCF fileDate is a global header field, not per-variant; see docstring.
        lit(None).cast("timestamp").alias("measurement_datetime"),# NULL: per OMOP CDM when per-variant call date is unavailable.
        lit(4182210).cast("integer").alias("measurement_type_concept_id"),  # Lab/EHR Record
        coalesce(when(col("qual") == ".", lit(0.0)).otherwise(col("qual")).cast("double"), lit(0.0)).alias("value_as_number"),
        # ClinVar clinical significance -> OMOP Meas Value standard concepts (SNOMED-based).
        # Verify against Athena (athena.ohdsi.org) before each vocabulary refresh.
        when(col("clinvar_sig") == "Pathogenic", 4181412)
        .when(col("clinvar_sig") == "Likely_pathogenic", 36768280)
        .when(col("clinvar_sig") == "Benign", 4049393)
        .when(col("clinvar_sig") == "Likely_benign", 36768279)
        .when(col("clinvar_sig") == "Uncertain_significance", 4078249)
        .otherwise(0).cast("integer").alias("value_as_concept_id"),
        lit("VCF_QUAL").cast("string").alias("unit_source_value"),
        concat_ws(":", col("_sample_id_ref"), col("filter")).cast("string").alias("measurement_source_value"),
        concat_ws(":", col("chrom"), col("pos"), col("ref"), col("alt"), col("id"), col("gene_symbol"), col("clinvar_sig")).cast("string").alias("value_source_value")
    )
