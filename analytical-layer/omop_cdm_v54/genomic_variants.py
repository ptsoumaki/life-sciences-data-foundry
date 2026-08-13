"""
Module: genomic_variants.py
Description: PySpark domain transformer mapping VCF v4.2 genomic variant annotations
             into OMOP CDM v5.4 MEASUREMENT table records.

Public API:
    transform_genomic_variants -- Maps Silver VCF rows to OMOP MEASUREMENT.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, lit, concat_ws, regexp_extract, when, coalesce, xxhash64, abs

def transform_genomic_variants(df_silver_genomics: DataFrame) -> DataFrame:
    """Transforms Silver-tier VCF variant records into OMOP CDM v5.4 MEASUREMENT format.

    Extracts ClinVar clinical significance and gene symbol from the VCF INFO field,
    maps CLNSIG strings to OMOP standard concepts, and derives a collision-resistant
    measurement_id by hashing patient, sample, locus, allele, and rsID.

    Concept ID Mappings:
      measurement_concept_id:
        35917873 = Genomic variant quality assessment (OMOP Genomic vocabulary)

      measurement_type_concept_id:
        4182210  = Lab/EHR Record (OMOP Standard — measurement provenance)

      value_as_concept_id (ClinVar CLNSIG -> OMOP Meas Value / SNOMED):
        4181412  = Pathogenic
        36768280 = Likely pathogenic
        4049393  = Benign
        36768279 = Likely benign
        4078249  = Uncertain significance
        0        = Not classified / not available
        Verify all concept IDs against Athena (athena.ohdsi.org) before each vocabulary refresh.

    Notes:
      - measurement_date is NULL: VCF fileDate is a file-level header, not a per-variant
        attribute. Per OMOP CDM, date fields must be NULL when the source date is unknown.
      - measurement_id includes the rsID (col("id")) to avoid hash collision on multi-allelic
        sites where multiple ALT alleles share the same CHROM+POS+REF.
      - value_source_value preserves the raw ClinVar CLNSIG string for full source lineage.

    Args:
        df_silver_genomics: Silver-tier DataFrame derived from a parsed VCF file, expected
            to contain columns: chrom, pos, id, ref, alt, qual, filter, info, ingestion_timestamp,
            and optionally patient_id / sample_id.

    Returns:
        OMOP MEASUREMENT DataFrame with all CDM v5.4 required columns.
    """
    # Safely handle patient_id and sample_id if present in input VCF DataFrame
    cols_map = {c.lower(): c for c in df_silver_genomics.columns}
    patient_col_name = cols_map.get("patient_id", cols_map.get("patient", None))
    sample_col_name = cols_map.get("sample_id", cols_map.get("sample", None))

    patient_col = col(patient_col_name) if patient_col_name else lit("PAT_1001")
    sample_col = col(sample_col_name) if sample_col_name else lit("SAMPLE_01")

    # Extract ClinVar clinical significance and gene symbol from VCF INFO field
    df_annotated = df_silver_genomics.withColumn(
        "gene_symbol", regexp_extract(col("info"), r"GENE=([^;]+)", 1)
    ).withColumn(
        "clinvar_sig", regexp_extract(col("info"), r"CLNSIG=([^;]+)", 1)
    ).withColumn(
        "patient_id_ref", patient_col
    ).withColumn(
        "sample_id_ref", sample_col
    )

    return df_annotated.select(
        # Include col("id") (rsID) in the key to prevent hash collision on multi-allelic sites
        # where multiple ALT alleles share the same CHROM+POS+REF with the same patient/sample.
        abs(xxhash64(concat_ws(":", col("patient_id_ref"), col("sample_id_ref"), col("chrom"), col("pos"), col("ref"), col("alt"), col("id")))).cast("long").alias("measurement_id"),
        abs(xxhash64(col("patient_id_ref"))).cast("long").alias("person_id"),
        lit(35917873).cast("integer").alias("measurement_concept_id"),
        lit(None).cast("date").alias("measurement_date"),       # OMOP CDM: NULL — VCF fileDate is a file-level header, not a per-variant attribute.
        lit(None).cast("timestamp").alias("measurement_datetime"), # OMOP CDM: NULL — no per-variant call timestamp available in VCF format.
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
        concat_ws(":", col("sample_id_ref"), col("filter")).cast("string").alias("measurement_source_value"),
        concat_ws(":", col("chrom"), col("pos"), col("ref"), col("alt"), col("id"), col("gene_symbol"), col("clinvar_sig")).cast("string").alias("value_source_value")
    )
