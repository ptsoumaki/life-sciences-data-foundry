"""
Module: genomic_variants.py
Description: PySpark domain transformer mapping VCF v4.2 genomic variant annotations
             into OMOP CDM v5.4 MEASUREMENT table records.

Public API:
    transform_genomic_variants -- Maps Silver VCF rows to OMOP MEASUREMENT.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    lit,
    regexp_extract,
    to_date,
    to_timestamp,
    when,
    xxhash64,
)

from omop_cdm_v54.vocabularies import (
    build_concept_lookup,
    get_clinvar_concept_mappings,
)


def transform_genomic_variants(
    df_silver_genomics: DataFrame,
    concept_mappings: dict[str, int] | None = None,
    default_measurement_date: str | None = None,
) -> DataFrame:
    """Transforms Silver-tier VCF variant records into OMOP CDM v5.4 MEASUREMENT format.

    Extracts ClinVar clinical significance and gene symbol from the VCF INFO field,
    maps CLNSIG strings to OMOP standard concepts dynamically via external vocabulary
    configuration (governance/concept_mappings.json), and derives a collision-resistant
    measurement_id by hashing patient, sample, locus, allele, and rsID.

    Args:
        df_silver_genomics: Silver-tier DataFrame derived from a parsed VCF file.
        concept_mappings: Optional custom dictionary mapping ClinVar CLNSIG strings to concept IDs.
            Defaults to mappings loaded from governance/concept_mappings.json.
        default_measurement_date: Optional ISO date string (YYYY-MM-DD) assigned to measurement_date
            and measurement_datetime when sequencing collection dates are known. Defaults to None (NULL).

    Returns:
        OMOP MEASUREMENT DataFrame with all CDM v5.4 required columns.
    """
    clinvar_map = (
        concept_mappings if concept_mappings is not None else get_clinvar_concept_mappings()
    )

    # Safely handle patient_id and sample_id if present in input VCF DataFrame
    cols_map = {c.lower(): c for c in df_silver_genomics.columns}
    patient_col_name = cols_map.get("patient_id", cols_map.get("patient", None))
    sample_col_name = cols_map.get("sample_id", cols_map.get("sample", None))

    patient_col = col(patient_col_name) if patient_col_name else lit("PAT_1001")
    sample_col = col(sample_col_name) if sample_col_name else lit("SAMPLE_01")

    # Extract ClinVar clinical significance and gene symbol from VCF INFO field
    df_annotated = (
        df_silver_genomics.withColumn(
            "gene_symbol", regexp_extract(col("info"), r"GENE=([^;]+)", 1)
        )
        .withColumn("clinvar_sig", regexp_extract(col("info"), r"CLNSIG=([^;]+)", 1))
        .withColumn("patient_id_ref", patient_col)
        .withColumn("sample_id_ref", sample_col)
    )

    value_concept_expr = build_concept_lookup(col("clinvar_sig"), clinvar_map, default_val=0)

    # Safe token expressions to guarantee 7 fixed colon-delimited tokens in value_source_value
    # Format: chrom:pos:ref:alt:id:gene_symbol:clinvar_sig (prevents token shifting when ID or tags are null/empty)
    safe_id_expr = coalesce(when(col("id") == "", lit(".")).otherwise(col("id")), lit("."))
    safe_gene_expr = coalesce(
        when(col("gene_symbol") == "", lit(".")).otherwise(col("gene_symbol")), lit(".")
    )
    safe_clinvar_expr = coalesce(
        when(col("clinvar_sig") == "", lit(".")).otherwise(col("clinvar_sig")), lit(".")
    )

    meas_date_expr = (
        to_date(lit(default_measurement_date))
        if default_measurement_date is not None
        else lit(None).cast("date")
    )
    meas_datetime_expr = (
        to_timestamp(lit(default_measurement_date))
        if default_measurement_date is not None
        else lit(None).cast("timestamp")
    )

    return df_annotated.select(
        xxhash64(
            concat_ws(
                ":",
                col("patient_id_ref"),
                col("sample_id_ref"),
                col("chrom"),
                col("pos"),
                col("ref"),
                col("alt"),
                safe_id_expr,
            )
        )
        .cast("long")
        .alias("measurement_id"),
        xxhash64(col("patient_id_ref")).cast("long").alias("person_id"),
        lit(35917873).cast("integer").alias("measurement_concept_id"),
        meas_date_expr.alias("measurement_date"),
        meas_datetime_expr.alias("measurement_datetime"),
        lit(4182210).cast("integer").alias("measurement_type_concept_id"),  # Lab/EHR Record
        coalesce(
            when(col("qual") == ".", lit(0.0)).otherwise(col("qual")).cast("double"), lit(0.0)
        ).alias("value_as_number"),
        value_concept_expr.cast("integer").alias("value_as_concept_id"),
        lit("VCF_QUAL").cast("string").alias("unit_source_value"),
        concat_ws(":", col("sample_id_ref"), col("filter"))
        .cast("string")
        .alias("measurement_source_value"),
        concat_ws(
            ":",
            col("chrom"),
            col("pos"),
            col("ref"),
            col("alt"),
            safe_id_expr,
            safe_gene_expr,
            safe_clinvar_expr,
        )
        .cast("string")
        .alias("value_source_value"),
    )
