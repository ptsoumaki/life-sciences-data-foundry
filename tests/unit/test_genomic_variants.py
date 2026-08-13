"""
Unit tests for omop_cdm_v54.genomic_variants domain transformer.
"""

from omop_cdm_v54.genomic_variants import transform_genomic_variants


def test_transform_genomic_variants_clinvar_mapping(spark):
    """Verifies parsing of ClinVar significance from VCF INFO field into OMOP standard concepts."""
    data = [
        ("chr17", 41245466, "G", "A", "rs80357906", "100.0", "PASS", "GENE=BRCA1;CLNSIG=Pathogenic", "P1", "S1"),
        ("chr13", 32906729, "A", "C", "rs80359254", "90.0", "PASS", "GENE=BRCA2;CLNSIG=Likely_pathogenic", "P2", "S2"),
        ("chr1", 123456, "C", "T", "rs1001", "80.0", "PASS", "GENE=TP53;CLNSIG=Benign", "P3", "S3"),
        ("chr2", 234567, "G", "T", "rs1002", "70.0", "PASS", "GENE=EGFR;CLNSIG=Likely_benign", "P4", "S4"),
        ("chr3", 345678, "T", "A", "rs1003", "60.0", "PASS", "GENE=KRAS;CLNSIG=Uncertain_significance", "P5", "S5"),
        ("chr4", 456789, "A", "G", "rs1004", "50.0", "PASS", "GENE=BRAF;CLNSIG=Not_provided", "P6", "S6"),
    ]
    df = spark.createDataFrame(
        data,
        ["chrom", "pos", "ref", "alt", "id", "qual", "filter", "info", "patient_id", "sample_id"]
    )

    res = transform_genomic_variants(df).collect()
    concept_map = {row["value_source_value"].split(":")[-1]: row["value_as_concept_id"] for row in res}

    assert concept_map["Pathogenic"] == 4181412
    assert concept_map["Likely_pathogenic"] == 36768280
    assert concept_map["Benign"] == 4049393
    assert concept_map["Likely_benign"] == 36768279
    assert concept_map["Uncertain_significance"] == 4078249
    assert concept_map["Not_provided"] == 0


def test_transform_genomic_variants_qual_score_and_null_dates(spark):
    """Verifies QUAL score conversion, null date attributes per OMOP CDM, and concept IDs."""
    data = [
        ("chr17", 41245466, "G", "A", "rs80357906", "99.5", "PASS", "GENE=BRCA1;CLNSIG=Pathogenic", "P1", "S1"),
        ("chr13", 32906729, "A", "C", ".", ".", "LowQual", "GENE=BRCA2;CLNSIG=Benign", "P2", "S2"),
    ]
    df = spark.createDataFrame(
        data,
        ["chrom", "pos", "ref", "alt", "id", "qual", "filter", "info", "patient_id", "sample_id"]
    )

    rows = transform_genomic_variants(df).collect()

    assert rows[0]["value_as_number"] == 99.5
    assert rows[1]["value_as_number"] == 0.0  # '.' converted to 0.0

    for row in rows:
        assert row["measurement_concept_id"] == 35917873  # Genomic variant quality assessment
        assert row["measurement_type_concept_id"] == 4182210   # Lab/EHR Record
        assert row["measurement_date"] is None            # OMOP spec: NULL for global VCF dates
        assert row["measurement_datetime"] is None
        assert row["unit_source_value"] == "VCF_QUAL"


def test_transform_genomic_variants_patient_sample_id_resolution(spark):
    """Verifies fallback patient/sample column resolution when column names vary."""
    data = [
        ("chr17", 41245466, "G", "A", "rs80357906", "99.0", "PASS", "GENE=BRCA1;CLNSIG=Pathogenic", "PAT_VAR1", "SMP_VAR1")
    ]
    # Use alternative column names: 'patient' and 'sample'
    df = spark.createDataFrame(
        data,
        ["chrom", "pos", "ref", "alt", "id", "qual", "filter", "info", "patient", "sample"]
    )

    row = transform_genomic_variants(df).first()

    assert isinstance(row["measurement_id"], int)
    assert row["measurement_id"] > 0
    assert isinstance(row["person_id"], int)
    assert row["person_id"] > 0
