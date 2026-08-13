"""
Unit tests for omop_cdm_v54.vocabularies concept mappings and PySpark dynamic expression engine.
"""

from omop_cdm_v54.vocabularies import (
    build_concept_lookup,
    get_clinvar_concept_mappings,
    get_ethnicity_concept_mappings,
    get_gender_concept_mappings,
    get_icd10_concept_mappings,
    get_loinc_concept_mappings,
    get_race_concept_mappings,
    load_concept_mappings,
)
from pyspark.sql.functions import col


def test_load_concept_mappings_structure():
    """Verifies that concept mappings load valid dictionary mappings for all categories."""
    mappings = load_concept_mappings()
    assert "icd10_to_snomed" in mappings
    assert "loinc_to_concept" in mappings
    assert "gender_to_concept" in mappings
    assert "race_to_concept" in mappings
    assert "ethnicity_to_concept" in mappings
    assert "clinvar_to_concept" in mappings

    assert get_icd10_concept_mappings()["E11.9"] == 201826
    assert get_loinc_concept_mappings()["4548-4"] == 3004410
    assert get_gender_concept_mappings()["MALE"] == 8507
    assert get_race_concept_mappings()["WHITE"] == 8527
    assert get_ethnicity_concept_mappings()["HISPANIC"] == 38003563
    assert get_clinvar_concept_mappings()["Pathogenic"] == 4181412


def test_build_concept_lookup_spark_expression(spark):
    """Verifies dynamic PySpark map expression generation and evaluation."""
    test_mapping = {
        "_description": "Test category metadata description",
        "_comment_CODE_A": "Comment for CODE_A",
        "CODE_A": 1001,
        "CODE_B": 1002,
    }
    df = spark.createDataFrame([("CODE_A",), ("CODE_B",), ("UNMAPPED",)], ["raw_code"])

    lookup_expr = build_concept_lookup(col("raw_code"), test_mapping, default_val=0)
    result_df = df.withColumn("mapped_concept_id", lookup_expr)

    rows = {row["raw_code"]: row["mapped_concept_id"] for row in result_df.collect()}
    assert rows["CODE_A"] == 1001
    assert rows["CODE_B"] == 1002
    assert rows["UNMAPPED"] == 0
