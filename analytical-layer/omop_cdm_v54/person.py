"""
Module: person.py
Description: PySpark domain transformer mapping clinical patient demographics into OHDSI OMOP CDM v5.4 PERSON table.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs,
    col,
    dayofmonth,
    month,
    trim,
    upper,
    xxhash64,
    year,
)

from omop_cdm_v54.vocabularies import (
    build_concept_lookup,
    get_ethnicity_concept_mappings,
    get_gender_concept_mappings,
    get_race_concept_mappings,
)


def transform_person(
    df_silver_clinical: DataFrame,
    gender_mappings: dict[str, int] | None = None,
    race_mappings: dict[str, int] | None = None,
    ethnicity_mappings: dict[str, int] | None = None,
) -> DataFrame:
    """Transforms Silver clinical patient DataFrames into standard OHDSI OMOP CDM v5.4 PERSON format.

    Maps gender, race, and ethnicity source strings to OMOP standard concepts dynamically
    via external vocabulary configuration (governance/concept_mappings.json).

    Args:
        df_silver_clinical: Silver-tier DataFrame containing clinical patient records.
        gender_mappings: Optional custom dictionary mapping gender strings to concept IDs.
        race_mappings: Optional custom dictionary mapping race strings to concept IDs.
        ethnicity_mappings: Optional custom dictionary mapping ethnicity strings to concept IDs.

    Returns:
        OMOP CDM v5.4 PERSON DataFrame.
    """
    g_map = gender_mappings if gender_mappings is not None else get_gender_concept_mappings()
    r_map = race_mappings if race_mappings is not None else get_race_concept_mappings()
    e_map = (
        ethnicity_mappings if ethnicity_mappings is not None else get_ethnicity_concept_mappings()
    )

    normalized_gender = upper(trim(col("gender")))
    normalized_race = upper(trim(col("race")))
    normalized_ethnicity = upper(trim(col("ethnicity")))

    gender_concept_expr = build_concept_lookup(normalized_gender, g_map, default_val=0)
    race_concept_expr = build_concept_lookup(normalized_race, r_map, default_val=0)
    ethnicity_concept_expr = build_concept_lookup(normalized_ethnicity, e_map, default_val=0)

    return df_silver_clinical.select(
        abs(xxhash64(col("raw_patient_id"))).cast("long").alias("person_id"),
        gender_concept_expr.cast("integer").alias("gender_concept_id"),
        year(col("parsed_birth_dt")).cast("integer").alias("year_of_birth"),
        month(col("parsed_birth_dt")).cast("integer").alias("month_of_birth"),
        dayofmonth(col("parsed_birth_dt")).cast("integer").alias("day_of_birth"),
        col("parsed_birth_dt").alias("birth_datetime"),
        race_concept_expr.cast("integer").alias("race_concept_id"),
        ethnicity_concept_expr.cast("integer").alias("ethnicity_concept_id"),
        col("raw_patient_id").cast("string").alias("person_source_value"),
        col("gender").cast("string").alias("gender_source_value"),
        col("race").cast("string").alias("race_source_value"),
        col("ethnicity").cast("string").alias("ethnicity_source_value"),
    )
