"""
Module: person.py
Description: PySpark domain transformer mapping clinical patient demographics into OHDSI OMOP CDM v5.4 PERSON table.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, when, year, month, dayofmonth, upper, trim


def transform_person(df_silver_clinical: DataFrame) -> DataFrame:
    """
    Transforms Silver clinical patient DataFrames into standard OHDSI OMOP CDM v5.4 PERSON format.

    OMOP Vocabulary Concept Mappings:
      Gender:     8507 = Male, 8532 = Female, 0 = Unknown
      Race:       8527 = White, 8515 = Asian, 8516 = Black, 0 = Unknown
      Ethnicity:  38003563 = Hispanic, 38003564 = Not Hispanic, 0 = Unknown
    """
    normalized_gender = upper(trim(col("gender")))
    normalized_race = upper(trim(col("race")))
    normalized_ethnicity = upper(trim(col("ethnicity")))

    return df_silver_clinical.select(
        expr("abs(hash(raw_patient_id))").cast("long").alias("person_id"),
        when(normalized_gender.isin("MALE", "M"), 8507)
        .when(normalized_gender.isin("FEMALE", "F"), 8532)
        .otherwise(0).cast("integer").alias("gender_concept_id"),
        year(col("parsed_birth_dt")).cast("integer").alias("year_of_birth"),
        month(col("parsed_birth_dt")).cast("integer").alias("month_of_birth"),
        dayofmonth(col("parsed_birth_dt")).cast("integer").alias("day_of_birth"),
        col("parsed_birth_dt").alias("birth_datetime"),
        when(normalized_race == "WHITE", 8527)
        .when(normalized_race == "ASIAN", 8515)
        .when(normalized_race == "BLACK", 8516)
        .otherwise(0).cast("integer").alias("race_concept_id"),
        when(normalized_ethnicity == "HISPANIC", 38003563)
        .when(normalized_ethnicity.isin("NOT_HISPANIC", "NON_HISPANIC"), 38003564)
        .otherwise(0).cast("integer").alias("ethnicity_concept_id"),
        col("raw_patient_id").cast("string").alias("person_source_value"),
        col("gender").cast("string").alias("gender_source_value"),
        col("race").cast("string").alias("race_source_value"),
        col("ethnicity").cast("string").alias("ethnicity_source_value")
    )
