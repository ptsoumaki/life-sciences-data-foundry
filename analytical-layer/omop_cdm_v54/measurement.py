"""
Module: measurement.py
Description: PySpark domain transformer mapping LOINC lab biomarkers and genomic quality metrics into OMOP CDM v5.4 MEASUREMENT table.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs,
    coalesce,
    col,
    concat_ws,
    expr,
    lit,
    to_timestamp,
    trim,
    upper,
    when,
    xxhash64,
)


def transform_measurement(df_silver_labs: DataFrame) -> DataFrame:
    """
    Transforms LOINC lab observations into standard OHDSI OMOP CDM v5.4 MEASUREMENT format.

    LOINC Concept Mappings:
      4548-4  (HbA1c Blood Panel)                      -> OMOP Concept 3004410
      2345-7  (Glucose in Serum/Plasma)                -> OMOP Concept 3000483
      2093-3  (Cholesterol in Serum/Plasma)            -> OMOP Concept 3004249
      2160-0  (Creatinine in Serum/Plasma)             -> OMOP Concept 3016723
      33959-8 (Alanine Aminotransferase / ALT Serum)   -> OMOP Concept 3006923

    Args:
        df_silver_labs: Silver-tier DataFrame containing lab measurement records.
            Expected to contain columns: lab_event_id, raw_patient_id, loinc_code,
            test_name, numeric_value, unit_value, parsed_lab_dt, and optionally
            parsed_lab_datetime or lab_datetime.

    Returns:
        OMOP CDM v5.4 MEASUREMENT DataFrame.
    """
    normalized_loinc = upper(trim(col("loinc_code")))

    cols = df_silver_labs.columns
    if "parsed_lab_datetime" in cols:
        meas_datetime = col("parsed_lab_datetime").cast("timestamp")
    elif "lab_datetime" in cols:
        meas_datetime = coalesce(
            to_timestamp(expr("try_cast(lab_datetime as timestamp)")),
            to_timestamp(expr("try_cast(lab_datetime as date)")),
        )
    else:
        meas_datetime = lit(None).cast("timestamp")

    if "parsed_lab_dt" in cols:
        meas_date = col("parsed_lab_dt").cast("date")
    elif "parsed_lab_datetime" in cols:
        meas_date = col("parsed_lab_datetime").cast("date")
    elif "lab_datetime" in cols:
        meas_date = meas_datetime.cast("date")
    else:
        meas_date = lit(None).cast("date")

    return df_silver_labs.select(
        abs(xxhash64(col("lab_event_id"))).cast("long").alias("measurement_id"),
        abs(xxhash64(col("raw_patient_id"))).cast("long").alias("person_id"),
        when(normalized_loinc == "4548-4", 3004410)
        .when(normalized_loinc == "2345-7", 3000483)
        .when(normalized_loinc == "2093-3", 3004249)
        .when(normalized_loinc == "2160-0", 3016723)
        .when(normalized_loinc == "33959-8", 3006923)
        .otherwise(0).cast("integer").alias("measurement_concept_id"),
        meas_date.alias("measurement_date"),
        meas_datetime.alias("measurement_datetime"),  # OMOP CDM v5.4: timestamp when present in source; NULL when source has date only
        lit(45754907).cast("integer").alias("measurement_type_concept_id"),  # Lab Result Concept
        col("numeric_value").cast("double").alias("value_as_number"),
        lit(0).cast("integer").alias("value_as_concept_id"),
        col("unit_value").cast("string").alias("unit_source_value"),
        concat_ws(":", col("loinc_code"), col("test_name")).cast("string").alias("measurement_source_value"),
        lit(None).cast("string").alias("value_source_value")
    )
