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
    xxhash64,
)

from omop_cdm_v54.vocabularies import (
    build_concept_lookup,
    get_loinc_concept_mappings,
)


def transform_measurement(
    df_silver_labs: DataFrame,
    concept_mappings: dict[str, int] | None = None,
) -> DataFrame:
    """Transforms LOINC lab observations into standard OHDSI OMOP CDM v5.4 MEASUREMENT format.

    Maps LOINC lab biomarker codes to OMOP standard concepts dynamically via external
    vocabulary configuration (governance/concept_mappings.json).

    Args:
        df_silver_labs: Silver-tier DataFrame containing lab measurement records.
            Expected to contain columns: lab_event_id, raw_patient_id, loinc_code,
            test_name, numeric_value, unit_value, parsed_lab_dt, and optionally
            parsed_lab_datetime or lab_datetime.
        concept_mappings: Optional custom dictionary mapping LOINC codes to OMOP concept IDs.
            Defaults to mappings loaded from governance/concept_mappings.json.

    Returns:
        OMOP CDM v5.4 MEASUREMENT DataFrame.
    """
    mapping_dict = (
        concept_mappings if concept_mappings is not None else get_loinc_concept_mappings()
    )
    normalized_loinc = upper(trim(col("loinc_code")))
    meas_concept_expr = build_concept_lookup(normalized_loinc, mapping_dict, default_val=0)

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
        meas_concept_expr.cast("integer").alias("measurement_concept_id"),
        meas_date.alias("measurement_date"),
        meas_datetime.alias(
            "measurement_datetime"
        ),  # OMOP CDM v5.4: timestamp when present in source; NULL when source has date only
        lit(45754907).cast("integer").alias("measurement_type_concept_id"),  # Lab Result Concept
        col("numeric_value").cast("double").alias("value_as_number"),
        lit(0).cast("integer").alias("value_as_concept_id"),
        col("unit_value").cast("string").alias("unit_source_value"),
        concat_ws(":", col("loinc_code"), col("test_name"))
        .cast("string")
        .alias("measurement_source_value"),
        lit(None).cast("string").alias("value_source_value"),
    )
