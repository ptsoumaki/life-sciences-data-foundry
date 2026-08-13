"""
Module: condition_occurrence.py
Description: PySpark domain transformer mapping ICD-10-CM clinical diagnoses to
             SNOMED CT standard concepts in the OMOP CDM v5.4 CONDITION_OCCURRENCE table.

Public API:
    transform_condition_occurrence -- Maps Silver diagnosis rows to OMOP CONDITION_OCCURRENCE.
"""


from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs,
    col,
    concat_ws,
    lit,
    regexp_replace,
    trim,
    upper,
    when,
    xxhash64,
)

try:
    from omop_cdm_v54.vocabularies import (
        build_concept_lookup,
        get_icd10_concept_mappings,
    )
except ImportError:
    from vocabularies import build_concept_lookup, get_icd10_concept_mappings


def transform_condition_occurrence(
    df_silver_diagnoses: DataFrame,
    concept_mappings: dict[str, int] | None = None,
) -> DataFrame:
    """Transforms Silver-tier diagnosis records into OMOP CDM v5.4 CONDITION_OCCURRENCE format.

    Maps ICD-10-CM source codes to SNOMED CT standard concepts dynamically via external
    vocabulary mappings (governance/concept_mappings.json). Fields with no source equivalent
    are set to NULL per OMOP CDM v5.4 specification. Unmapped ICD-10 codes produce
    condition_concept_id = 0.

    Args:
        df_silver_diagnoses: Silver-tier DataFrame with columns: encounter_id,
            raw_patient_id, parsed_diag_dt, icd10_code, diagnosis_description.
        concept_mappings: Optional custom dictionary mapping ICD-10 codes to SNOMED CT concept IDs.
            Defaults to mappings loaded from governance/concept_mappings.json.

    Returns:
        OMOP CONDITION_OCCURRENCE DataFrame with all CDM v5.4 required columns.
    """
    mapping_dict = concept_mappings if concept_mappings is not None else get_icd10_concept_mappings()

    normalized_icd = upper(trim(col("icd10_code")))
    dotless_icd = regexp_replace(normalized_icd, "\\.", "")

    mapped_concept = build_concept_lookup(normalized_icd, mapping_dict, default_val=0)
    dotless_concept = build_concept_lookup(dotless_icd, mapping_dict, default_val=0)
    concept_id_expr = when(mapped_concept != 0, mapped_concept).otherwise(dotless_concept)

    return df_silver_diagnoses.select(
        abs(xxhash64(col("encounter_id"))).cast("long").alias("condition_occurrence_id"),
        abs(xxhash64(col("raw_patient_id"))).cast("long").alias("person_id"),
        concept_id_expr.cast("integer").alias("condition_concept_id"),
        col("parsed_diag_dt").alias("condition_start_date"),
        lit(None).cast("timestamp").alias("condition_start_datetime"),  # OMOP CDM v5.4 §5.3: NULL when source carries date only, not datetime
        lit(None).cast("date").alias("condition_end_date"),              # OMOP CDM v5.4 §5.3: NULL when end date is unknown; do not impute.
        lit(None).cast("timestamp").alias("condition_end_datetime"),    # OMOP CDM v5.4 §5.3: NULL when source carries date only, not datetime
        lit(32817).cast("integer").alias("condition_type_concept_id"),  # Concept 32817 = EHR Primary Diagnosis
        lit(None).cast("string").alias("stop_reason"),                  # OMOP CDM v5.4: NULL when source does not record a stop reason
        lit(None).cast("long").alias("provider_id"),           # OMOP CDM v5.4: NULL — no provider context in source.
        lit(None).cast("long").alias("visit_occurrence_id"),   # OMOP CDM v5.4: NULL — no visit context in source.
        concat_ws(":", col("icd10_code"), col("diagnosis_description")).cast("string").alias("condition_source_value"),
        lit(0).cast("integer").alias("condition_source_concept_id")     # OMOP CDM v5.4: INTEGER; 0 when no standard source concept mapping exists
    )
