"""
Module: condition_occurrence.py
Description: PySpark domain transformer mapping ICD-10-CM clinical diagnoses to
             SNOMED CT standard concepts in the OMOP CDM v5.4 CONDITION_OCCURRENCE table.

Public API:
    transform_condition_occurrence -- Maps Silver diagnosis rows to OMOP CONDITION_OCCURRENCE.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, lit, when, concat_ws, upper, trim, regexp_replace, xxhash64, abs


def transform_condition_occurrence(df_silver_diagnoses: DataFrame) -> DataFrame:
    """Transforms Silver-tier diagnosis records into OMOP CDM v5.4 CONDITION_OCCURRENCE format.

    Maps ICD-10-CM source codes to SNOMED CT standard concepts via an inline lookup chain.
    Fields with no source equivalent are set to NULL per OMOP CDM v5.4 specification.
    Unmapped ICD-10 codes produce condition_concept_id = 0 (non-standard; to be resolved
    via an Athena vocabulary join in production).

    ICD-10 to SNOMED CT Concept Mappings (inline; verify against Athena on vocabulary refresh):
      E11.9   (Type 2 Diabetes Mellitus)               -> SNOMED 201826
      I10     (Essential Primary Hypertension)         -> SNOMED 316866
      J45.909 (Unspecified Asthma)                     -> SNOMED 195080
      I21.9   (Acute Myocardial Infarction)            -> SNOMED 4329847
      C34.90  (Malignant Neoplasm of Lung/Bronchus)    -> SNOMED 254637

    Args:
        df_silver_diagnoses: Silver-tier DataFrame with columns: encounter_id,
            raw_patient_id, parsed_diag_dt, icd10_code, diagnosis_description.

    Returns:
        OMOP CONDITION_OCCURRENCE DataFrame with all CDM v5.4 required columns.
    """
    normalized_icd = upper(trim(col("icd10_code")))
    dotless_icd = regexp_replace(normalized_icd, "\\.", "")

    return df_silver_diagnoses.select(
        abs(xxhash64(col("encounter_id"))).cast("long").alias("condition_occurrence_id"),
        abs(xxhash64(col("raw_patient_id"))).cast("long").alias("person_id"),
        when(normalized_icd.isin("E11.9", "E119") | (dotless_icd == "E119"), 201826)
        .when(normalized_icd.isin("I10", "I10.0") | (dotless_icd == "I10"), 316866)
        .when(normalized_icd.isin("J45.909", "J45909") | (dotless_icd == "J45909"), 195080)
        .when(normalized_icd.isin("I21.9", "I219") | (dotless_icd == "I219"), 4329847)
        .when(normalized_icd.isin("C34.90", "C3490") | (dotless_icd == "C3490"), 254637)
        .otherwise(0).cast("integer").alias("condition_concept_id"),  # 0 = unmapped/non-standard; replace with Athena join in production.
        col("parsed_diag_dt").alias("condition_start_date"),
        lit(None).cast("timestamp").alias("condition_start_datetime"),  # OMOP CDM v5.4 §5.3: NULL when source carries date only, not datetime
        lit(None).cast("date").alias("condition_end_date"),              # OMOP CDM v5.4 §5.3: NULL when end date is unknown; do not impute.
        lit(None).cast("timestamp").alias("condition_end_datetime"),    # OMOP CDM v5.4 §5.3: NULL when source carries date only, not datetime
        lit(32817).cast("integer").alias("condition_type_concept_id"),  # Concept 32817 = EHR Primary Diagnosis
        lit(None).cast("string").alias("stop_reason"),                  # OMOP CDM v5.4: NULL when source does not record a stop reason
        lit(None).cast("long").alias("provider_id"),           # OMOP CDM v5.4: NULL — no provider context in source; 0 is not a valid OMOP FK value.
        lit(None).cast("long").alias("visit_occurrence_id"),   # OMOP CDM v5.4: NULL — no visit context in source; 0 is not a valid OMOP FK value.
        concat_ws(":", col("icd10_code"), col("diagnosis_description")).cast("string").alias("condition_source_value"),
        lit(0).cast("integer").alias("condition_source_concept_id")     # OMOP CDM v5.4: INTEGER; 0 when no standard source concept mapping exists
    )
