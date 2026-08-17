"""
Package: omop_cdm_v54
Description: Modular PySpark Medallion Lakehouse pipeline mapping clinical RWE, lab biomarker panels,
             phenotype diagnoses, and genomic variant calls to standard OHDSI OMOP CDM v5.4 relational structures.
Author: Vivi Tsoumaki
"""

from .condition_occurrence import transform_condition_occurrence
from .genomic_variants import transform_genomic_variants
from .measurement import transform_measurement
from .person import transform_person
from .pipeline import create_spark_session, run_omop_pipeline
from .vocabularies import (
    get_clinvar_concept_mappings,
    get_ethnicity_concept_mappings,
    get_gender_concept_mappings,
    get_icd10_concept_mappings,
    get_loinc_concept_mappings,
    get_race_concept_mappings,
    load_concept_mappings,
)

__all__ = [
    "create_spark_session",
    "get_clinvar_concept_mappings",
    "get_ethnicity_concept_mappings",
    "get_gender_concept_mappings",
    "get_icd10_concept_mappings",
    "get_loinc_concept_mappings",
    "get_race_concept_mappings",
    "load_concept_mappings",
    "run_omop_pipeline",
    "transform_condition_occurrence",
    "transform_genomic_variants",
    "transform_measurement",
    "transform_person",
]
