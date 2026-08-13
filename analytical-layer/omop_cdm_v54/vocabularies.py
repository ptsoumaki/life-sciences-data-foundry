"""
Module: vocabularies.py
Description: OMOP CDM v5.4 vocabulary loader and dynamic PySpark concept lookup engine.
             Loads standard concept ID mappings from external configuration (governance/concept_mappings.json)
             and constructs native PySpark map expressions for high-performance GxP concept resolution.

Public API:
    load_concept_mappings           -- Loads all vocabulary mappings from JSON or defaults.
    get_icd10_concept_mappings      -- ICD-10-CM -> SNOMED CT diagnosis concept mapping.
    get_loinc_concept_mappings      -- LOINC code -> OMOP measurement concept mapping.
    get_gender_concept_mappings     -- Gender string -> OMOP gender concept mapping.
    get_race_concept_mappings       -- Race string -> OMOP race concept mapping.
    get_ethnicity_concept_mappings  -- Ethnicity string -> OMOP ethnicity concept mapping.
    get_clinvar_concept_mappings    -- ClinVar CLNSIG -> OMOP clinical significance concept mapping.
    build_concept_lookup            -- Generates a native PySpark map lookup Column expression.
"""

import json
import os

from pyspark.sql.functions import Column, coalesce, create_map, lit

# Default fallback vocabulary concept mappings in case JSON config is unavailable
DEFAULT_ICD10_MAPPINGS: dict[str, int] = {
    "E11.9": 201826,
    "E119": 201826,
    "I10": 316866,
    "I10.0": 316866,
    "I100": 316866,
    "J45.909": 195080,
    "J45909": 195080,
    "I21.9": 4329847,
    "I219": 4329847,
    "C34.90": 254637,
    "C3490": 254637,
}

DEFAULT_LOINC_MAPPINGS: dict[str, int] = {
    "4548-4": 3004410,   # HbA1c Blood Panel
    "2345-7": 3000483,   # Glucose in Serum/Plasma
    "2093-3": 3004249,   # Cholesterol in Serum/Plasma
    "2160-0": 3016723,   # Creatinine in Serum/Plasma
    "33959-8": 3006923,  # ALT Serum
}

DEFAULT_GENDER_MAPPINGS: dict[str, int] = {
    "MALE": 8507,
    "M": 8507,
    "FEMALE": 8532,
    "F": 8532,
}

DEFAULT_RACE_MAPPINGS: dict[str, int] = {
    "WHITE": 8527,
    "ASIAN": 8515,
    "BLACK": 8516,
}

DEFAULT_ETHNICITY_MAPPINGS: dict[str, int] = {
    "HISPANIC": 38003563,
    "NOT_HISPANIC": 38003564,
    "NON_HISPANIC": 38003564,
}

DEFAULT_CLINVAR_MAPPINGS: dict[str, int] = {
    "Pathogenic": 4181412,
    "Likely_pathogenic": 36768280,
    "Benign": 4049393,
    "Likely_benign": 36768279,
    "Uncertain_significance": 4078249,
}


def _resolve_mappings_file_path(custom_path: str | None = None) -> str | None:
    """Resolves the absolute path to concept_mappings.json."""
    if custom_path and os.path.exists(custom_path):
        return custom_path

    env_path = os.getenv("LSDF_CONCEPT_MAPPINGS_PATH")
    if env_path and os.path.exists(env_path):
        return env_path

    # Check relative to analytical-layer / repo root
    pkg_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    repo_root = os.path.dirname(pkg_dir)
    candidate_paths = [
        os.path.join(repo_root, "governance", "concept_mappings.json"),
        os.path.join(pkg_dir, "governance", "concept_mappings.json"),
    ]
    for p in candidate_paths:
        if os.path.exists(p):
            return p
    return None


def load_concept_mappings(mapping_file: str | None = None) -> dict[str, dict[str, int]]:
    """Loads concept mappings from the specified JSON file or defaults.

    Args:
        mapping_file: Optional path to concept_mappings.json.

    Returns:
        Dictionary containing mapping dictionaries by category.
    """
    resolved_path = _resolve_mappings_file_path(mapping_file)
    if resolved_path and os.path.exists(resolved_path):
        try:
            with open(resolved_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return {
                    "icd10_to_snomed": data.get("icd10_to_snomed", DEFAULT_ICD10_MAPPINGS),
                    "loinc_to_concept": data.get("loinc_to_concept", DEFAULT_LOINC_MAPPINGS),
                    "gender_to_concept": data.get("gender_to_concept", DEFAULT_GENDER_MAPPINGS),
                    "race_to_concept": data.get("race_to_concept", DEFAULT_RACE_MAPPINGS),
                    "ethnicity_to_concept": data.get("ethnicity_to_concept", DEFAULT_ETHNICITY_MAPPINGS),
                    "clinvar_to_concept": data.get("clinvar_to_concept", DEFAULT_CLINVAR_MAPPINGS),
                }
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as e:
            print(f"[VOCABULARY WARNING] Failed to load {resolved_path} ({e}); using built-in defaults.")

    return {
        "icd10_to_snomed": DEFAULT_ICD10_MAPPINGS,
        "loinc_to_concept": DEFAULT_LOINC_MAPPINGS,
        "gender_to_concept": DEFAULT_GENDER_MAPPINGS,
        "race_to_concept": DEFAULT_RACE_MAPPINGS,
        "ethnicity_to_concept": DEFAULT_ETHNICITY_MAPPINGS,
        "clinvar_to_concept": DEFAULT_CLINVAR_MAPPINGS,
    }


def get_icd10_concept_mappings(mapping_file: str | None = None) -> dict[str, int]:
    """Returns ICD-10-CM to SNOMED CT standard concept ID mappings."""
    return load_concept_mappings(mapping_file)["icd10_to_snomed"]


def get_loinc_concept_mappings(mapping_file: str | None = None) -> dict[str, int]:
    """Returns LOINC to OMOP measurement standard concept ID mappings."""
    return load_concept_mappings(mapping_file)["loinc_to_concept"]


def get_gender_concept_mappings(mapping_file: str | None = None) -> dict[str, int]:
    """Returns gender string to OMOP concept ID mappings."""
    return load_concept_mappings(mapping_file)["gender_to_concept"]


def get_race_concept_mappings(mapping_file: str | None = None) -> dict[str, int]:
    """Returns race string to OMOP concept ID mappings."""
    return load_concept_mappings(mapping_file)["race_to_concept"]


def get_ethnicity_concept_mappings(mapping_file: str | None = None) -> dict[str, int]:
    """Returns ethnicity string to OMOP concept ID mappings."""
    return load_concept_mappings(mapping_file)["ethnicity_to_concept"]


def get_clinvar_concept_mappings(mapping_file: str | None = None) -> dict[str, int]:
    """Returns ClinVar CLNSIG to OMOP clinical significance concept ID mappings."""
    return load_concept_mappings(mapping_file)["clinvar_to_concept"]


def build_concept_lookup(lookup_col: Column, mapping_dict: dict[str, int], default_val: int = 0) -> Column:
    """Constructs a native PySpark map expression for dictionary-based concept lookup.

    Filters out metadata/comment keys starting with '_' and non-integer values.

    Args:
        lookup_col: PySpark Column containing source string keys.
        mapping_dict: Dictionary mapping string codes to integer OMOP concept IDs.
        default_val: Fallback concept ID when code is unmapped (default: 0).

    Returns:
        PySpark Column evaluating to the mapped integer concept ID.
    """
    if not mapping_dict:
        return lit(default_val).cast("integer")
    kv_pairs = []
    for k, v in mapping_dict.items():
        if not str(k).startswith("_"):
            try:
                kv_pairs.extend([lit(str(k)), lit(int(v))])
            except (ValueError, TypeError):
                continue
    if not kv_pairs:
        return lit(default_val).cast("integer")
    spark_map = create_map(*kv_pairs)
    return coalesce(spark_map[lookup_col], lit(default_val)).cast("integer")
