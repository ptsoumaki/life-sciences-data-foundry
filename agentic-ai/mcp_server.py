"""
Module: mcp_server.py
Description: Model Context Protocol (FastMCP) Clinical Data Server exposing OMOP CDM v5.4
             concept lookups, vocabulary relationships, Delta Lake transaction commit logs,
             Great Expectations data contracts, and MLflow GxP lineage auditing to AI agent assistants.

Dependencies:
    Requires `mcp>=0.1.0`. Install with: `pip install -e .`

Author: Vivi Tsoumaki
"""

import argparse
import asyncio
import glob
import importlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

try:
    import mlflow
    from mlflow.tracking import MlflowClient
except ImportError:
    mlflow = None  # type: ignore[assignment]
    MlflowClient = None  # type: ignore[assignment, misc]

# Ensure repository root and agentic-ai directory are in sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AGENTIC_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYTICAL_DIR = os.path.join(BASE_DIR, "analytical-layer")

for p in [BASE_DIR, AGENTIC_DIR, ANALYTICAL_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from graph_auditor import GxPGraphAuditor
except ImportError:
    GxPGraphAuditor = None  # type: ignore[assignment, misc]

MCPServer: Any = None

try:
    _fastmcp_mod = importlib.import_module("mcp.server.fastmcp")
    MCPServer = getattr(_fastmcp_mod, "FastMCP", None)
except ImportError:
    pass

if MCPServer is None:
    try:
        _mcpserver_mod = importlib.import_module("mcp.server.mcpserver")
        MCPServer = getattr(_mcpserver_mod, "MCPServer", None)
    except ImportError:
        MCPServer = None


from governance.crypto import compute_sha256  # noqa: E402
from omop_cdm_v54.vocabularies import (  # noqa: E402
    DEFAULT_CLINVAR_MAPPINGS,
    DEFAULT_ICD10_MAPPINGS,
    DEFAULT_LOINC_MAPPINGS,
    load_concept_mappings,
)

__all__ = [
    "OMOP_CDM_V54_SCHEMAS",
    "FoundryMCPServer",
]
# Standard OMOP CDM v5.4 Table Schema Definitions
OMOP_CDM_V54_SCHEMAS: dict[str, dict[str, Any]] = {
    "person": {
        "table_name": "PERSON",
        "description": "Demographic and identity records for patients in the OMOP Common Data Model v5.4.",
        "primary_key": "person_id",
        "clustering_keys": ["person_id"],
        "columns": [
            {
                "name": "person_id",
                "type": "long",
                "nullable": False,
                "description": "Unique cryptographic integer identifier for each individual person.",
            },
            {
                "name": "gender_concept_id",
                "type": "integer",
                "nullable": False,
                "description": "Standard OMOP concept ID for gender (e.g., 8507=Male, 8532=Female).",
            },
            {
                "name": "year_of_birth",
                "type": "integer",
                "nullable": False,
                "description": "Year of birth extracted from longitudinal demographic records.",
            },
            {
                "name": "month_of_birth",
                "type": "integer",
                "nullable": True,
                "description": "Month of birth (1-12).",
            },
            {
                "name": "day_of_birth",
                "type": "integer",
                "nullable": True,
                "description": "Day of birth (1-31).",
            },
            {
                "name": "birth_datetime",
                "type": "timestamp",
                "nullable": True,
                "description": "ISO-8601 UTC timestamp of birth.",
            },
            {
                "name": "race_concept_id",
                "type": "integer",
                "nullable": False,
                "description": "Standard OMOP concept ID representing race.",
            },
            {
                "name": "ethnicity_concept_id",
                "type": "integer",
                "nullable": False,
                "description": "Standard OMOP concept ID representing ethnicity.",
            },
            {
                "name": "location_id",
                "type": "long",
                "nullable": True,
                "description": "Foreign key to LOCATION table.",
            },
            {
                "name": "provider_id",
                "type": "long",
                "nullable": True,
                "description": "Foreign key to PROVIDER table.",
            },
            {
                "name": "care_site_id",
                "type": "long",
                "nullable": True,
                "description": "Foreign key to CARE_SITE table.",
            },
            {
                "name": "person_source_value",
                "type": "string",
                "nullable": True,
                "description": "Raw source identifier for the individual.",
            },
            {
                "name": "gender_source_value",
                "type": "string",
                "nullable": True,
                "description": "Raw source string for gender.",
            },
            {
                "name": "gender_source_concept_id",
                "type": "integer",
                "nullable": True,
                "description": "Source concept ID for gender.",
            },
            {
                "name": "race_source_value",
                "type": "string",
                "nullable": True,
                "description": "Raw source string for race.",
            },
            {
                "name": "race_source_concept_id",
                "type": "integer",
                "nullable": True,
                "description": "Source concept ID for race.",
            },
            {
                "name": "ethnicity_source_value",
                "type": "string",
                "nullable": True,
                "description": "Raw source string for ethnicity.",
            },
            {
                "name": "ethnicity_source_concept_id",
                "type": "integer",
                "nullable": True,
                "description": "Source concept ID for ethnicity.",
            },
        ],
    },
    "condition_occurrence": {
        "table_name": "CONDITION_OCCURRENCE",
        "description": "Clinical diagnosis, disease, and health condition events mapped to standard SNOMED CT concepts.",
        "primary_key": "condition_occurrence_id",
        "clustering_keys": ["person_id", "condition_concept_id"],
        "columns": [
            {
                "name": "condition_occurrence_id",
                "type": "long",
                "nullable": False,
                "description": "Unique identifier for each condition occurrence event.",
            },
            {
                "name": "person_id",
                "type": "long",
                "nullable": False,
                "description": "Foreign key to PERSON table.",
            },
            {
                "name": "condition_concept_id",
                "type": "integer",
                "nullable": False,
                "description": "Standard SNOMED CT concept ID representing the diagnosed condition.",
            },
            {
                "name": "condition_start_date",
                "type": "date",
                "nullable": False,
                "description": "Date when the condition was diagnosed (YYYY-MM-DD).",
            },
            {
                "name": "condition_start_datetime",
                "type": "timestamp",
                "nullable": True,
                "description": "Timestamp of condition onset in ISO-8601 format.",
            },
            {
                "name": "condition_end_date",
                "type": "date",
                "nullable": True,
                "description": "Date when condition resolved (YYYY-MM-DD).",
            },
            {
                "name": "condition_end_datetime",
                "type": "timestamp",
                "nullable": True,
                "description": "Timestamp of condition resolution.",
            },
            {
                "name": "condition_type_concept_id",
                "type": "integer",
                "nullable": False,
                "description": "Provenance concept — always 32817 (EHR) for pipeline-generated EHR condition records.",
            },
            {
                "name": "condition_source_value",
                "type": "string",
                "nullable": True,
                "description": "Raw ICD-10-CM source code string.",
            },
            {
                "name": "condition_source_concept_id",
                "type": "integer",
                "nullable": True,
                "description": "Source concept ID if mapped.",
            },
            {
                "name": "condition_status_source_value",
                "type": "string",
                "nullable": True,
                "description": "Source status of condition (e.g. primary, secondary).",
            },
            {
                "name": "condition_status_concept_id",
                "type": "integer",
                "nullable": True,
                "description": "Standard concept for condition status.",
            },
        ],
    },
    "measurement": {
        "table_name": "MEASUREMENT",
        "description": "Structured quantitative laboratory test results, biomarker observations, and genomic variant measurements.",
        "primary_key": "measurement_id",
        "clustering_keys": ["person_id", "measurement_concept_id"],
        "columns": [
            {
                "name": "measurement_id",
                "type": "long",
                "nullable": False,
                "description": "Unique identifier for each measurement event.",
            },
            {
                "name": "person_id",
                "type": "long",
                "nullable": False,
                "description": "Foreign key to PERSON table.",
            },
            {
                "name": "measurement_concept_id",
                "type": "integer",
                "nullable": False,
                "description": "Standard LOINC or OMOP concept ID representing the laboratory assay.",
            },
            {
                "name": "measurement_date",
                "type": "date",
                "nullable": False,
                "description": "Date the assay/observation was performed (YYYY-MM-DD).",
            },
            {
                "name": "measurement_datetime",
                "type": "timestamp",
                "nullable": True,
                "description": "ISO-8601 UTC timestamp of assay execution.",
            },
            {
                "name": "measurement_type_concept_id",
                "type": "integer",
                "nullable": False,
                "description": "Provenance concept — always 45754907 (Lab result) for pipeline-generated lab measurements.",
            },
            {
                "name": "operator_concept_id",
                "type": "integer",
                "nullable": True,
                "description": "Concept for mathematical operator (=, <, >, <=, >=).",
            },
            {
                "name": "value_as_number",
                "type": "double",
                "nullable": True,
                "description": "Quantitative numeric test measurement result.",
            },
            {
                "name": "value_as_concept_id",
                "type": "integer",
                "nullable": True,
                "description": "Coded categorical result or ClinVar significance concept ID.",
            },
            {
                "name": "unit_concept_id",
                "type": "integer",
                "nullable": True,
                "description": "Standard UCUM unit concept ID (e.g. 8840=mg/dL, 8554=% HbA1c).",
            },
            {
                "name": "range_low",
                "type": "double",
                "nullable": True,
                "description": "Lower reference limit for normal physiological values.",
            },
            {
                "name": "range_high",
                "type": "double",
                "nullable": True,
                "description": "Upper reference limit for normal physiological values.",
            },
            {
                "name": "measurement_source_value",
                "type": "string",
                "nullable": True,
                "description": "Source test or genomic variant identifier (e.g., LOINC code, rsID).",
            },
            {
                "name": "unit_source_value",
                "type": "string",
                "nullable": True,
                "description": "Source unit string (e.g., mg/dL, mmol/L).",
            },
            {
                "name": "value_source_value",
                "type": "string",
                "nullable": True,
                "description": "Raw string value of observation result or genotype.",
            },
        ],
    },
    "cohort": {
        "table_name": "COHORT",
        "description": "Standard OHDSI analytical cohort membership table recording index event dates and cohort definition bounds.",
        "primary_key": "cohort_definition_id, subject_id, cohort_start_date",
        "clustering_keys": ["cohort_definition_id", "subject_id"],
        "columns": [
            {
                "name": "cohort_definition_id",
                "type": "long",
                "nullable": False,
                "description": "Unique identifier for the analytical phenotyping cohort definition.",
            },
            {
                "name": "subject_id",
                "type": "long",
                "nullable": False,
                "description": "Foreign key to PERSON.person_id.",
            },
            {
                "name": "cohort_start_date",
                "type": "string",
                "nullable": False,
                "description": "Index start date (T0) when the subject enters the cohort (YYYY-MM-DD).",
            },
            {
                "name": "cohort_end_date",
                "type": "string",
                "nullable": False,
                "description": "End date when the subject ceases to satisfy cohort criteria or study concludes.",
            },
        ],
    },
    "quarantine_patients": {
        "table_name": "QUARANTINE_PATIENTS",
        "description": "Dead-letter quarantine repository for non-compliant, malformed, or unvalidated patient demographic records.",
        "primary_key": "quarantine_id",
        "clustering_keys": ["failure_code", "status"],
        "columns": [
            {
                "name": "quarantine_id",
                "type": "string",
                "nullable": False,
                "description": "Unique cryptographic UUID identifier for the quarantined patient record.",
            },
            {
                "name": "table_name",
                "type": "string",
                "nullable": False,
                "description": "Originating source table name ('patients' / 'clinical_demographics').",
            },
            {
                "name": "raw_payload",
                "type": "string",
                "nullable": False,
                "description": "Verbatim serialized JSON payload of the rejected record ensuring zero data loss.",
            },
            {
                "name": "failure_code",
                "type": "string",
                "nullable": False,
                "description": "Standardized clinical failure code taxonomy (e.g. SCHEMA_VIOLATION, TEMPORAL_ANOMALY).",
            },
            {
                "name": "failure_reason",
                "type": "string",
                "nullable": False,
                "description": "Deterministic human-readable explanation of rejection rationale.",
            },
            {
                "name": "failure_timestamp",
                "type": "timestamp",
                "nullable": False,
                "description": "ISO-8601 UTC timestamp when the record was quarantined.",
            },
            {
                "name": "mlflow_run_id",
                "type": "string",
                "nullable": False,
                "description": "MLflow run ID for FDA 21 CFR Part 11 cryptographic traceability.",
            },
            {
                "name": "status",
                "type": "string",
                "nullable": False,
                "description": "Remediation lifecycle state: 'QUARANTINED' or 'REMEDIATED'.",
            },
            {
                "name": "remediation_timestamp",
                "type": "timestamp",
                "nullable": True,
                "description": "ISO-8601 UTC timestamp when the record was remediated and promoted to Silver.",
            },
        ],
    },
    "quarantine_conditions": {
        "table_name": "QUARANTINE_CONDITIONS",
        "description": "Dead-letter quarantine repository for non-compliant, unmapped, or invalid clinical condition/diagnosis records.",
        "primary_key": "quarantine_id",
        "clustering_keys": ["failure_code", "status"],
        "columns": [
            {
                "name": "quarantine_id",
                "type": "string",
                "nullable": False,
                "description": "Unique cryptographic UUID identifier for the quarantined condition record.",
            },
            {
                "name": "table_name",
                "type": "string",
                "nullable": False,
                "description": "Originating source table name ('diagnoses' / 'clinical_diagnoses').",
            },
            {
                "name": "raw_payload",
                "type": "string",
                "nullable": False,
                "description": "Verbatim serialized JSON payload of the rejected condition record.",
            },
            {
                "name": "failure_code",
                "type": "string",
                "nullable": False,
                "description": "Standardized clinical failure code taxonomy (e.g. UNMAPPED_TERMINOLOGY, TEMPORAL_ANOMALY).",
            },
            {
                "name": "failure_reason",
                "type": "string",
                "nullable": False,
                "description": "Deterministic human-readable explanation of condition rejection rationale.",
            },
            {
                "name": "failure_timestamp",
                "type": "timestamp",
                "nullable": False,
                "description": "ISO-8601 UTC timestamp when the condition was quarantined.",
            },
            {
                "name": "mlflow_run_id",
                "type": "string",
                "nullable": False,
                "description": "MLflow run ID for FDA 21 CFR Part 11 cryptographic traceability.",
            },
            {
                "name": "status",
                "type": "string",
                "nullable": False,
                "description": "Remediation lifecycle state: 'QUARANTINED' or 'REMEDIATED'.",
            },
            {
                "name": "remediation_timestamp",
                "type": "timestamp",
                "nullable": True,
                "description": "ISO-8601 UTC timestamp when the condition was remediated and promoted to Silver.",
            },
        ],
    },
    "quarantine_measurements": {
        "table_name": "QUARANTINE_MEASUREMENTS",
        "description": "Dead-letter quarantine repository for unmapped, out-of-bounds, or non-compliant lab measurements.",
        "primary_key": "quarantine_id",
        "clustering_keys": ["failure_code", "status"],
        "columns": [
            {
                "name": "quarantine_id",
                "type": "string",
                "nullable": False,
                "description": "Unique cryptographic UUID identifier for the quarantined measurement record.",
            },
            {
                "name": "table_name",
                "type": "string",
                "nullable": False,
                "description": "Originating source table name ('labs' / 'lab_measurements').",
            },
            {
                "name": "raw_payload",
                "type": "string",
                "nullable": False,
                "description": "Verbatim serialized JSON payload of the rejected lab observation.",
            },
            {
                "name": "failure_code",
                "type": "string",
                "nullable": False,
                "description": "Standardized clinical failure code taxonomy (e.g. OUT_OF_BOUNDS_LAB, UNMAPPED_TERMINOLOGY).",
            },
            {
                "name": "failure_reason",
                "type": "string",
                "nullable": False,
                "description": "Deterministic human-readable explanation of measurement rejection rationale.",
            },
            {
                "name": "failure_timestamp",
                "type": "timestamp",
                "nullable": False,
                "description": "ISO-8601 UTC timestamp when the measurement was quarantined.",
            },
            {
                "name": "mlflow_run_id",
                "type": "string",
                "nullable": False,
                "description": "MLflow run ID for FDA 21 CFR Part 11 cryptographic traceability.",
            },
            {
                "name": "status",
                "type": "string",
                "nullable": False,
                "description": "Remediation lifecycle state: 'QUARANTINED' or 'REMEDIATED'.",
            },
            {
                "name": "remediation_timestamp",
                "type": "timestamp",
                "nullable": True,
                "description": "ISO-8601 UTC timestamp when the measurement was remediated and promoted to Silver.",
            },
        ],
    },
    "survival_mart": {
        "table_name": "SURVIVAL_MART",
        "description": "Longitudinal time-to-event and survival analysis analytical data mart conforming to SURVIVAL_FRAME_SCHEMA.",
        "primary_key": "subject_id",
        "clustering_keys": ["cohort_definition_id", "subject_id"],
        "columns": [
            {
                "name": "cohort_definition_id",
                "type": "long",
                "nullable": False,
                "description": "Unique identifier for the study cohort definition.",
            },
            {
                "name": "subject_id",
                "type": "long",
                "nullable": False,
                "description": "Unique cryptographic integer identifier for the patient.",
            },
            {
                "name": "cohort_start_date",
                "type": "date",
                "nullable": False,
                "description": "Index date (T0) anchoring the survival analysis follow-up window.",
            },
            {
                "name": "time_to_event_days",
                "type": "integer",
                "nullable": False,
                "description": "Duration in days from cohort_start_date to event or censoring cutoff.",
            },
            {
                "name": "event",
                "type": "integer",
                "nullable": False,
                "description": "Binary endpoint event indicator (1=Event observed, 0=Censored).",
            },
            {
                "name": "censoring_reason",
                "type": "string",
                "nullable": False,
                "description": "Reason for censoring (STUDY_END, OBSERVATION_END, MAX_FOLLOWUP, or DEATH_EVENT).",
            },
            {
                "name": "age_at_index",
                "type": "integer",
                "nullable": True,
                "description": "Patient age in years at cohort index date.",
            },
            {
                "name": "gender_concept_id",
                "type": "long",
                "nullable": True,
                "description": "Standard OMOP concept ID for gender.",
            },
            {
                "name": "has_pathogenic_variant",
                "type": "integer",
                "nullable": False,
                "description": "Flag indicating confirmed ClinVar pathogenic/likely pathogenic variant (1=Yes, 0=No).",
            },
            {
                "name": "stratum",
                "type": "string",
                "nullable": False,
                "description": "Stratum label ('Pathogenic Variant' or 'Wild-Type / VUS').",
            },
        ],
    },
    "patient_feature_store": {
        "table_name": "PATIENT_FEATURE_STORE",
        "description": "Machine-learning-ready patient feature store with Charlson Comorbidity Index, rolling condition counts, and biomarker aggregations.",
        "primary_key": "subject_id",
        "clustering_keys": ["cohort_definition_id", "subject_id"],
        "columns": [
            {
                "name": "cohort_definition_id",
                "type": "long",
                "nullable": False,
                "description": "Unique identifier for the study cohort definition.",
            },
            {
                "name": "subject_id",
                "type": "long",
                "nullable": False,
                "description": "Unique cryptographic integer identifier for the patient.",
            },
            {
                "name": "cohort_start_date",
                "type": "date",
                "nullable": False,
                "description": "Cohort index date (T0).",
            },
            {
                "name": "cohort_end_date",
                "type": "date",
                "nullable": False,
                "description": "Cohort exit date.",
            },
            {
                "name": "gender_concept_id",
                "type": "long",
                "nullable": True,
                "description": "Standard OMOP concept ID for gender.",
            },
            {
                "name": "year_of_birth",
                "type": "integer",
                "nullable": True,
                "description": "Year of birth.",
            },
            {
                "name": "age_at_index",
                "type": "integer",
                "nullable": True,
                "description": "Age at index date.",
            },
            {
                "name": "is_female",
                "type": "integer",
                "nullable": False,
                "description": "Binary indicator for female sex.",
            },
            {
                "name": "is_male",
                "type": "integer",
                "nullable": False,
                "description": "Binary indicator for male sex.",
            },
            {
                "name": "condition_count_30d",
                "type": "integer",
                "nullable": False,
                "description": "Count of conditions diagnosed within 30 days prior to index.",
            },
            {
                "name": "condition_count_180d",
                "type": "integer",
                "nullable": False,
                "description": "Count of conditions diagnosed within 180 days prior to index.",
            },
            {
                "name": "condition_count_365d",
                "type": "integer",
                "nullable": False,
                "description": "Count of conditions diagnosed within 365 days prior to index.",
            },
            {
                "name": "distinct_condition_count_365d",
                "type": "integer",
                "nullable": False,
                "description": "Count of distinct condition concepts diagnosed within 365 days prior to index.",
            },
            {
                "name": "condition_count_lifetime",
                "type": "integer",
                "nullable": False,
                "description": "Lifetime count of conditions diagnosed prior to index.",
            },
            {
                "name": "charlson_comorbidity_index",
                "type": "integer",
                "nullable": False,
                "description": "Total weighted Charlson Comorbidity Index score.",
            },
            {
                "name": "has_pathogenic_variant",
                "type": "integer",
                "nullable": False,
                "description": "Binary indicator for confirmed ClinVar pathogenic mutation.",
            },
            {
                "name": "num_pathogenic_variants",
                "type": "integer",
                "nullable": False,
                "description": "Count of distinct pathogenic ClinVar mutations observed.",
            },
        ],
    },
    "target_disease_evidence": {
        "table_name": "TARGET_DISEASE_EVIDENCE",
        "description": "Target-to-Phenotype Evidence Mart associating ClinVar genomic variants with OMOP CDM condition diagnoses across cohorts.",
        "primary_key": "target_gene_symbol",
        "clustering_keys": ["target_gene_symbol", "disease_concept_id"],
        "columns": [
            {
                "name": "target_gene_symbol",
                "type": "string",
                "nullable": False,
                "description": "Canonical uppercase HGNC gene symbol.",
            },
            {
                "name": "disease_concept_id",
                "type": "long",
                "nullable": False,
                "description": "Standard OMOP condition concept identifier.",
            },
            {
                "name": "carrier_cases",
                "type": "integer",
                "nullable": False,
                "description": "Subjects carrying variant with disease diagnosis (cell a).",
            },
            {
                "name": "carrier_controls",
                "type": "integer",
                "nullable": False,
                "description": "Subjects carrying variant without disease diagnosis (cell b).",
            },
            {
                "name": "non_carrier_cases",
                "type": "integer",
                "nullable": False,
                "description": "Wild-type subjects with disease diagnosis (cell c).",
            },
            {
                "name": "non_carrier_controls",
                "type": "integer",
                "nullable": False,
                "description": "Wild-type subjects without disease diagnosis (cell d).",
            },
            {
                "name": "total_cohort_size",
                "type": "integer",
                "nullable": False,
                "description": "Total evaluated cohort denominator.",
            },
            {
                "name": "target_mutation_burden",
                "type": "double",
                "nullable": False,
                "description": "Mean variant count per carrier.",
            },
            {
                "name": "carrier_frequency",
                "type": "double",
                "nullable": False,
                "description": "Proportion of cohort carrying target variants.",
            },
            {
                "name": "phenotype_prevalence",
                "type": "double",
                "nullable": False,
                "description": "Proportion of cohort diagnosed with disease.",
            },
            {
                "name": "odds_ratio",
                "type": "double",
                "nullable": False,
                "description": "Haldane-Anscombe continuity-corrected phenotypic Odds Ratio.",
            },
            {
                "name": "log_odds_ratio",
                "type": "double",
                "nullable": False,
                "description": "Natural logarithm of Haldane-Anscombe corrected Odds Ratio.",
            },
            {
                "name": "se_log_odds_ratio",
                "type": "double",
                "nullable": False,
                "description": "Asymptotic standard error of log Odds Ratio.",
            },
            {
                "name": "odds_ratio_ci_lower",
                "type": "double",
                "nullable": False,
                "description": "95% confidence interval lower bound for Odds Ratio.",
            },
            {
                "name": "odds_ratio_ci_upper",
                "type": "double",
                "nullable": False,
                "description": "95% confidence interval upper bound for Odds Ratio.",
            },
            {
                "name": "p_value",
                "type": "double",
                "nullable": False,
                "description": "Two-tailed asymptotic association p-value via polynomial standard normal CDF approximation.",
            },
            {
                "name": "biomarker_correlation",
                "type": "double",
                "nullable": False,
                "description": "Standardized biomarker mean shift between carriers and controls [-1.0, 1.0].",
            },
            {
                "name": "target_tractability_score",
                "type": "double",
                "nullable": False,
                "description": "Composite target tractability score bounded in [0.0, 1.0].",
            },
            {
                "name": "evidence_tier",
                "type": "string",
                "nullable": False,
                "description": "Clinical evidence tier (TIER_1_VALIDATED, TIER_2_CANDIDATE, TIER_3_EXPLORATORY).",
            },
            {
                "name": "created_at",
                "type": "timestamp",
                "nullable": False,
                "description": "UTC timestamp of mart generation.",
            },
            {
                "name": "mlflow_run_id",
                "type": "string",
                "nullable": False,
                "description": "MLflow run ID for FDA 21 CFR Part 11 audit lineage.",
            },
        ],
    },
    "quarantine_target_evidence": {
        "table_name": "QUARANTINE_TARGET_EVIDENCE",
        "description": "Dead-letter quarantine repository for non-compliant target discovery evidence records breaching GxP data contracts.",
        "primary_key": "quarantine_id",
        "clustering_keys": ["failure_code", "status"],
        "columns": [
            {
                "name": "quarantine_id",
                "type": "string",
                "nullable": False,
                "description": "Unique cryptographic UUID identifier for the quarantined record.",
            },
            {
                "name": "table_name",
                "type": "string",
                "nullable": False,
                "description": "Originating target table name ('quarantine_target_evidence').",
            },
            {
                "name": "raw_payload",
                "type": "string",
                "nullable": False,
                "description": "Verbatim serialized JSON payload of the rejected target evidence row.",
            },
            {
                "name": "failure_code",
                "type": "string",
                "nullable": False,
                "description": "Standardized clinical failure code (TARGET_CONTRACT_VIOLATION).",
            },
            {
                "name": "failure_reason",
                "type": "string",
                "nullable": False,
                "description": "Deterministic explanation of contract invariant breach.",
            },
            {
                "name": "failure_timestamp",
                "type": "timestamp",
                "nullable": False,
                "description": "ISO-8601 UTC timestamp when record was quarantined.",
            },
            {
                "name": "mlflow_run_id",
                "type": "string",
                "nullable": False,
                "description": "MLflow run ID for FDA 21 CFR Part 11 audit traceability.",
            },
            {
                "name": "status",
                "type": "string",
                "nullable": False,
                "description": "Remediation lifecycle state ('QUARANTINED' or 'REMEDIATED').",
            },
            {
                "name": "remediation_timestamp",
                "type": "timestamp",
                "nullable": True,
                "description": "ISO-8601 UTC timestamp when the record was remediated.",
            },
        ],
    },
}


def _resolve_repo_path(relative_path: str) -> str:
    """Resolves a relative file path against the project repository root."""
    if os.path.isabs(relative_path):
        return relative_path
    candidate = os.path.join(BASE_DIR, relative_path)
    if os.path.exists(candidate):
        return candidate
    return relative_path


# =====================================================================
# Foundry MCP Core Tool Implementations
# =====================================================================


def tool_lookup_icd10_to_snomed(icd10_code: str, mapping_file: str | None = None) -> dict[str, Any]:
    """Translates an ICD-10-CM clinical diagnosis code to a standard SNOMED CT concept ID.

    Queries the GxP concept mappings repository to resolve diagnoses (e.g., 'E11.9' for
    Type 2 Diabetes, 'I10' for Hypertension, 'C34.90' for Lung Neoplasm) to OHDSI OMOP
    standard condition concept IDs.

    Args:
        icd10_code: Raw ICD-10-CM diagnosis code (e.g. 'E11.9', 'I10', 'J45.909').
        mapping_file: Optional path to custom concept_mappings.json.

    Returns:
        Dictionary with query code, standard concept ID, domain, and clinical description.
    """
    clean_code = str(icd10_code).strip().upper()
    code_no_dot = clean_code.replace(".", "")
    resolved_file = _resolve_repo_path(mapping_file) if mapping_file else None
    mappings_data = load_concept_mappings(resolved_file)
    icd10_map = mappings_data.get("icd10_to_snomed", DEFAULT_ICD10_MAPPINGS)

    # Check raw code, uppercase, and dot-stripped variations
    concept_id = icd10_map.get(clean_code)
    if concept_id is None:
        concept_id = icd10_map.get(code_no_dot, 0)

    found = concept_id != 0
    description = (
        f"SNOMED CT standard concept ID {concept_id}"
        if found
        else f"Unmapped ICD-10 code '{icd10_code}' (mapped to concept 0=Unmapped)"
    )

    return {
        "query_code": icd10_code,
        "normalized_code": clean_code,
        "standard_concept_id": int(concept_id),
        "vocabulary_id": "SNOMED",
        "domain_id": "Condition",
        "target_table": "CONDITION_OCCURRENCE",
        "target_field": "condition_concept_id",
        "found": found,
        "description": description,
    }


def tool_lookup_loinc_concept(loinc_code: str, mapping_file: str | None = None) -> dict[str, Any]:
    """Translates a LOINC laboratory biomarker code to a standard OMOP Measurement concept ID.

    Resolves laboratory assay codes (e.g., '4548-4' for HbA1c, '2345-7' for Glucose,
    '2093-3' for Cholesterol) into OMOP CDM v5.4 MEASUREMENT standard concepts.

    Args:
        loinc_code: LOINC laboratory code string (e.g. '4548-4', '2345-7').
        mapping_file: Optional path to custom concept_mappings.json.

    Returns:
        Dictionary with query code, standard concept ID, domain, and clinical description.
    """
    clean_code = str(loinc_code).strip()
    resolved_file = _resolve_repo_path(mapping_file) if mapping_file else None
    mappings_data = load_concept_mappings(resolved_file)
    loinc_map = mappings_data.get("loinc_to_concept", DEFAULT_LOINC_MAPPINGS)

    concept_id = loinc_map.get(clean_code, 0)
    found = concept_id != 0
    description = (
        f"OMOP Measurement standard concept ID {concept_id}"
        if found
        else f"Unmapped LOINC code '{loinc_code}' (mapped to concept 0=Unmapped)"
    )

    return {
        "query_code": loinc_code,
        "normalized_code": clean_code,
        "standard_concept_id": int(concept_id),
        "vocabulary_id": "LOINC",
        "domain_id": "Measurement",
        "target_table": "MEASUREMENT",
        "target_field": "measurement_concept_id",
        "found": found,
        "description": description,
    }


def tool_lookup_demographic_concept(
    domain: str, value: str, mapping_file: str | None = None
) -> dict[str, Any]:
    """Resolves demographic values (gender, race, ethnicity) to OMOP CDM standard concept IDs.

    Args:
        domain: Demographic domain ('gender', 'race', or 'ethnicity').
        value: Raw demographic source string (e.g., 'MALE', 'WHITE', 'HISPANIC').
        mapping_file: Optional path to custom concept_mappings.json.

    Returns:
        Dictionary containing mapped concept ID and target PERSON table field.
    """
    clean_domain = str(domain).strip().lower()
    clean_val = str(value).strip().upper()
    resolved_file = _resolve_repo_path(mapping_file) if mapping_file else None
    mappings_data = load_concept_mappings(resolved_file)

    domain_key = f"{clean_domain}_to_concept"
    if domain_key not in mappings_data:
        return {
            "domain": domain,
            "query_value": value,
            "error": f"Invalid demographic domain '{domain}'. Supported: 'gender', 'race', 'ethnicity'",
            "standard_concept_id": 0,
            "found": False,
        }

    domain_map = mappings_data[domain_key]
    concept_id = domain_map.get(clean_val, 0)
    found = concept_id != 0

    return {
        "domain": clean_domain,
        "query_value": value,
        "normalized_value": clean_val,
        "standard_concept_id": int(concept_id),
        "target_table": "PERSON",
        "target_field": f"{clean_domain}_concept_id",
        "found": found,
        "description": f"OMOP {clean_domain.capitalize()} standard concept ID {concept_id}",
    }


def tool_lookup_genomic_variant_concept(
    clnsig: str, mapping_file: str | None = None
) -> dict[str, Any]:
    """Resolves ClinVar clinical significance terms (CLNSIG) to OMOP Meas Value concept IDs.

    Args:
        clnsig: ClinVar clinical significance term (e.g., 'Pathogenic', 'Benign', 'Likely_pathogenic').
        mapping_file: Optional path to custom concept_mappings.json.

    Returns:
        Dictionary with mapped concept ID and OMOP MEASUREMENT.value_as_concept_id field.
    """
    clean_clnsig = str(clnsig).strip()
    resolved_file = _resolve_repo_path(mapping_file) if mapping_file else None
    mappings_data = load_concept_mappings(resolved_file)
    clinvar_map = mappings_data.get("clinvar_to_concept", DEFAULT_CLINVAR_MAPPINGS)

    concept_id = clinvar_map.get(clean_clnsig)
    if concept_id is None:
        # Try title/capitalized variations
        for k, v in clinvar_map.items():
            if k.lower() == clean_clnsig.lower():
                concept_id = v
                break

    if concept_id is None:
        concept_id = 0

    found = concept_id != 0
    return {
        "clinical_significance": clnsig,
        "standard_concept_id": int(concept_id),
        "domain_id": "Meas Value",
        "target_table": "MEASUREMENT",
        "target_field": "value_as_concept_id",
        "found": found,
        "description": f"ClinVar clinical significance concept ID {concept_id}",
    }


def tool_query_vocabulary_mappings(
    category: str | None = None, mapping_file: str | None = None
) -> dict[str, Any]:
    """Queries active vocabulary mappings and concept lookup dictionaries.

    Args:
        category: Optional category filter ('icd10_to_snomed', 'loinc_to_concept',
                  'gender_to_concept', 'race_to_concept', 'ethnicity_to_concept',
                  'clinvar_to_concept'). If None, returns all categories.
        mapping_file: Optional path to custom concept_mappings.json.

    Returns:
        Dictionary containing mapped term-to-concept dictionaries.
    """
    resolved_file = _resolve_repo_path(mapping_file) if mapping_file else None
    mappings_data = load_concept_mappings(resolved_file)

    if category:
        clean_cat = str(category).strip().lower()
        if clean_cat in mappings_data:
            return {clean_cat: mappings_data[clean_cat]}
        return {
            "error": f"Unknown category '{category}'. Available: {list(mappings_data.keys())}",
            "available_categories": list(mappings_data.keys()),
        }

    return mappings_data


def tool_inspect_omop_table_schema(table_name: str) -> dict[str, Any]:
    """Returns official OMOP CDM v5.4 and quarantine dead-letter schema definitions, column data types, and primary keys.

    Args:
        table_name: Name of OMOP CDM or quarantine table ('person', 'condition_occurrence', 'measurement', 'cohort', 'quarantine_patients', 'quarantine_conditions', 'quarantine_measurements').

    Returns:
        Schema dictionary including columns, types, nullability, and primary/clustering keys.
    """
    clean_table = str(table_name).strip().lower()
    if clean_table in OMOP_CDM_V54_SCHEMAS:
        return OMOP_CDM_V54_SCHEMAS[clean_table]

    return {
        "error": f"Table '{table_name}' not found in OMOP CDM v5.4 registry.",
        "available_tables": list(OMOP_CDM_V54_SCHEMAS.keys()),
    }


def tool_get_pipeline_execution_state(
    run_id: str | None = None,
    experiment_name: str = "gxp_clinical_governance",
    tracking_uri: str | None = None,
) -> dict[str, Any]:
    """Queries MLflow execution lineage and GxP data contract validation metrics.

    Args:
        run_id: Optional specific MLflow run ID. If None, retrieves the latest active or completed run.
        experiment_name: MLflow experiment name (default: 'gxp_clinical_governance').
        tracking_uri: Optional MLflow tracking server URI.

    Returns:
        Dictionary containing run status, metrics, parameters, tags, and audit certificates.
    """
    try:
        if mlflow is None or MlflowClient is None:
            return {
                "status": "UNAVAILABLE",
                "error": "MLflow is not installed or available in the environment.",
            }

        effective_uri = tracking_uri or os.getenv("MLFLOW_TRACKING_URI")
        if not effective_uri:
            db_candidate = _resolve_repo_path("mlflow.db")
            if os.path.exists(db_candidate):
                effective_uri = f"sqlite:///{Path(db_candidate).as_posix()}"
            else:
                effective_uri = "file:./mlruns"

        mlflow.set_tracking_uri(effective_uri)
        client = MlflowClient(tracking_uri=effective_uri)

        target_run_id = run_id
        if not target_run_id:
            exp = client.get_experiment_by_name(experiment_name)
            if not exp:
                return {
                    "status": "NOT_FOUND",
                    "reason": f"Experiment '{experiment_name}' not found at tracking URI {effective_uri}.",
                }
            runs = client.search_runs(
                experiment_ids=[exp.experiment_id],
                max_results=1,
                order_by=["attributes.start_time DESC"],
            )
            if not runs:
                return {
                    "status": "NO_RUNS",
                    "reason": f"No runs found in experiment '{experiment_name}'.",
                }
            target_run_id = runs[0].info.run_id

        run = client.get_run(target_run_id)
        info = run.info
        data = run.data

        # List artifacts
        artifacts = []
        try:
            art_list = client.list_artifacts(target_run_id)
            artifacts = [a.path for a in art_list]
        except Exception:
            pass

        return {
            "run_id": info.run_id,
            "experiment_id": info.experiment_id,
            "status": info.status,
            "start_time": info.start_time,
            "end_time": info.end_time,
            "metrics": data.metrics,
            "params": data.params,
            "tags": data.tags,
            "artifacts": artifacts,
            "gxp_gate_passed": bool(data.metrics.get("gxp_gate_passed", 0) == 1.0),
            "expectation_success_rate": data.metrics.get("expectation_success_rate", 0.0),
        }
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}


def tool_inspect_delta_table_log(delta_table_path: str, limit: int = 10) -> dict[str, Any]:
    """Inspects Delta Lake `_delta_log/*.json` transaction commit records.

    Verifies commit history, operations (WRITE, MERGE, CREATE), schema changes,
    and commit timestamps.

    Args:
        delta_table_path: Absolute or relative path to Delta Lake table directory.
        limit: Maximum number of recent commit log records to return.

    Returns:
        Dictionary containing commit history, table metadata, and schema information.
    """
    resolved_path = _resolve_repo_path(delta_table_path)
    log_dir = os.path.join(resolved_path, "_delta_log")
    if not os.path.exists(log_dir):
        return {
            "status": "ERROR",
            "error": f"Delta Lake transaction log not found at: {log_dir}",
            "delta_table_path": delta_table_path,
        }

    commit_files = sorted(glob.glob(os.path.join(log_dir, "*.json")))
    if not commit_files:
        return {
            "status": "EMPTY_LOG",
            "delta_table_path": delta_table_path,
            "commit_count": 0,
            "commits": [],
        }

    commits = []
    latest_schema = None
    partition_cols = []

    for commit_path in commit_files[-limit:]:
        base_name = os.path.basename(commit_path)
        try:
            version_num = int(base_name.split(".")[0])
        except ValueError:
            version_num = -1

        commit_info: dict[str, Any] = {}
        added_files = 0
        removed_files = 0

        with open(commit_path, encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line.strip())
                    if "commitInfo" in entry:
                        commit_info = entry["commitInfo"]
                    elif "metaData" in entry:
                        meta = entry["metaData"]
                        partition_cols = meta.get("partitionColumns", [])
                        if "schemaString" in meta:
                            try:
                                latest_schema = json.loads(meta["schemaString"])
                            except Exception:
                                latest_schema = meta["schemaString"]
                    elif "add" in entry:
                        added_files += 1
                    elif "remove" in entry:
                        removed_files += 1
                except json.JSONDecodeError:
                    continue

        commits.append(
            {
                "version": version_num,
                "file": base_name,
                "timestamp": commit_info.get("timestamp"),
                "operation": commit_info.get("operation", "UNKNOWN"),
                "engine_info": commit_info.get("engineInfo"),
                "user_metadata": commit_info.get("userMetadata"),
                "added_files_count": added_files,
                "removed_files_count": removed_files,
            }
        )

    return {
        "status": "SUCCESS",
        "delta_table_path": delta_table_path,
        "total_commit_count": len(commit_files),
        "inspected_commits_count": len(commits),
        "partition_columns": partition_cols,
        "latest_schema": latest_schema,
        "commits": commits,
    }


def tool_verify_gxp_audit_lineage(
    run_id: str | None = None,
    delta_table_path: str | None = None,
    rules_path: str = "governance/rules.json",
    tracking_uri: str | None = None,
) -> dict[str, Any]:
    """Executes the LangGraph GxP State Graph Auditor for FDA 21 CFR Part 11 compliance evaluation.

    Args:
        run_id: Optional MLflow run ID to audit.
        delta_table_path: Optional physical Delta Lake table directory path.
        rules_path: Path to Great Expectations rules contract.
        tracking_uri: Optional MLflow tracking URI.

    Returns:
        Comprehensive GxP audit report with compliance status, score, receipts, and findings.
    """
    try:
        if GxPGraphAuditor is None:
            return {
                "compliance_status": "ERROR",
                "error": "GxPGraphAuditor module is not available",
            }

        auditor = GxPGraphAuditor()
        resolved_rules = _resolve_repo_path(rules_path)
        resolved_delta = _resolve_repo_path(delta_table_path) if delta_table_path else None

        report = auditor.audit_run_lineage(
            run_id=run_id,
            delta_table_path=resolved_delta,
            rules_path=resolved_rules,
            tracking_uri=tracking_uri,
            enable_hitl=False,
            log_to_mlflow=False,
        )

        return {
            "compliance_status": report.get("compliance_status"),
            "compliance_score": report.get("compliance_score"),
            "audit_receipt_sha256": report.get("audit_receipt_sha256"),
            "evidence_summary": report.get("evidence_summary", {}),
            "summary": report.get("summary", {}),
            "findings_count": len(report.get("findings", [])),
            "errors": report.get("errors", []),
        }
    except Exception as e:
        return {"compliance_status": "AUDIT_FAILED", "error": str(e)}


def tool_inspect_data_contract(
    rules_path: str = "governance/rules.json",
) -> dict[str, Any]:
    """Inspects Great Expectations data contract rules and GxP assertion suites.

    Args:
        rules_path: Relative or absolute path to rules.json specification.

    Returns:
        Dictionary containing contract metadata, compliance standards, and expectations list.
    """
    resolved_path = _resolve_repo_path(rules_path)
    if not os.path.exists(resolved_path):
        return {"error": f"Rules contract file not found at: {rules_path}"}

    try:
        with open(resolved_path, encoding="utf-8") as f:
            suite_data = json.load(f)

        rules_hash = compute_sha256(resolved_path)
        return {
            "rules_path": rules_path,
            "rules_sha256": rules_hash,
            "data_asset_type": suite_data.get("data_asset_type"),
            "expectation_suite_name": suite_data.get("expectation_suite_name"),
            "meta": suite_data.get("meta", {}),
            "expectations_count": len(suite_data.get("expectations", [])),
            "expectations": suite_data.get("expectations", []),
        }
    except Exception as e:
        return {"error": f"Failed to parse data contract: {e}"}


def tool_validate_clinical_record(
    record: dict[str, Any], rules_path: str = "governance/rules.json"
) -> dict[str, Any]:
    """Validates an in-memory clinical record dictionary against data contract rules.

    Args:
        record: Dictionary representing a clinical row (e.g., person_id, gender_concept_id, birth_datetime).
        rules_path: Path to Great Expectations rules.json contract.

    Returns:
        Dictionary with validation pass/fail status and specific rule assertion results.
    """
    contract = tool_inspect_data_contract(rules_path)
    if "error" in contract:
        return {"success": False, "error": contract["error"]}

    violations = []
    expectations = contract.get("expectations", [])

    for exp in expectations:
        exp_type = exp.get("expectation_type")
        kwargs = exp.get("kwargs", {})
        col = kwargs.get("column")
        meta = exp.get("meta", {})
        severity = meta.get("severity", "ERROR")

        if exp_type == "expect_column_values_to_not_be_null":
            if col not in record or record[col] is None:
                violations.append(
                    {
                        "column": col,
                        "expectation": exp_type,
                        "severity": severity,
                        "message": f"Column '{col}' must not be null.",
                    }
                )

        elif exp_type == "expect_column_values_to_be_in_set":
            value_set = kwargs.get("value_set", [])
            val = record.get(col)
            if val is not None and val not in value_set:
                violations.append(
                    {
                        "column": col,
                        "expectation": exp_type,
                        "severity": severity,
                        "message": f"Value {val} in column '{col}' is not in allowed set {value_set}.",
                    }
                )

        elif exp_type == "expect_column_values_to_match_regex":
            pattern = kwargs.get("regex", "")
            val = str(record.get(col, ""))
            if val and not re.match(pattern, val):
                violations.append(
                    {
                        "column": col,
                        "expectation": exp_type,
                        "severity": severity,
                        "message": f"Value '{val}' in column '{col}' does not match regex '{pattern}'.",
                    }
                )

        elif exp_type == "expect_column_values_to_be_between":
            min_val = kwargs.get("min_value")
            max_val = kwargs.get("max_value")
            val = record.get(col)
            if val is not None:
                try:
                    num_val = float(val)
                    if min_val is not None and num_val < float(min_val):
                        violations.append(
                            {
                                "column": col,
                                "expectation": exp_type,
                                "severity": severity,
                                "message": f"Value {val} in column '{col}' is below allowed minimum {min_val}.",
                            }
                        )
                    elif max_val is not None and num_val > float(max_val):
                        violations.append(
                            {
                                "column": col,
                                "expectation": exp_type,
                                "severity": severity,
                                "message": f"Value {val} in column '{col}' is above allowed maximum {max_val}.",
                            }
                        )
                except (ValueError, TypeError):
                    violations.append(
                        {
                            "column": col,
                            "expectation": exp_type,
                            "severity": severity,
                            "message": f"Value '{val}' in column '{col}' cannot be cast to numeric for bounds comparison.",
                        }
                    )

        elif exp_type == "expect_table_columns_to_match_set":
            col_set = kwargs.get("column_set", [])
            exact_match = kwargs.get("exact_match", False)
            record_cols = set(record.keys())
            missing_cols = set(col_set) - record_cols
            if missing_cols:
                violations.append(
                    {
                        "column": ", ".join(sorted(missing_cols)),
                        "expectation": exp_type,
                        "severity": severity,
                        "message": f"Required columns missing from record: {sorted(missing_cols)}.",
                    }
                )
            if exact_match:
                extra_cols = record_cols - set(col_set)
                if extra_cols:
                    violations.append(
                        {
                            "column": ", ".join(sorted(extra_cols)),
                            "expectation": exp_type,
                            "severity": severity,
                            "message": f"Unexpected extra columns found in record: {sorted(extra_cols)}.",
                        }
                    )

    passed = len(violations) == 0
    return {
        "success": passed,
        "record": record,
        "evaluated_rules_count": len(expectations),
        "violations_count": len(violations),
        "violations": violations,
    }


# =====================================================================
# Foundry MCP Server Class
# =====================================================================


class FoundryMCPServer:
    """Model Context Protocol (MCP) server for Life Sciences Data Foundry.

    Exposes Lakehouse normalization, OMOP CDM v5.4 vocabularies, Delta Lake commit logs,
    and GxP 21 CFR Part 11 governance tools to AI agent assistants.
    """

    def __init__(
        self,
        name: str = "life-sciences-data-foundry",
        host: str = "127.0.0.1",
        port: int = 8080,
    ) -> None:
        """Initializes the Foundry MCP server configuration.

        Args:
            name: Identifier name for the MCP service instance.
            host: Bind host address for the MCP service.
            port: Port number for the MCP service.
        """
        self.name = name
        self.host = host
        self.port = port
        self.server: Any = None
        self._tools_registered = False

        if MCPServer is not None:
            self.server = MCPServer(
                name=self.name,
                instructions=(
                    "Life Sciences Data Foundry Model Context Protocol Server. "
                    "Provides OMOP CDM v5.4 clinical and genomic vocabularies, "
                    "Delta Lake transaction log queries, and GxP 21 CFR Part 11 lineage tools."
                ),
            )
            self.register_tools()

    def register_tools(self) -> None:
        """Registers Foundry ETL, governance evaluation, and query tools with the MCP protocol."""
        if not self.server or self._tools_registered:
            return

        # Register tools using FastMCP / MCPServer decorator
        self.server.tool()(tool_lookup_icd10_to_snomed)
        self.server.tool()(tool_lookup_loinc_concept)
        self.server.tool()(tool_lookup_demographic_concept)
        self.server.tool()(tool_lookup_genomic_variant_concept)
        self.server.tool()(tool_query_vocabulary_mappings)
        self.server.tool()(tool_inspect_omop_table_schema)
        self.server.tool()(tool_get_pipeline_execution_state)
        self.server.tool()(tool_inspect_delta_table_log)
        self.server.tool()(tool_verify_gxp_audit_lineage)
        self.server.tool()(tool_inspect_data_contract)
        self.server.tool()(tool_validate_clinical_record)

        self._tools_registered = True

    def get_server(self) -> Any:
        """Returns the underlying MCP server instance."""
        return self.server

    async def list_tools(self) -> list[Any]:
        """Asynchronously lists all registered MCP tools."""
        if not self.server:
            return []
        return await self.server.list_tools()

    async def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        """Calls a registered MCP tool by name with arguments.

        Args:
            name: Tool function identifier.
            arguments: Keyword arguments for tool execution.

        Returns:
            Result structure from the MCP tool call.
        """
        if not self.server:
            raise RuntimeError("MCP server is not initialized.")
        return await self.server.call_tool(name, arguments or {})

    def serve(self, transport: str = "stdio") -> None:
        """Starts the MCP server event loop.

        Args:
            transport: Transport mode ('stdio' for local agent pipes, 'sse' for HTTP/SSE).
        """
        if not self.server:
            raise RuntimeError("MCP package is required to run the server.")

        print(f"[FOUNDRY MCP] Starting {self.name} on transport '{transport}'...")
        if transport == "stdio":
            self.server.run(transport="stdio")
        elif transport == "sse":
            self.server.run(transport="sse")
        else:
            raise ValueError(f"Unsupported transport '{transport}'. Choose 'stdio' or 'sse'.")


# =====================================================================
# CLI Entry Point
# =====================================================================


def main() -> None:
    """Command-line interface for starting the Foundry MCP server."""
    parser = argparse.ArgumentParser(
        description="Life Sciences Data Foundry — Model Context Protocol (FastMCP) Server"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host address for MCP service (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port number for MCP service (default: 8080)",
    )
    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "sse"],
        default="stdio",
        help="Transport mode: 'stdio' for agent pipes or 'sse' for HTTP (default: stdio)",
    )
    parser.add_argument(
        "--list-tools",
        action="store_true",
        help="Print registered MCP tools and exit",
    )

    args = parser.parse_args()
    server_instance = FoundryMCPServer(host=args.host, port=args.port)

    if args.list_tools:

        async def _print_tools() -> None:
            tools = await server_instance.list_tools()
            print(f"\nRegistered FastMCP Tools ({len(tools)} total):")
            for t in tools:
                print(
                    f"  • {t.name}: {t.description.splitlines()[0] if t.description else 'No description'}"
                )
            print()

        asyncio.run(_print_tools())
        sys.exit(0)

    server_instance.serve(transport=args.transport)


if __name__ == "__main__":
    main()
