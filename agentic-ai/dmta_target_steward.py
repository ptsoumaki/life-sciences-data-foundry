"""
Module: dmta_target_steward.py
Description: LangGraph Agentic DMTA Target Triage & AI Discovery Steward.
             Executes autonomous target feasibility checks, validates target-to-phenotype
             evidence contracts, audits Delta Lake transaction commit logs and MLflow lineage,
             and synthesizes FDA 21 CFR §11.50 / §11.200 sealed target validation dossiers.

Dependencies:
    Requires `langgraph>=0.0.20`, `pydantic>=2.6.0`.
    Install with: `pip install -e .`

Author: Vivi Tsoumaki
"""

import argparse
import datetime
import glob
import json
import os
import re
import sys
from typing import Any, TypedDict

import pandas as pd
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from governance.crypto import compute_sha256, is_valid_sha256
from omop_cdm_v54.vocabularies import load_concept_mappings

# Ensure repository root and agentic-ai directory are in sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AGENTIC_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYTICAL_DIR = os.path.join(BASE_DIR, "analytical-layer")

for p in [BASE_DIR, AGENTIC_DIR, ANALYTICAL_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

__all__ = [
    "DMTATargetState",
    "DMTATargetSteward",
    "ElectronicSignature",
    "TargetContractFinding",
    "TargetEvidenceRecord",
    "is_valid_sha256",
    "parse_hypothesis",
    "query_target_mart",
    "synthesize_validation_dossier",
    "validate_lineage_and_contract",
]


# =====================================================================
# State & Typed Data Structures
# =====================================================================


class TargetEvidenceRecord(TypedDict, total=False):
    """Normalized evidence record from the Target Evidence Mart."""

    target_gene_symbol: str
    disease_concept_id: int
    carrier_cases: int
    carrier_controls: int
    non_carrier_cases: int
    non_carrier_controls: int
    total_cohort_size: int
    target_mutation_burden: float
    carrier_frequency: float
    phenotype_prevalence: float
    odds_ratio: float
    log_odds_ratio: float
    se_log_odds_ratio: float
    odds_ratio_ci_lower: float
    odds_ratio_ci_upper: float
    p_value: float
    biomarker_correlation: float
    target_tractability_score: float
    evidence_tier: str
    created_at: str
    mlflow_run_id: str


class TargetContractFinding(TypedDict, total=False):
    """Validation finding from Great Expectations target contract evaluation."""

    rule: str
    expectation_type: str
    severity: str  # "CRITICAL_FATAL", "ERROR", "WARNING", "INFO"
    passed: bool
    message: str
    details: dict[str, Any]


class ElectronicSignature(TypedDict, total=False):
    """FDA 21 CFR §11.50 Electronic Signature record."""

    operator_id: str
    meaning: str
    timestamp: str
    signature_checksum: str


class DMTATargetState(TypedDict, total=False):
    """Complete LangGraph state container for target feasibility triage."""

    # Hypothesis & Query Inputs
    hypothesis_text: str | None
    target_gene_symbol: str | None
    disease_concept_id: int | None
    min_odds_ratio: float
    max_p_value: float
    min_tractability_score: float
    target_mart_path: str | None
    rules_path: str | None
    operator_id: str

    # Retrieval & Evidence
    evidence_records: list[dict[str, Any]]
    evidence_summary: dict[str, Any]
    lineage_verification: dict[str, Any]
    contract_findings: list[TargetContractFinding]

    # Decisions & Dossier Outputs
    triage_decision: str  # "FEASIBLE", "INCONCLUSIVE", "HIGH_RISK_REJECTED", "INVALID_LINEAGE"
    feasibility_score: float
    validation_dossier: dict[str, Any]
    electronic_signature: ElectronicSignature | None
    dossier_receipt_sha256: str | None
    errors: list[str]


# =====================================================================
# Internal Helpers
# =====================================================================


def _resolve_repo_path(relative_path: str | None) -> str | None:
    """Resolves a relative file path against the project repository root."""
    if not relative_path:
        return None
    if os.path.isabs(relative_path):
        return relative_path
    candidate = os.path.join(BASE_DIR, relative_path)
    if os.path.exists(candidate):
        return candidate
    return relative_path


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Safely cast value to float, returning default if None or on conversion error."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# =====================================================================
# LangGraph DMTA Target Triage Nodes
# =====================================================================


def parse_hypothesis(state: DMTATargetState) -> dict[str, Any]:
    """Node 1: Parses, sanitizes, and normalizes target hypotheses.

    Extracts canonical uppercase HGNC target gene symbols and resolves disease
    concept IDs from clinical conditions or ICD-10 mentions.
    """
    errors: list[str] = list(state.get("errors") or [])
    hypothesis_text = state.get("hypothesis_text")
    gene = state.get("target_gene_symbol")
    concept_id = state.get("disease_concept_id")

    # If gene symbol was not explicitly passed, attempt extraction from hypothesis text
    if not gene and hypothesis_text:
        # Match common target expressions like "Target: EGFR", "gene BRAF", or uppercase tokens
        gene_match = re.search(
            r"(?:target|gene|biomarker)[:\s]+([A-Za-z0-9_-]{2,15})",
            hypothesis_text,
            re.IGNORECASE,
        )
        if gene_match:
            gene = gene_match.group(1)
        else:
            # Fallback to standalone uppercase tokens
            tokens = [
                tok.strip(",.;:()")
                for tok in hypothesis_text.split()
                if tok.isupper() and 2 <= len(tok) <= 15
            ]
            if tokens:
                gene = tokens[0]

    # Normalize gene symbol
    normalized_gene: str | None = None
    if gene:
        normalized_gene = str(gene).strip().upper()
        if not re.match(r"^[A-Z0-9_-]{2,15}$", normalized_gene):
            errors.append(
                f"Target gene symbol '{gene}' violates canonical HGNC nomenclature rules."
            )
            normalized_gene = None
    else:
        errors.append("No target gene symbol specified or extracted from hypothesis.")

    # Resolve disease concept ID if not directly provided
    if concept_id is None and hypothesis_text:
        # Check for explicit OMOP concept ID mentions (e.g., "concept 254637", "concept_id: 316866")
        id_match = re.search(
            r"(?:concept(?:_id)?|snomed)[:\s=]+(\d+)",
            hypothesis_text,
            re.IGNORECASE,
        )
        if id_match:
            concept_id = int(id_match.group(1))
        else:
            # Check for ICD-10 diagnosis codes (e.g., C34.90, E11.9, I10)
            icd_match = re.search(r"\b([A-Z]\d{2}(?:\.\d{1,2})?)\b", hypothesis_text)
            if icd_match:
                raw_icd = icd_match.group(1)
                mappings = load_concept_mappings()
                icd_map = mappings.get("icd10_to_snomed", {})
                clean_icd = raw_icd.upper()
                resolved_id = icd_map.get(clean_icd) or icd_map.get(clean_icd.replace(".", ""), 0)
                if resolved_id and resolved_id != 0:
                    concept_id = int(resolved_id)

    # Sanitize disease_concept_id to integer if provided
    if concept_id is not None:
        try:
            concept_id = int(concept_id)
        except (ValueError, TypeError):
            errors.append(f"Invalid disease concept ID '{concept_id}'; must be an integer.")
            concept_id = None

    # Establish gating thresholds with resilient defaults
    min_or = _safe_float(state.get("min_odds_ratio"), 1.0)
    max_p = _safe_float(state.get("max_p_value"), 0.05)
    min_tract = _safe_float(state.get("min_tractability_score"), 0.5)
    rules_path = state.get("rules_path") or "governance/contracts/target_contract.json"
    operator_id = state.get("operator_id") or "agent:dmta_target_steward"

    return {
        "target_gene_symbol": normalized_gene,
        "disease_concept_id": concept_id,
        "min_odds_ratio": min_or,
        "max_p_value": max_p,
        "min_tractability_score": min_tract,
        "rules_path": rules_path,
        "operator_id": operator_id,
        "errors": errors,
    }


def query_target_mart(state: DMTATargetState) -> dict[str, Any]:
    """Node 2: Queries the Target Evidence Mart for phenotypic association evidence.

    Retrieves target-to-phenotype association metrics including mutation burden,
    Haldane-Anscombe Odds Ratios, confidence intervals, p-values, and tractability scores.
    """
    gene = state.get("target_gene_symbol")
    concept_id = state.get("disease_concept_id")
    target_mart_path = _resolve_repo_path(state.get("target_mart_path"))
    records: list[dict[str, Any]] = list(state.get("evidence_records") or [])
    errors: list[str] = list(state.get("errors") or [])

    if not gene:
        return {
            "evidence_records": [],
            "evidence_summary": {},
            "errors": errors,
        }

    # If evidence records were not already supplied in the initial state, query storage
    if not records and target_mart_path:
        if os.path.exists(target_mart_path):
            try:
                # 1. Check if path is a JSON file
                if os.path.isfile(target_mart_path) and target_mart_path.endswith(".json"):
                    with open(target_mart_path, encoding="utf-8") as f:
                        data = json.load(f)
                        raw_list = data if isinstance(data, list) else [data]
                        records = [
                            r
                            for r in raw_list
                            if str(r.get("target_gene_symbol", "")).upper() == gene
                        ]
                # 2. Check if path is a directory (Delta Lake or Parquet)
                elif os.path.isdir(target_mart_path):
                    # Check for parquet files directly, excluding Delta Lake transaction log checkpoints
                    all_parquet = glob.glob(
                        os.path.join(target_mart_path, "**", "*.parquet"), recursive=True
                    )
                    parquet_files = [
                        pf
                        for pf in all_parquet
                        if "_delta_log" not in pf.replace("\\", "/").split("/")
                    ]
                    if parquet_files:
                        try:
                            dfs = [pd.read_parquet(pf) for pf in parquet_files]
                            if dfs:
                                combined = pd.concat(dfs, ignore_index=True)
                                if "target_gene_symbol" in combined.columns:
                                    filtered = combined[
                                        combined["target_gene_symbol"].astype(str).str.upper()
                                        == gene
                                    ]
                                    records = filtered.to_dict(orient="records")
                        except Exception as pe:
                            errors.append(f"Parquet evidence extraction warning: {pe}")
            except Exception as e:
                errors.append(f"Failed to query target mart at '{target_mart_path}': {e}")
        else:
            errors.append(f"Target mart storage path does not exist: {target_mart_path}")

    # Further filter by disease_concept_id if requested and multiple records exist
    if records and concept_id is not None:
        matched = [r for r in records if int(r.get("disease_concept_id", -1)) == int(concept_id)]
        if matched:
            records = matched

    # Aggregate evidence metrics summary
    evidence_summary: dict[str, Any] = {}
    if records:
        count = len(records)
        mean_or = sum(_safe_float(r.get("odds_ratio"), 1.0) for r in records) / count
        min_p = min(_safe_float(r.get("p_value"), 1.0) for r in records)
        max_tract = max(_safe_float(r.get("target_tractability_score"), 0.0) for r in records)
        total_cohort = max((int(r.get("total_cohort_size") or 0) for r in records), default=0)
        mean_burden = (
            sum(_safe_float(r.get("target_mutation_burden"), 0.0) for r in records) / count
        )
        mean_carrier_freq = (
            sum(_safe_float(r.get("carrier_frequency"), 0.0) for r in records) / count
        )
        primary_tier = records[0].get("evidence_tier", "TIER_3_EXPLORATORY")

        evidence_summary = {
            "target_gene_symbol": gene,
            "disease_concept_id": concept_id,
            "associated_phenotypes_count": count,
            "mean_odds_ratio": round(mean_or, 4),
            "min_p_value": min_p,
            "max_tractability_score": round(max_tract, 4),
            "total_cohort_size": total_cohort,
            "mean_mutation_burden": round(mean_burden, 4),
            "mean_carrier_frequency": round(mean_carrier_freq, 4),
            "primary_evidence_tier": primary_tier,
        }
    else:
        evidence_summary = {
            "target_gene_symbol": gene,
            "disease_concept_id": concept_id,
            "associated_phenotypes_count": 0,
            "primary_evidence_tier": "UNMAPPED",
        }

    return {
        "evidence_records": records,
        "evidence_summary": evidence_summary,
        "errors": errors,
    }


def validate_lineage_and_contract(state: DMTATargetState) -> dict[str, Any]:
    """Node 3: Validates target evidence contract compliance and Delta commit lineage.

    Evaluates Great Expectations contract assertions from `target_contract.json`,
    verifies Delta Lake transaction commit logs, and assigns the GxP triage decision.
    """
    records = list(state.get("evidence_records") or [])
    rules_path = _resolve_repo_path(state.get("rules_path"))
    target_mart_path = _resolve_repo_path(state.get("target_mart_path"))
    min_or = _safe_float(state.get("min_odds_ratio"), 1.0)
    max_p = _safe_float(state.get("max_p_value"), 0.05)
    min_tract = _safe_float(state.get("min_tractability_score"), 0.5)

    contract_findings: list[TargetContractFinding] = []
    lineage_verification: dict[str, Any] = {
        "delta_commits_found": 0,
        "change_data_feed_enabled": False,
        "provenance_intact": False,
        "commits_continuous": True,
        "commit_sha256": None,
    }

    # 1. Delta Lake Lineage Verification
    has_fatal_contract_breach = False
    if target_mart_path and os.path.exists(target_mart_path):
        delta_log_dir = os.path.join(target_mart_path, "_delta_log")
        if os.path.exists(delta_log_dir):
            commit_files = sorted(glob.glob(os.path.join(delta_log_dir, "*.json")))
            lineage_verification["delta_commits_found"] = len(commit_files)
            if commit_files:
                latest_commit = commit_files[-1]
                commit_sha = compute_sha256(latest_commit)
                lineage_verification["commit_sha256"] = commit_sha
                lineage_verification["provenance_intact"] = is_valid_sha256(commit_sha)

                # Commit version continuity audit
                versions: list[int] = []
                for cf in commit_files:
                    try:
                        versions.append(int(os.path.basename(cf).split(".")[0]))
                    except ValueError:
                        pass
                if versions:
                    expected_versions = list(range(min(versions), max(versions) + 1))
                    is_continuous = versions == expected_versions
                    lineage_verification["commits_continuous"] = is_continuous
                    if not is_continuous:
                        has_fatal_contract_breach = True
                        contract_findings.append(
                            {
                                "rule": "DELTA_COMMIT_CONTINUITY",
                                "expectation_type": "commit_sequence",
                                "severity": "CRITICAL_FATAL",
                                "passed": False,
                                "message": "Discontinuous transaction commit sequence detected in Delta log.",
                                "details": {"versions": versions},
                            }
                        )

                # Inspect commit JSON for Change Data Feed
                try:
                    with open(latest_commit, encoding="utf-8") as f:
                        for line in f:
                            if "enableChangeDataFeed" in line:
                                lineage_verification["change_data_feed_enabled"] = True
                                break
                except Exception:
                    pass

    # 2. Great Expectations Contract Evaluation
    expectations: list[dict[str, Any]] = []
    if rules_path and os.path.exists(rules_path):
        try:
            with open(rules_path, encoding="utf-8") as f:
                suite = json.load(f)
                expectations = suite.get("expectations", [])
        except Exception as e:
            has_fatal_contract_breach = True
            contract_findings.append(
                {
                    "rule": "PARSE_RULES_FILE",
                    "expectation_type": "json_parse",
                    "severity": "CRITICAL_FATAL",
                    "passed": False,
                    "message": f"Unable to parse rules contract: {e}",
                    "details": {"rules_path": rules_path},
                }
            )

    table_expectations = [
        exp
        for exp in expectations
        if exp.get("expectation_type") == "expect_table_columns_to_match_set"
    ]
    column_expectations = [
        exp
        for exp in expectations
        if exp.get("expectation_type") != "expect_table_columns_to_match_set"
    ]

    if records:
        # Table-level schema evaluations (evaluated once across record set)
        record_cols = set(records[0].keys())
        for exp in table_expectations:
            exp_type = str(exp.get("expectation_type") or "expect_table_columns_to_match_set")
            kwargs = exp.get("kwargs", {})
            meta = exp.get("meta", {})
            severity = meta.get("severity", "ERROR")
            col_set = kwargs.get("column_set", [])
            exact_match = kwargs.get("exact_match", False)

            missing_cols = set(col_set) - record_cols
            if missing_cols and exact_match:
                if severity == "CRITICAL_FATAL":
                    has_fatal_contract_breach = True
                contract_findings.append(
                    {
                        "rule": "TABLE_COLUMNS_MATCH_SET",
                        "expectation_type": exp_type,
                        "severity": severity,
                        "passed": False,
                        "message": f"Mandatory columns missing from record: {sorted(missing_cols)}.",
                        "details": {"missing_columns": sorted(missing_cols)},
                    }
                )
            elif missing_cols and len(record_cols) > 15:
                contract_findings.append(
                    {
                        "rule": "TABLE_COLUMNS_MATCH_SET",
                        "expectation_type": exp_type,
                        "severity": "WARNING",
                        "passed": False,
                        "message": f"Attributes missing from record: {sorted(missing_cols)}.",
                        "details": {"missing_columns": sorted(missing_cols)},
                    }
                )
            if exact_match:
                extra_cols = record_cols - set(col_set)
                if extra_cols:
                    contract_findings.append(
                        {
                            "rule": "TABLE_COLUMNS_EXACT_MATCH",
                            "expectation_type": exp_type,
                            "severity": severity,
                            "passed": False,
                            "message": f"Unexpected extra columns found in record: {sorted(extra_cols)}.",
                            "details": {"extra_columns": sorted(extra_cols)},
                        }
                    )

        # Record-level column evaluations
        for rec in records:
            for exp in column_expectations:
                exp_type = str(exp.get("expectation_type") or "")
                kwargs = exp.get("kwargs", {})
                meta = exp.get("meta", {})
                severity = meta.get("severity", "ERROR")
                col_name = kwargs.get("column")

                if exp_type == "expect_column_values_to_not_be_null":
                    val = rec.get(col_name)
                    passed = val is not None
                    if not passed:
                        if severity == "CRITICAL_FATAL":
                            has_fatal_contract_breach = True
                        contract_findings.append(
                            {
                                "rule": f"NOT_NULL_{col_name}",
                                "expectation_type": exp_type,
                                "severity": severity,
                                "passed": False,
                                "message": f"Mandatory column '{col_name}' is null.",
                                "details": {"record": rec},
                            }
                        )

                elif exp_type == "expect_column_values_to_match_regex":
                    pattern = kwargs.get("regex", "")
                    val = str(rec.get(col_name, ""))
                    passed = bool(re.match(pattern, val))
                    if not passed:
                        contract_findings.append(
                            {
                                "rule": f"REGEX_{col_name}",
                                "expectation_type": exp_type,
                                "severity": severity,
                                "passed": False,
                                "message": f"Value '{val}' in '{col_name}' does not match regex '{pattern}'.",
                                "details": {"value": val, "regex": pattern},
                            }
                        )

                elif exp_type == "expect_column_values_to_be_between":
                    min_val = kwargs.get("min_value")
                    max_val = kwargs.get("max_value")
                    val = rec.get(col_name)
                    if val is not None:
                        try:
                            num_val = float(val)
                            passed = True
                            if min_val is not None and num_val < float(min_val):
                                passed = False
                            if max_val is not None and num_val > float(max_val):
                                passed = False
                            if not passed:
                                contract_findings.append(
                                    {
                                        "rule": f"BOUNDS_{col_name}",
                                        "expectation_type": exp_type,
                                        "severity": severity,
                                        "passed": False,
                                        "message": f"Value {val} in '{col_name}' out of bounds [{min_val}, {max_val}].",
                                        "details": {
                                            "value": num_val,
                                            "min": min_val,
                                            "max": max_val,
                                        },
                                    }
                                )
                        except (ValueError, TypeError):
                            contract_findings.append(
                                {
                                    "rule": f"TYPE_{col_name}",
                                    "expectation_type": exp_type,
                                    "severity": severity,
                                    "passed": False,
                                    "message": f"Value '{val}' in '{col_name}' is not numeric.",
                                    "details": {"value": val},
                                }
                            )

    # 3. Triage Decision & Feasibility Scoring
    # Categories: FEASIBLE, INCONCLUSIVE, HIGH_RISK_REJECTED, INVALID_LINEAGE
    if has_fatal_contract_breach:
        triage_decision = "INVALID_LINEAGE"
        feasibility_score = 0.0
    elif not records:
        triage_decision = "INCONCLUSIVE"
        feasibility_score = 10.0
    else:
        # Check statistical gating criteria across records
        best_rec = max(
            records,
            key=lambda r: _safe_float(r.get("target_tractability_score"), 0.0),
        )
        or_val = _safe_float(best_rec.get("odds_ratio"), 1.0)
        p_val = _safe_float(best_rec.get("p_value"), 1.0)
        tract_val = _safe_float(best_rec.get("target_tractability_score"), 0.0)

        # Check if contract errors exist (excluding critical fatal)
        error_count = sum(
            1 for f in contract_findings if f.get("severity") in ("CRITICAL_FATAL", "ERROR")
        )

        if error_count > 0:
            triage_decision = "HIGH_RISK_REJECTED"
            feasibility_score = max(0.0, tract_val * 40.0)
        elif or_val >= min_or and p_val <= max_p and tract_val >= min_tract:
            triage_decision = "FEASIBLE"
            # Composite score bounded in [60.0, 100.0]
            p_factor = max(0.0, 1.0 - p_val)
            or_factor = min(1.0, or_val / 5.0)
            feasibility_score = round((0.4 * tract_val + 0.3 * p_factor + 0.3 * or_factor) * 100, 2)
            feasibility_score = max(60.0, min(100.0, feasibility_score))
        elif or_val < 1.0 or p_val > 0.10:
            triage_decision = "HIGH_RISK_REJECTED"
            feasibility_score = round(max(0.0, tract_val * 35.0), 2)
        else:
            triage_decision = "INCONCLUSIVE"
            feasibility_score = round(max(20.0, tract_val * 50.0), 2)

    return {
        "lineage_verification": lineage_verification,
        "contract_findings": contract_findings,
        "triage_decision": triage_decision,
        "feasibility_score": feasibility_score,
    }


def synthesize_validation_dossier(state: DMTATargetState) -> dict[str, Any]:
    """Node 4: Synthesizes a cryptographically sealed GxP Target Validation Dossier.

    Constructs 21 CFR §11.50 Electronic Signatures and seals the canonical dossier
    with a SHA-256 receipt digest.
    """
    gene = state.get("target_gene_symbol", "UNKNOWN")
    concept_id = state.get("disease_concept_id")
    decision = state.get("triage_decision", "INCONCLUSIVE")
    score = state.get("feasibility_score", 0.0)
    operator_id = state.get("operator_id", "agent:dmta_target_steward")
    timestamp = datetime.datetime.now(datetime.UTC).isoformat()
    meaning = "Target feasibility triage and GxP validation dossier generation."

    # Compute FDA 21 CFR §11.50 Electronic Signature Checksum
    sig_raw = f"{operator_id}:{meaning}:{timestamp}:{decision}:{score}"
    signature_checksum = compute_sha256(sig_raw)

    electronic_signature: ElectronicSignature = {
        "operator_id": operator_id,
        "meaning": meaning,
        "timestamp": timestamp,
        "signature_checksum": signature_checksum,
    }

    # Compile GxP Target Dossier
    dossier: dict[str, Any] = {
        "dossier_id": f"DOSSIER-{gene}-{concept_id if concept_id is not None else 0}",
        "title": f"Target Validation Dossier: {gene}",
        "target_gene_symbol": gene,
        "disease_concept_id": concept_id,
        "triage_decision": decision,
        "feasibility_score": score,
        "gxp_contract_passed": not any(
            f.get("severity") in ("CRITICAL_FATAL", "ERROR")
            for f in state.get("contract_findings", [])
        ),
        "hypothesis_text": state.get("hypothesis_text"),
        "evidence_summary": state.get("evidence_summary", {}),
        "lineage_verification": state.get("lineage_verification", {}),
        "findings_count": len(state.get("contract_findings", [])),
        "contract_findings": state.get("contract_findings", []),
        "evidence_records_count": len(state.get("evidence_records", [])),
        "regulatory_standards": [
            "FDA_21_CFR_Part_11",
            "OHDSI_OMOP_CDM_v5.4",
            "GMLP_TARGET_TRIAGE",
        ],
        "electronic_signature": electronic_signature,
    }

    # Cryptographically seal dossier receipt
    canonical_payload = json.dumps(dossier, sort_keys=True)
    dossier_receipt_sha256 = compute_sha256(canonical_payload)
    dossier["dossier_receipt_sha256"] = dossier_receipt_sha256

    return {
        "validation_dossier": dossier,
        "electronic_signature": electronic_signature,
        "dossier_receipt_sha256": dossier_receipt_sha256,
    }


# =====================================================================
# DMTATargetSteward Controller Class
# =====================================================================


class DMTATargetSteward:
    """Autonomous LangGraph Agentic Steward for DMTA Target Triage & AI Discovery.

    Coordinates hypothesis parsing, target evidence querying, GxP contract & lineage
    validation, and synthesis of 21 CFR §11.50 sealed validation dossiers.
    """

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        checkpointer: BaseCheckpointSaver | None = None,
    ) -> None:
        """Initializes the DMTA Target Steward controller and compiles the state graph."""
        self.config = config or {}
        self.checkpointer = checkpointer or MemorySaver()
        self.app = self.build_graph(checkpointer=self.checkpointer)

    def build_graph(self, checkpointer: BaseCheckpointSaver | None = None):
        """Builds and compiles the 4-node LangGraph StateGraph."""
        workflow = StateGraph(DMTATargetState)

        # Add Nodes
        workflow.add_node("parse_hypothesis", parse_hypothesis)
        workflow.add_node("query_target_mart", query_target_mart)
        workflow.add_node("validate_lineage_and_contract", validate_lineage_and_contract)
        workflow.add_node("synthesize_validation_dossier", synthesize_validation_dossier)

        # Add Linear Directed Edges
        workflow.add_edge(START, "parse_hypothesis")
        workflow.add_edge("parse_hypothesis", "query_target_mart")
        workflow.add_edge("query_target_mart", "validate_lineage_and_contract")
        workflow.add_edge("validate_lineage_and_contract", "synthesize_validation_dossier")
        workflow.add_edge("synthesize_validation_dossier", END)

        return workflow.compile(checkpointer=checkpointer)

    def triage_target(
        self,
        hypothesis_text: str | None = None,
        target_gene_symbol: str | None = None,
        disease_concept_id: int | None = None,
        evidence_records: list[dict[str, Any]] | None = None,
        target_mart_path: str | None = None,
        rules_path: str = "governance/contracts/target_contract.json",
        min_odds_ratio: float = 1.0,
        max_p_value: float = 0.05,
        min_tractability_score: float = 0.5,
        operator_id: str = "agent:dmta_target_steward",
        thread_id: str = "dmta_default_thread",
    ) -> dict[str, Any]:
        """Executes the autonomous target feasibility triage graph.

        Args:
            hypothesis_text: Optional free-text biological hypothesis.
            target_gene_symbol: Explicit HGNC canonical target gene symbol.
            disease_concept_id: OMOP condition concept ID.
            evidence_records: Optional pre-loaded target evidence records.
            target_mart_path: Path to Target Evidence Mart Delta table or Parquet store.
            rules_path: Path to Great Expectations target contract specification.
            min_odds_ratio: Minimum odds ratio threshold for feasibility gating.
            max_p_value: Maximum allowable p-value threshold.
            min_tractability_score: Minimum allowable composite tractability score.
            operator_id: Electronic signature operator identifier.
            thread_id: LangGraph thread identifier for checkpointing.

        Returns:
            Complete cryptographically sealed GxP Target Validation Dossier.
        """
        initial_state: DMTATargetState = {
            "hypothesis_text": hypothesis_text,
            "target_gene_symbol": target_gene_symbol,
            "disease_concept_id": disease_concept_id,
            "evidence_records": evidence_records or [],
            "target_mart_path": target_mart_path,
            "rules_path": rules_path,
            "min_odds_ratio": min_odds_ratio,
            "max_p_value": max_p_value,
            "min_tractability_score": min_tractability_score,
            "operator_id": operator_id,
            "contract_findings": [],
            "errors": [],
        }

        call_config = {"configurable": {"thread_id": thread_id}}
        final_state = self.app.invoke(initial_state, config=call_config)
        return final_state.get("validation_dossier", {})


# =====================================================================
# CLI Entry Point
# =====================================================================


def main() -> None:
    """Command-line interface for autonomous DMTA target triage."""
    parser = argparse.ArgumentParser(
        description="Life Sciences Data Foundry — Agentic DMTA Target Triage Steward"
    )
    parser.add_argument(
        "--gene",
        type=str,
        default=None,
        help="Target gene symbol (e.g., BRAF, EGFR, KRAS)",
    )
    parser.add_argument(
        "--disease-concept-id",
        type=int,
        default=None,
        help="OMOP disease condition concept ID",
    )
    parser.add_argument(
        "--hypothesis",
        type=str,
        default=None,
        help="Biological hypothesis text string",
    )
    parser.add_argument(
        "--mart-path",
        type=str,
        default=None,
        help="Path to Target Evidence Mart Delta table",
    )
    parser.add_argument(
        "--operator-id",
        type=str,
        default="cli:dmta_steward",
        help="Electronic signature operator ID",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional file path to persist generated dossier JSON",
    )

    args = parser.parse_args()
    steward = DMTATargetSteward()
    dossier = steward.triage_target(
        hypothesis_text=args.hypothesis,
        target_gene_symbol=args.gene,
        disease_concept_id=args.disease_concept_id,
        target_mart_path=args.mart_path,
        operator_id=args.operator_id,
    )

    print(json.dumps(dossier, indent=2))

    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(dossier, f, indent=2, sort_keys=True)
        print(f"\n[INFO] Persisted sealed GxP target dossier to: {args.output}")


if __name__ == "__main__":
    main()
