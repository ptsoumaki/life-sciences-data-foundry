"""
Module: test_dmta_steward.py
Description: Comprehensive unit test suite for LangGraph Agentic DMTA Target Steward (Phase 12).
Author: Vivi Tsoumaki
"""

import json
import os
import sys

# Ensure repository root and agentic-ai directory are in sys.path
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
agentic_dir = os.path.join(base_dir, "agentic-ai")
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)
if agentic_dir not in sys.path:
    sys.path.insert(0, agentic_dir)

from dmta_target_steward import (  # noqa: E402
    DMTATargetState,
    DMTATargetSteward,
    is_valid_sha256,
    main,
    parse_hypothesis,
    query_target_mart,
    synthesize_validation_dossier,
    validate_lineage_and_contract,
)


def test_steward_graph_compilation():
    """Validates that DMTATargetSteward builds and compiles all 4 LangGraph nodes."""
    steward = DMTATargetSteward()
    assert steward.app is not None

    nodes = steward.app.get_graph().nodes
    assert "parse_hypothesis" in nodes
    assert "query_target_mart" in nodes
    assert "validate_lineage_and_contract" in nodes
    assert "synthesize_validation_dossier" in nodes


def test_parse_hypothesis_structured_and_freetext():
    """Validates hypothesis parsing for structured inputs, free-text regex, and ICD-10 resolution."""
    # 1. Explicit structured inputs
    state_structured: DMTATargetState = {
        "target_gene_symbol": "braf",
        "disease_concept_id": 254637,
    }
    res_1 = parse_hypothesis(state_structured)
    assert res_1["target_gene_symbol"] == "BRAF"
    assert res_1["disease_concept_id"] == 254637
    assert len(res_1["errors"]) == 0

    # 2. Free-text with "Target: EGFR" and concept ID
    state_freetext: DMTATargetState = {
        "hypothesis_text": "Investigate Target: EGFR in non-small cell lung cancer concept 254637",
    }
    res_2 = parse_hypothesis(state_freetext)
    assert res_2["target_gene_symbol"] == "EGFR"
    assert res_2["disease_concept_id"] == 254637

    # 3. Free-text with ICD-10 code resolution (C34.90 -> SNOMED 254637)
    state_icd: DMTATargetState = {
        "hypothesis_text": "Evaluate KRAS mutations in malignant neoplasm of lung with ICD-10 C34.90",
    }
    res_3 = parse_hypothesis(state_icd)
    assert res_3["target_gene_symbol"] == "KRAS"
    assert res_3["disease_concept_id"] == 254637

    # 4. Invalid gene symbol
    state_invalid: DMTATargetState = {
        "target_gene_symbol": "!",
    }
    res_4 = parse_hypothesis(state_invalid)
    assert res_4["target_gene_symbol"] is None
    assert any("violates canonical HGNC nomenclature" in e for e in res_4["errors"])

    # 5. Missing gene symbol
    state_empty: DMTATargetState = {}
    res_5 = parse_hypothesis(state_empty)
    assert res_5["target_gene_symbol"] is None
    assert any("No target gene symbol specified" in e for e in res_5["errors"])

    # 6. String concept ID conversion
    state_str_concept: DMTATargetState = {
        "target_gene_symbol": "BRAF",
        "disease_concept_id": "254637",  # type: ignore[typeddict-item]
    }
    res_6 = parse_hypothesis(state_str_concept)
    assert res_6["disease_concept_id"] == 254637

    # 7. Invalid string concept ID handling
    state_inv_concept: DMTATargetState = {
        "target_gene_symbol": "BRAF",
        "disease_concept_id": "invalid_id",  # type: ignore[typeddict-item]
    }
    res_7 = parse_hypothesis(state_inv_concept)
    assert res_7["disease_concept_id"] is None
    assert any("Invalid disease concept ID" in e for e in res_7["errors"])


def test_query_target_mart_in_memory_and_file(tmp_path):
    """Validates target evidence retrieval from in-memory records and on-disk JSON/storage."""
    # 1. In-memory preloaded evidence records
    mock_record = {
        "target_gene_symbol": "BRAF",
        "disease_concept_id": 254637,
        "odds_ratio": 3.5,
        "p_value": 0.0001,
        "target_tractability_score": 0.85,
        "target_mutation_burden": 1.2,
        "carrier_frequency": 0.08,
        "total_cohort_size": 500,
        "evidence_tier": "TIER_1_VALIDATED",
    }
    state_mem: DMTATargetState = {
        "target_gene_symbol": "BRAF",
        "evidence_records": [mock_record],
    }
    res_mem = query_target_mart(state_mem)
    assert len(res_mem["evidence_records"]) == 1
    summary = res_mem["evidence_summary"]
    assert summary["target_gene_symbol"] == "BRAF"
    assert summary["mean_odds_ratio"] == 3.5
    assert summary["min_p_value"] == 0.0001
    assert summary["max_tractability_score"] == 0.85

    # 2. Query from JSON file
    mart_file = (tmp_path / "target_mart.json").as_posix()
    with open(mart_file, "w", encoding="utf-8") as f:
        json.dump([mock_record], f)

    state_file: DMTATargetState = {
        "target_gene_symbol": "BRAF",
        "target_mart_path": mart_file,
    }
    res_file = query_target_mart(state_file)
    assert len(res_file["evidence_records"]) == 1
    assert res_file["evidence_summary"]["associated_phenotypes_count"] == 1

    # 3. Target not found
    state_missing: DMTATargetState = {
        "target_gene_symbol": "NOTFOUND",
        "target_mart_path": mart_file,
    }
    res_missing = query_target_mart(state_missing)
    assert len(res_missing["evidence_records"]) == 0
    assert res_missing["evidence_summary"]["associated_phenotypes_count"] == 0

    # 4. Safe None handling in records
    record_with_nones = {
        "target_gene_symbol": "BRAF",
        "disease_concept_id": 254637,
        "odds_ratio": None,
        "p_value": None,
        "target_tractability_score": None,
        "target_mutation_burden": None,
        "carrier_frequency": None,
        "total_cohort_size": None,
    }
    state_none: DMTATargetState = {
        "target_gene_symbol": "BRAF",
        "evidence_records": [record_with_nones],
    }
    res_none = query_target_mart(state_none)
    assert res_none["evidence_summary"]["mean_odds_ratio"] == 1.0
    assert res_none["evidence_summary"]["max_tractability_score"] == 0.0
    assert res_none["evidence_summary"]["total_cohort_size"] == 0


def test_validate_lineage_and_contract(tmp_path):
    """Validates Great Expectations target contract evaluation and Delta commit lineage verification."""
    # Create mock Delta Lake directory with _delta_log
    delta_dir = tmp_path / "delta_target_mart"
    log_dir = delta_dir / "_delta_log"
    log_dir.mkdir(parents=True)

    commit_file = log_dir / "00000000000000000000.json"
    commit_payload = (
        json.dumps({"commitInfo": {"timestamp": 1726963200000, "operation": "WRITE"}})
        + "\n"
        + json.dumps(
            {
                "metaData": {
                    "id": "test-id",
                    "format": {"provider": "parquet"},
                    "configuration": {"delta.enableChangeDataFeed": "true"},
                }
            }
        )
        + "\n"
    )
    commit_file.write_text(commit_payload, encoding="utf-8")

    # Compliant record
    valid_record = {
        "target_gene_symbol": "EGFR",
        "disease_concept_id": 254637,
        "odds_ratio": 4.2,
        "p_value": 0.00005,
        "target_tractability_score": 0.88,
        "biomarker_correlation": 0.45,
        "total_cohort_size": 1000,
        "evidence_tier": "TIER_1_VALIDATED",
    }
    state_valid: DMTATargetState = {
        "target_gene_symbol": "EGFR",
        "evidence_records": [valid_record],
        "target_mart_path": delta_dir.as_posix(),
        "rules_path": "governance/contracts/target_contract.json",
        "min_odds_ratio": 1.5,
        "max_p_value": 0.01,
        "min_tractability_score": 0.60,
    }
    res_valid = validate_lineage_and_contract(state_valid)
    assert res_valid["triage_decision"] == "FEASIBLE"
    assert res_valid["feasibility_score"] >= 70.0
    assert res_valid["lineage_verification"]["delta_commits_found"] == 1
    assert res_valid["lineage_verification"]["change_data_feed_enabled"] is True
    assert res_valid["lineage_verification"]["commits_continuous"] is True
    assert is_valid_sha256(res_valid["lineage_verification"]["commit_sha256"]) is True

    # Discontinuous Delta commit sequence
    disc_delta_dir = tmp_path / "delta_discontinuous"
    disc_log_dir = disc_delta_dir / "_delta_log"
    disc_log_dir.mkdir(parents=True)
    (disc_log_dir / "00000000000000000000.json").write_text("{}\n", encoding="utf-8")
    (disc_log_dir / "00000000000000000002.json").write_text("{}\n", encoding="utf-8")

    state_disc: DMTATargetState = {
        "target_gene_symbol": "EGFR",
        "evidence_records": [valid_record],
        "target_mart_path": disc_delta_dir.as_posix(),
        "rules_path": "governance/contracts/target_contract.json",
    }
    res_disc = validate_lineage_and_contract(state_disc)
    assert res_disc["triage_decision"] == "INVALID_LINEAGE"
    assert res_disc["lineage_verification"]["commits_continuous"] is False
    assert any(f.get("rule") == "DELTA_COMMIT_CONTINUITY" for f in res_disc["contract_findings"])

    # High risk / out of bounds record (p_value = 0.40)
    high_risk_record = {
        "target_gene_symbol": "EGFR",
        "disease_concept_id": 254637,
        "odds_ratio": 0.8,
        "p_value": 0.40,
        "target_tractability_score": 0.35,
        "biomarker_correlation": 0.05,
        "total_cohort_size": 100,
        "evidence_tier": "TIER_3_EXPLORATORY",
    }
    state_risk: DMTATargetState = {
        "target_gene_symbol": "EGFR",
        "evidence_records": [high_risk_record],
        "target_mart_path": delta_dir.as_posix(),
        "rules_path": "governance/contracts/target_contract.json",
    }
    res_risk = validate_lineage_and_contract(state_risk)
    assert res_risk["triage_decision"] == "HIGH_RISK_REJECTED"

    # Fatal contract breach (null disease_concept_id)
    fatal_record = {
        "target_gene_symbol": "EGFR",
        "disease_concept_id": None,
        "odds_ratio": 2.0,
        "p_value": 0.01,
        "target_tractability_score": 0.70,
        "total_cohort_size": 100,
    }
    state_fatal: DMTATargetState = {
        "target_gene_symbol": "EGFR",
        "evidence_records": [fatal_record],
        "rules_path": "governance/contracts/target_contract.json",
    }
    res_fatal = validate_lineage_and_contract(state_fatal)
    assert res_fatal["triage_decision"] == "INVALID_LINEAGE"


def test_synthesize_validation_dossier():
    """Validates 21 CFR §11.50 Electronic Signatures and cryptographic dossier sealing."""
    state: DMTATargetState = {
        "target_gene_symbol": "BRAF",
        "disease_concept_id": 254637,
        "triage_decision": "FEASIBLE",
        "feasibility_score": 85.5,
        "operator_id": "test_qa_steward",
        "hypothesis_text": "Investigate BRAF in colorectal cancer",
        "evidence_summary": {"associated_phenotypes_count": 1, "mean_odds_ratio": 3.4},
        "evidence_records": [{"target_gene_symbol": "BRAF", "odds_ratio": 3.4}],
        "contract_findings": [],
        "lineage_verification": {"provenance_intact": True},
    }

    res = synthesize_validation_dossier(state)
    dossier = res["validation_dossier"]
    sig = res["electronic_signature"]
    seal = res["dossier_receipt_sha256"]

    # Verify Electronic Signature (21 CFR §11.50)
    assert sig["operator_id"] == "test_qa_steward"
    assert sig["meaning"] == "Target feasibility triage and GxP validation dossier generation."
    assert is_valid_sha256(sig["signature_checksum"]) is True

    # Verify Cryptographic Dossier Seal
    assert is_valid_sha256(seal) is True
    assert dossier["dossier_receipt_sha256"] == seal
    assert dossier["triage_decision"] == "FEASIBLE"
    assert dossier["feasibility_score"] == 85.5
    assert dossier["gxp_contract_passed"] is True
    assert "FDA_21_CFR_Part_11" in dossier["regulatory_standards"]


def test_end_to_end_triage_target(tmp_path):
    """Validates complete DMTATargetSteward.triage_target execution across multiple scenarios."""
    steward = DMTATargetSteward()

    # Scenario 1: Highly Feasible Target
    dossier_feasible = steward.triage_target(
        hypothesis_text="Evaluate BRAF target in colorectal cancer (concept 254637)",
        evidence_records=[
            {
                "target_gene_symbol": "BRAF",
                "disease_concept_id": 254637,
                "odds_ratio": 3.8,
                "p_value": 0.0001,
                "target_tractability_score": 0.85,
                "biomarker_correlation": 0.40,
                "total_cohort_size": 500,
                "carrier_cases": 15,
                "carrier_controls": 5,
                "non_carrier_cases": 80,
                "non_carrier_controls": 400,
                "evidence_tier": "TIER_1_VALIDATED",
            }
        ],
        operator_id="qa_lead_vivi",
        thread_id="test_thread_feasible",
    )

    assert dossier_feasible["triage_decision"] == "FEASIBLE"
    assert dossier_feasible["feasibility_score"] >= 65.0
    assert dossier_feasible["gxp_contract_passed"] is True
    assert is_valid_sha256(dossier_feasible["dossier_receipt_sha256"]) is True
    assert is_valid_sha256(dossier_feasible["electronic_signature"]["signature_checksum"]) is True

    # Scenario 2: Inconclusive Target (No evidence found)
    dossier_inconclusive = steward.triage_target(
        target_gene_symbol="UNKNOWN_TARGET",
        evidence_records=[],
        thread_id="test_thread_inconclusive",
    )
    assert dossier_inconclusive["triage_decision"] == "INCONCLUSIVE"
    assert dossier_inconclusive["evidence_records_count"] == 0

    # Scenario 3: High Risk Rejected Target
    dossier_rejected = steward.triage_target(
        target_gene_symbol="KRAS",
        disease_concept_id=316866,
        evidence_records=[
            {
                "target_gene_symbol": "KRAS",
                "disease_concept_id": 316866,
                "odds_ratio": 0.45,  # Low protective / unassociated
                "p_value": 0.35,  # Non-significant
                "target_tractability_score": 0.20,
                "biomarker_correlation": -0.05,
                "total_cohort_size": 250,
                "evidence_tier": "TIER_3_EXPLORATORY",
            }
        ],
        thread_id="test_thread_rejected",
    )
    assert dossier_rejected["triage_decision"] == "HIGH_RISK_REJECTED"


def test_cli_execution(monkeypatch, tmp_path, capsys):
    """Validates DMTA steward command-line interface execution."""
    out_file = (tmp_path / "cli_dossier.json").as_posix()
    test_args = [
        "dmta_target_steward.py",
        "--gene",
        "BRAF",
        "--disease-concept-id",
        "254637",
        "--operator-id",
        "cli_tester",
        "--output",
        out_file,
    ]
    monkeypatch.setattr(sys, "argv", test_args)

    main()
    captured = capsys.readouterr()
    assert "DOSSIER-BRAF-254637" in captured.out
    assert os.path.exists(out_file)

    with open(out_file, encoding="utf-8") as f:
        saved_dossier = json.load(f)
    assert saved_dossier["target_gene_symbol"] == "BRAF"
    assert is_valid_sha256(saved_dossier["dossier_receipt_sha256"]) is True
