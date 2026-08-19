"""
Module: test_mcp_server.py
Description: Unit test suite for Model Context Protocol (FastMCP) Clinical Data Server (Phase 7).
Author: Vivi Tsoumaki
"""

import asyncio
import json
import os
import sys

import mlflow

# Add repository root and agentic-ai directory to path
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
agentic_dir = os.path.join(base_dir, "agentic-ai")
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)
if agentic_dir not in sys.path:
    sys.path.insert(0, agentic_dir)

from mcp_server import (  # noqa: E402
    FoundryMCPServer,
    tool_get_pipeline_execution_state,
    tool_inspect_data_contract,
    tool_inspect_delta_table_log,
    tool_inspect_omop_table_schema,
    tool_lookup_demographic_concept,
    tool_lookup_genomic_variant_concept,
    tool_lookup_icd10_to_snomed,
    tool_lookup_loinc_concept,
    tool_query_vocabulary_mappings,
    tool_validate_clinical_record,
    tool_verify_gxp_audit_lineage,
)


def test_mcp_server_initialization_and_tool_list():
    """Validates that FoundryMCPServer initializes and registers all 11 core tools."""
    server = FoundryMCPServer(name="test-mcp-server")
    assert server.name == "test-mcp-server"
    assert server.server is not None

    async def _check():
        tools = await server.list_tools()
        tool_names = [t.name for t in tools]
        assert len(tool_names) == 11

        expected_tools = [
            "tool_lookup_icd10_to_snomed",
            "tool_lookup_loinc_concept",
            "tool_lookup_demographic_concept",
            "tool_lookup_genomic_variant_concept",
            "tool_query_vocabulary_mappings",
            "tool_inspect_omop_table_schema",
            "tool_get_pipeline_execution_state",
            "tool_inspect_delta_table_log",
            "tool_verify_gxp_audit_lineage",
            "tool_inspect_data_contract",
            "tool_validate_clinical_record",
        ]
        for exp in expected_tools:
            assert exp in tool_names

    asyncio.run(_check())


def test_tool_lookup_icd10_to_snomed_mapped_and_unmapped():
    """Validates ICD-10 to SNOMED CT concept resolution for valid and unmapped codes."""
    res_e119 = tool_lookup_icd10_to_snomed("E11.9")
    assert res_e119["standard_concept_id"] == 201826
    assert res_e119["found"] is True
    assert res_e119["target_table"] == "CONDITION_OCCURRENCE"
    assert res_e119["target_field"] == "condition_concept_id"

    # Dot-less code
    res_e119_nodot = tool_lookup_icd10_to_snomed("E119")
    assert res_e119_nodot["standard_concept_id"] == 201826
    assert res_e119_nodot["found"] is True

    # Hypertension
    res_i10 = tool_lookup_icd10_to_snomed("I10")
    assert res_i10["standard_concept_id"] == 316866
    assert res_i10["found"] is True

    # Lung neoplasm
    res_c34 = tool_lookup_icd10_to_snomed("C34.90")
    assert res_c34["standard_concept_id"] == 254637
    assert res_c34["found"] is True

    # Unmapped code
    res_unmapped = tool_lookup_icd10_to_snomed("Z99.999")
    assert res_unmapped["standard_concept_id"] == 0
    assert res_unmapped["found"] is False


def test_tool_lookup_loinc_concept_mapped_and_unmapped():
    """Validates LOINC to OMOP measurement concept mapping."""
    # HbA1c
    res_hba1c = tool_lookup_loinc_concept("4548-4")
    assert res_hba1c["standard_concept_id"] == 3004410
    assert res_hba1c["found"] is True
    assert res_hba1c["target_table"] == "MEASUREMENT"
    assert res_hba1c["target_field"] == "measurement_concept_id"

    # Glucose
    res_glucose = tool_lookup_loinc_concept("2345-7")
    assert res_glucose["standard_concept_id"] == 3000483
    assert res_glucose["found"] is True

    # Unmapped LOINC
    res_unmapped = tool_lookup_loinc_concept("99999-9")
    assert res_unmapped["standard_concept_id"] == 0
    assert res_unmapped["found"] is False


def test_tool_lookup_demographic_concept():
    """Validates demographic concepts for gender, race, and ethnicity."""
    # Gender
    res_male = tool_lookup_demographic_concept(domain="gender", value="MALE")
    assert res_male["standard_concept_id"] == 8507
    assert res_male["found"] is True
    assert res_male["target_field"] == "gender_concept_id"

    res_female = tool_lookup_demographic_concept(domain="gender", value="FEMALE")
    assert res_female["standard_concept_id"] == 8532
    assert res_female["found"] is True

    # Race
    res_white = tool_lookup_demographic_concept(domain="race", value="WHITE")
    assert res_white["standard_concept_id"] == 8527
    assert res_white["found"] is True

    # Ethnicity
    res_hisp = tool_lookup_demographic_concept(domain="ethnicity", value="HISPANIC")
    assert res_hisp["standard_concept_id"] == 38003563
    assert res_hisp["found"] is True

    # Invalid domain
    res_invalid_domain = tool_lookup_demographic_concept(domain="blood_type", value="O_POS")
    assert res_invalid_domain["found"] is False
    assert "error" in res_invalid_domain


def test_tool_lookup_genomic_variant_concept():
    """Validates ClinVar variant clinical significance concept resolution."""
    res_path = tool_lookup_genomic_variant_concept("Pathogenic")
    assert res_path["standard_concept_id"] == 4181412
    assert res_path["found"] is True
    assert res_path["target_field"] == "value_as_concept_id"

    res_benign = tool_lookup_genomic_variant_concept("Benign")
    assert res_benign["standard_concept_id"] == 4049393
    assert res_benign["found"] is True

    res_unknown = tool_lookup_genomic_variant_concept("Non_Standard_Significance")
    assert res_unknown["standard_concept_id"] == 0
    assert res_unknown["found"] is False


def test_tool_query_vocabulary_mappings():
    """Validates querying vocabulary categories and full mapping dictionary."""
    all_maps = tool_query_vocabulary_mappings()
    assert "icd10_to_snomed" in all_maps
    assert "loinc_to_concept" in all_maps
    assert "gender_to_concept" in all_maps

    icd_only = tool_query_vocabulary_mappings(category="icd10_to_snomed")
    assert "icd10_to_snomed" in icd_only
    assert len(icd_only) == 1

    invalid_cat = tool_query_vocabulary_mappings(category="non_existent_category")
    assert "error" in invalid_cat


def test_tool_inspect_omop_table_schema():
    """Validates retrieving official OMOP CDM v5.4 table schemas."""
    for tbl in ["person", "condition_occurrence", "measurement", "cohort"]:
        schema = tool_inspect_omop_table_schema(tbl)
        assert schema["table_name"] == tbl.upper()
        assert len(schema["columns"]) > 0
        assert "primary_key" in schema

    invalid_tbl = tool_inspect_omop_table_schema("unknown_table")
    assert "error" in invalid_tbl
    assert "available_tables" in invalid_tbl


def test_tool_get_pipeline_execution_state(tmp_path):
    """Validates querying MLflow pipeline execution metrics and run state."""
    db_path = (tmp_path / "mcp_mlflow.db").as_posix()
    mlflow_uri = f"sqlite:///{db_path}"
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("mcp_test_experiment")

    with mlflow.start_run(run_name="mcp_run") as run:
        run_id = run.info.run_id
        mlflow.log_param("data_input_path", "test_data.parquet")
        mlflow.log_param("compliance_standard", "FDA_21_CFR_Part_11")
        mlflow.log_metric("total_records_ingested", 500)
        mlflow.log_metric("expectation_success_rate", 100.0)
        mlflow.log_metric("gxp_gate_passed", 1.0)

    # Query specific run ID
    state = tool_get_pipeline_execution_state(run_id=run_id, tracking_uri=mlflow_uri)
    assert state["run_id"] == run_id
    assert state["status"] in ["RUNNING", "FINISHED"]
    assert state["gxp_gate_passed"] is True
    assert state["expectation_success_rate"] == 100.0
    assert state["params"]["compliance_standard"] == "FDA_21_CFR_Part_11"

    # Query latest run in experiment
    latest_state = tool_get_pipeline_execution_state(
        experiment_name="mcp_test_experiment", tracking_uri=mlflow_uri
    )
    assert latest_state["run_id"] == run_id


def test_tool_inspect_delta_table_log(tmp_path):
    """Validates Delta Lake transaction commit log inspection."""
    delta_dir = tmp_path / "delta_test_table"
    delta_log = delta_dir / "_delta_log"
    delta_log.mkdir(parents=True)

    commit_0 = [
        {"commitInfo": {"timestamp": 1700000000000, "operation": "CREATE TABLE"}},
        {
            "metaData": {
                "id": "t1",
                "schemaString": json.dumps(
                    {
                        "type": "struct",
                        "fields": [{"name": "person_id", "type": "long", "nullable": False}],
                    }
                ),
                "partitionColumns": ["person_id"],
            }
        },
        {"add": {"path": "file1.parquet", "size": 1024}},
    ]

    with open(delta_log / "00000000000000000000.json", "w", encoding="utf-8") as f:
        f.writelines(json.dumps(e) + "\n" for e in commit_0)

    res = tool_inspect_delta_table_log(str(delta_dir))
    assert res["status"] == "SUCCESS"
    assert res["total_commit_count"] == 1
    assert len(res["commits"]) == 1
    assert res["commits"][0]["operation"] == "CREATE TABLE"
    assert res["commits"][0]["added_files_count"] == 1

    # Non-existent table
    err_res = tool_inspect_delta_table_log(str(tmp_path / "non_existent"))
    assert err_res["status"] == "ERROR"


def test_tool_verify_gxp_audit_lineage(tmp_path):
    """Validates calling LangGraph GxP auditor via MCP tool wrapper."""
    db_path = (tmp_path / "mcp_audit.db").as_posix()
    mlflow_uri = f"sqlite:///{db_path}"
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("mcp_audit_exp")

    with mlflow.start_run(run_name="audit_test_run") as run:
        run_id = run.info.run_id
        mlflow.log_param("data_input_path", "data.csv")
        mlflow.log_param("data_sha256", "a" * 64)
        mlflow.log_param("rules_sha256", "b" * 64)
        mlflow.log_metric("gxp_gate_passed", 1.0)
        mlflow.log_metric("total_records_ingested", 100)

    audit_res = tool_verify_gxp_audit_lineage(
        run_id=run_id,
        tracking_uri=mlflow_uri,
        rules_path="governance/rules.json",
    )
    assert "compliance_status" in audit_res
    assert audit_res["compliance_status"] in ["COMPLIANT", "FLAGGED_FOR_REVIEW", "NON_COMPLIANT"]
    assert "compliance_score" in audit_res


def test_tool_inspect_data_contract():
    """Validates inspecting Great Expectations rules contract JSON."""
    contract = tool_inspect_data_contract("governance/rules.json")
    assert contract["data_asset_type"] == "clinical_omop_person_ingest"
    assert contract["expectation_suite_name"] == "gxp_clinical_integrity_checks"
    assert contract["expectations_count"] >= 4
    assert len(contract["rules_sha256"]) == 64

    # Non-existent file
    err_contract = tool_inspect_data_contract("governance/non_existent_rules.json")
    assert "error" in err_contract


def test_tool_validate_clinical_record():
    """Validates in-memory clinical record validation against contract rules."""
    valid_record = {
        "person_id": 1001,
        "gender_concept_id": 8507,
        "year_of_birth": 1985,
        "birth_datetime": "1985-06-15T08:30:00Z",
        "race_concept_id": 8527,
        "ethnicity_concept_id": 38003564,
    }
    val_res = tool_validate_clinical_record(valid_record, "governance/rules.json")
    assert val_res["success"] is True
    assert val_res["violations_count"] == 0

    # Invalid record (missing person_id, invalid gender, bad date regex)
    invalid_record = {
        "person_id": None,
        "gender_concept_id": 999999,
        "birth_datetime": "invalid-date",
    }
    invalid_res = tool_validate_clinical_record(invalid_record, "governance/rules.json")
    assert invalid_res["success"] is False
    assert invalid_res["violations_count"] >= 2


def test_call_tool_via_mcp_server():
    """Validates end-to-end async MCP tool invocation via FoundryMCPServer.call_tool()."""
    server = FoundryMCPServer()

    async def _test():
        res = await server.call_tool("tool_lookup_icd10_to_snomed", {"icd10_code": "E11.9"})
        assert res is not None
        assert res.is_error is False

        res_loinc = await server.call_tool("tool_lookup_loinc_concept", {"loinc_code": "4548-4"})
        assert res_loinc is not None
        assert res_loinc.is_error is False

    asyncio.run(_test())
