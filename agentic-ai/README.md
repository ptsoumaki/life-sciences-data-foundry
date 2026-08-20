# Agentic Compliance Auditing Tier 🤖

This component implements the **Agentic Compliance Auditing Tier** — an autonomous multi-agent system using LangGraph state graphs and Model Context Protocol (FastMCP) to evaluate platform configurations, Delta Lake transaction commit logs, and MLflow execution lineage against **FDA 21 CFR Part 11** and GxP regulatory parameters.

> **Status:** Production-ready LangGraph GxP state auditor with Human-in-the-Loop (HITL) electronic signatures, Delta Lake commit log analysis, and automated MLflow audit certificate generation.

---

## 🏛️ Architecture & State Graph Topology

```text
                                  [ START ]
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │  collect_mlflow_evidence  │
                        └─────────────┬─────────────┘
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │ collect_delta_log_evidence│
                        └─────────────┬─────────────┘
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │evaluate_cfr_part_11_comp  │
                        └─────────────┬─────────────┘
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │ evaluate_schema_integrity │
                        └─────────────┬─────────────┘
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │  generate_audit_findings  │
                        └─────────────┬─────────────┘
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │      human_qa_review      │◄─── [ 21 CFR §11.50 Electronic Signature ]
                        └─────────────┬─────────────┘
                                      │
                                      ▼
                                   [ END ]
```

---

## 📦 Core Components

### 1. `graph_auditor.py` — LangGraph GxP Compliance State Auditor
* **6-Node State Graph**: Sequentially collects MLflow provenance, parses Delta Lake transaction logs, evaluates 21 CFR Part 11 parameters, checks OMOP CDM schema conformance, computes weighted compliance scores, and triggers HITL review when deviations occur.
* **FDA 21 CFR §11.10(e) Provenance**: Validates SHA-256 cryptographic hashes for datasets and data contract rules using [`governance.crypto`](../governance/crypto.py).
* **Delta Lake Log Analysis**: Directly inspects `_delta_log/*.json` commit files to verify uninterrupted commit sequence continuity (`DLT_SEQ_001`), monotonic timestamps (`DLT_TIME_002`), and schema metadata.
* **Human-in-the-Loop (HITL) Review**: Pauses execution via LangGraph `interrupt()` on non-compliant runs (`FLAGGED_FOR_REVIEW`, `NON_COMPLIANT`) and resumes with formal QA electronic signatures (`21 CFR §11.50`).
* **Automated MLflow GxP Audit Certificates**: Automatically attaches `audit_receipts/gxp_audit_certificate.json` and records regulatory tags (`gxp_audit_status`, `gxp_audit_receipt_sha256`) directly into the audited MLflow run.

### 2. `mcp_server.py` — Model Context Protocol (FastMCP) Clinical Data Server
* **11 Core FastMCP Tools**: Exposes Lakehouse clinical normalization, OMOP CDM v5.4 vocabularies, Delta Lake commit logs, Great Expectations data contracts, and GxP 21 CFR Part 11 lineage tools to AI assistants (e.g. Claude Desktop, Antigravity IDE, Cursor).
  * `tool_lookup_icd10_to_snomed`: Maps ICD-10-CM codes (e.g. `E11.9`, `I10`) to standard SNOMED CT concept IDs.
  * `tool_lookup_loinc_concept`: Maps LOINC lab codes (e.g. `4548-4`, `2345-7`) to standard OMOP Measurement concepts.
  * `tool_lookup_demographic_concept`: Resolves gender, race, and ethnicity terms to standard OMOP concept IDs.
  * `tool_lookup_genomic_variant_concept`: Resolves ClinVar clinical significance terms (`Pathogenic`, `Benign`) to OMOP concepts.
  * `tool_query_vocabulary_mappings`: Queries dynamic concept lookup dictionaries across all domains.
  * `tool_inspect_omop_table_schema`: Returns official OMOP CDM v5.4 table schemas, column types, nullability, and primary keys (`PERSON`, `CONDITION_OCCURRENCE`, `MEASUREMENT`, `COHORT`).
  * `tool_get_pipeline_execution_state`: Queries MLflow for execution metrics, expectation pass rates, and GxP gate status.
  * `tool_inspect_delta_table_log`: Inspects Delta Lake `_delta_log/*.json` transaction commits, schema evolution, and timestamps.
  * `tool_verify_gxp_audit_lineage`: Runs the LangGraph `GxPGraphAuditor` state graph to evaluate 21 CFR Part 11 compliance.
  * `tool_inspect_data_contract`: Returns Great Expectations rules, expectation suites, and severity levels.
  * `tool_validate_clinical_record`: Performs in-memory validation of clinical records against contract rules.
* **Dual Transports**: Supports both standard input/output pipe transport (`stdio`) for local agent CLI workflows and Server-Sent Events (`sse`) for microservice integration.

---

## 🚀 Usage & CLI Execution

### Command Line Interface

```bash
# 1. Audit an MLflow pipeline run and attach GxP certificate to run artifacts
python agentic-ai/graph_auditor.py --run-id <MLFLOW_RUN_ID> --tracking-uri sqlite:///mlflow.db

# 2. Audit a physical Delta Lake table directly without an MLflow run
python agentic-ai/graph_auditor.py --delta-path /path/to/gold_person --rules governance/rules.json

# 3. Audit with Human-in-the-Loop review enabled and export JSON report
python agentic-ai/graph_auditor.py --run-id <RUN_ID> --enable-hitl --output reports/gxp_audit.json

# 4. List all registered FastMCP clinical and governance tools
python agentic-ai/mcp_server.py --list-tools

# 5. Start FastMCP Server over Standard I/O (default for LLM agent desktop clients)
python agentic-ai/mcp_server.py --transport stdio

# 6. Start FastMCP Server over Server-Sent Events (SSE) / HTTP
python agentic-ai/mcp_server.py --transport sse --host 0.0.0.0 --port 8080
```

### Programmatic Python API

```python
import sys
import os
import asyncio

sys.path.insert(0, os.path.abspath("agentic-ai"))
from graph_auditor import GxPGraphAuditor
from mcp_server import FoundryMCPServer, tool_lookup_icd10_to_snomed

# 1. GxP State Graph Auditor
auditor = GxPGraphAuditor()
report = auditor.audit_run_lineage(
    run_id="f8b2a1...",
    delta_table_path="data/gold/person",
    rules_path="governance/rules.json",
    log_to_mlflow=True,
)
print(f"Status: {report['compliance_status']} | Score: {report['compliance_score']}/100")

# 2. FastMCP Server & Clinical Tools
server = FoundryMCPServer()
res = tool_lookup_icd10_to_snomed("E11.9")
print(f"ICD-10 E11.9 -> SNOMED Concept: {res['standard_concept_id']} ({res['description']})")
```

---

## 🧪 Testing

```bash
# Run GxP Graph Auditor and MCP Server unit test suites
pytest tests/unit/test_graph_auditor.py tests/unit/test_mcp_server.py -v
```

---

## 📦 Dependencies

* `langgraph >= 0.0.20`
* `mlflow >= 2.10.0`
* `pydantic >= 2.6.0`
* `mcp >= 0.1.0`
