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

### 2. `mcp_server.py` — Model Context Protocol (FastMCP) Server *(Phase 7)*
* Exposes Lakehouse normalization and governance inspection tools to AI agent assistants via the open Model Context Protocol.

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
```

### Programmatic Python API

```python
import sys
import os

sys.path.insert(0, os.path.abspath("agentic-ai"))
from graph_auditor import GxPGraphAuditor

# Initialize auditor
auditor = GxPGraphAuditor()

# Audit an MLflow run
report = auditor.audit_run_lineage(
    run_id="f8b2a1...",
    delta_table_path="data/gold/person",
    rules_path="governance/rules.json",
    log_to_mlflow=True,
)

print(f"Status: {report['compliance_status']} | Score: {report['compliance_score']}/100")
print(f"Cryptographic Receipt: {report['audit_receipt_sha256']}")
```

---

## 🧪 Testing

```bash
pytest tests/unit/test_graph_auditor.py -v
```

---

## 📦 Dependencies

* `langgraph >= 0.0.20`
* `mlflow >= 2.10.0`
* `pydantic >= 2.6.0`
* `mcp >= 0.1.0`
