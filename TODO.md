# 📋 Data Engineering & Clinical Analytics Backlog

This document tracks active development phases and engineering priorities for the **Enterprise Life Sciences Data Platform Blueprint**.

---

## 🧪 Phase 4: Data Lakehouse & Clinical Normalization Engine (Active Sprint)

- [ ] **Modular PySpark OMOP CDM v5.4 Package Refactoring (`analytical-layer/omop_mapping.py` → `analytical-layer/omop_cdm_v54/`)**
  - Refactor monolithic mapping script into modular domain packages (`person.py`, `measurement.py`, `condition_occurrence.py`, `genomic_variants.py`).
  - Map clinical diagnosis phenotypes to SNOMED standard concept IDs (`201826`, `316866`) and LOINC laboratory codes.
  - Build PySpark transformation modules mapping genomic variant fields (VCF/FASTQ metadata) to the OMOP `EPISODE` and `MEASUREMENT` structures.
- [ ] **Delta Lake Performance & Storage Optimization (`analytical-layer/medallion/`)**
  - Implement PySpark write sinks utilizing Delta Lake Liquid Clustering (`CLUSTER BY (person_id, concept_id)`).
  - Enforce schema evolution and merge contracts (`option("mergeSchema", "true")`) for incoming unstructured variant payloads.
- [ ] **Data Contract Runtime Enforcement (`governance/rules.json` & `analytical-layer/omop_mapping.py`)**
  - Integrate Great Expectations runtime assertions directly into PySpark DataFrame write streams before Silver-to-Gold tier persistence.

---

## 🤖 Phase 5: Agentic Lineage & MLOps Infrastructure

- [ ] **LangGraph Delta Lake Lineage Auditor (`agentic-ai/graph_auditor.py`)**
  - Build state graph evaluating MLflow lineage trees (`governance/mlflow_tracker.py`), Delta Lake transaction commit logs (`_delta_log/`), and schema integrity against FDA 21 CFR Part 11 parameters.
- [ ] **Model Context Protocol (MCP) Clinical Data Server (`agentic-ai/mcp_server.py`)**
  - Expose FastMCP tools for querying OMOP CDM concept hierarchies, vocabulary relationships, and pipeline execution state.

---

## ⚡ Phase 6: Automated DataOps & Quality Engineering

- [ ] **PySpark Integration & Governance Test Suite (`tests/`)**
  - Construct `pytest` / `pytest-spark` test suite covering OMOP CDM transformation logic, Delta Lake schema enforcement, and MLflow tracking (`governance/mlflow_tracker.py`).
- [ ] **DataOps CI/CD Gate Expansion (`.github/workflows/tf-lint.yml`)**
  - Configure GitHub Actions to execute `ruff`, `mypy`, and PySpark unit tests on all feature branch pull requests.

---

## 🛠️ Background Utilities & Nice-to-Haves

- [ ] **Environment Cleanup Utilities (`scripts/clean.ps1` / `scripts/clean.sh`)**
  - Maintain utility scripts to reset local execution artifacts (`mock_data/`, `mlruns/`, `metastore_db/`, `spark-warehouse/`).
- [ ] **GitHub Projects (v2) Automated Status Tracking (`.github/workflows/projects-automation.yml`)** *(Nice-to-Have)*
  - Optional automation workflow for syncing issues, pull request status, and GitHub Projects v2 board columns upon PR open/merge.
