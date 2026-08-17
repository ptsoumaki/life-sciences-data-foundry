# 📋 Data Engineering & Clinical Analytics Backlog

This document tracks active development phases and engineering priorities for the **Enterprise Life Sciences Data Platform Blueprint**.

---

## 🧪 Phase 4: Data Lakehouse & Clinical Normalization Engine

- [x] **Modular PySpark OMOP CDM v5.4 Package Refactoring (`analytical-layer/omop_cdm_v54/`)**
  - Refactored monolithic mapping script into modular domain packages (`person.py`, `measurement.py`, `condition_occurrence.py`, `genomic_variants.py`, `connectors.py`).
  - Mapped clinical diagnosis phenotypes to SNOMED standard concept IDs (`201826`, `316866`) and LOINC laboratory codes.
  - Built PySpark transformation modules mapping genomic variant fields (VCF v4.2 metadata) to OMOP `MEASUREMENT` structures.
  - Implemented Dual Ingestion Modes (`--mode demo`, `--mode remote`, `--data_dir`).
- [x] **Dynamic Vocabulary & Concept Mapping Engine (`governance/concept_mappings.json` & `analytical-layer/omop_cdm_v54/vocabularies.py`)**
  - Externalized hardcoded ICD-10, LOINC, demographic, and ClinVar concept mappings into structured GxP JSON specification with clinical descriptions.
  - Implemented dynamic PySpark `create_map` column expression generator (`build_concept_lookup`) with automatic caching.
- [x] **Delta Lake Performance & Storage Optimization (`analytical-layer/medallion/`, `databricks.yml` & `terraform/databricks_medallion.tf`)**
  - Implemented PySpark write sinks utilizing Delta Lake Liquid Clustering (`CLUSTER BY (person_id, concept_id)`).
  - Enforced schema evolution and merge contracts (`option("mergeSchema", "true")`) for incoming unstructured variant payloads.
  - Provisioned Databricks Asset Bundles (DABs `databricks.yml`) and Terraform workspace modules (`terraform/databricks_medallion.tf`).
- [x] **Data Contract Runtime Enforcement (`governance/rules.json` & `analytical-layer/omop_cdm_v54/pipeline.py`)**
  - Integrated Great Expectations runtime assertions directly into PySpark DataFrame write streams before Silver-to-Gold tier persistence with MLflow 21 CFR Part 11 cryptographic lineage auditing.

---

## 🧪 Phase 5: Automated Testing & Quality Assurance Suite

- [x] **PySpark Unit Testing Suite (`tests/unit/`)**
  - Construct isolated `pytest` unit tests for each domain transformer (`person.py`, `condition_occurrence.py`, `measurement.py`, `genomic_variants.py`, `vocabularies.py`, `test_data_contracts.py`).
  - Validate string normalization, ICD-10 code mapping, LOINC code resolution, dynamic dictionary lookup expressions, and explicit OMOP CDM v5.4 type casting.
- [x] **End-to-End Integration Testing Suite (`tests/integration/`)**
  - Construct `pytest-spark` integration tests verifying full Medallion pipeline execution (`--mode demo` and `--mode remote`).
  - Validate Great Expectations rule enforcement and MLflow SHA-256 cryptographic lineage tracking (`governance/mlflow_tracker.py`).

---

## ⚡ Phase 6: Production DataOps & CI/CD Pipeline Automation (Active Sprint)

- [ ] **DataOps CI/CD Gate Expansion (`.github/workflows/tf-lint.yml`)**
  - Configure GitHub Actions to execute `ruff`, `mypy`, and PySpark unit/integration test suites on all feature branch pull requests.

---

## 🤖 Phase 7: Agentic Lineage & MLOps Infrastructure

- [x] **LangGraph Delta Lake Lineage Auditor (`agentic-ai/graph_auditor.py`)**
  - Built LangGraph state graph evaluating MLflow lineage trees (`governance/mlflow_tracker.py`), Delta Lake transaction commit logs (`_delta_log/`), and OMOP CDM schema integrity against FDA 21 CFR Part 11 parameters.
  - Implemented 5-node audit workflow with SHA-256 cryptographic provenance checks, commit sequence/monotonicity validation, and automated compliance scoring.
  - Added unit test suite (`tests/unit/test_graph_auditor.py`) with 100% pass rate.
- [ ] **Model Context Protocol (MCP) Clinical Data Server (`agentic-ai/mcp_server.py`)**
  - Expose FastMCP tools for querying OMOP CDM concept hierarchies, vocabulary relationships, and pipeline execution state.

---

## 🛠️ Background Utilities & Nice-to-Haves

- [ ] **Environment Cleanup Utilities (`scripts/clean.ps1` / `scripts/clean.sh`)**
  - Maintain utility scripts to reset local execution artifacts (`mock_data/`, `mlruns/`, `metastore_db/`, `spark-warehouse/`).
- [ ] **GitHub Projects (v2) Automated Status Tracking (`.github/workflows/projects-automation.yml`)** *(Nice-to-Have)*
  - Optional automation workflow for syncing issues, pull request status, and GitHub Projects v2 board columns upon PR open/merge.
