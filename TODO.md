# 📋 Data Engineering & Clinical Analytics Backlog

This document tracks active development phases and engineering priorities for the **Enterprise Life Sciences Data Platform Blueprint**.

---

## 🏗️ Phase 1: Medallion Storage & Infrastructure Core

- [x] **Declarative Cloud Infrastructure & S3 WORM Storage (`terraform/`)**
  - Declarative Terraform IaC infrastructure with cryptographic S3 WORM Object Locking (`COMPLIANCE` retention mode in `prod`).
  - Isolated Amazon ECS compute cluster topology and AWS Batch execution environments for episodic, containerized workflow execution.
  - Automated DataOps CI/CD linting gate (`.github/workflows/tf-lint.yml`) validating HCL syntax and Nextflow configurations.

---

## 🛡️ Phase 2: GxP Governance & Data Integrity Gates

- [x] **Data Quality Rules & Cryptographic Provenance Tracking (`governance/`)**
  - Programmatic data quality suite using Great Expectations (`governance/rules.json`) enforcing FDA 21 CFR Part 11 electronic records integrity.
  - Automated execution lineage, SHA-256 cryptographic file tracking, and metric logging via MLflow (`governance/mlflow_tracker.py`).

---

## 🧬 Phase 3: Base Clinical Normalization Ring (OMOP CDM)

- [x] **Base PySpark Semantic Mapping (`analytical-layer/omop_cdm_v54/`)**
  - PySpark semantic mapping package translating unstructured genomic and clinical fields into standard OHDSI OMOP CDM v5.4 `PERSON`, `CONDITION_OCCURRENCE`, and `MEASUREMENT` structures.

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

## ⚡ Phase 6: Production DataOps & CI/CD Pipeline Automation

- [x] **DataOps CI/CD Gate Expansion (`.github/workflows/tf-lint.yml`)**
  - Configured multi-job GitHub Actions workflow to execute Terraform IaC syntax checks, Nextflow stub evaluation, `ruff` linter/formatter, `mypy` strict static type verification, and PySpark unit/integration test suites with `pytest-cov` reporting on all feature branch pull requests.
  - Standardized toolchain configurations in `pyproject.toml` (`[tool.ruff]`, `[tool.mypy]`, `[tool.pytest.ini_options]`, `[tool.coverage]`).
- [x] **Enterprise GxP Pull Request Template (`.github/pull_request_template.md`)**
  - Established a standardized PR template requiring Conventional Commit classifications, FDA 21 CFR Part 11 / data contract compliance checklists, DataOps test verification sign-offs, and security declarations.

---

## 🤖 Phase 7: Agentic Lineage & MLOps Infrastructure

- [ ] **LangGraph Delta Lake Lineage Auditor (`agentic-ai/graph_auditor.py`)**
  - Build state graph evaluating MLflow lineage trees (`governance/mlflow_tracker.py`), Delta Lake transaction commit logs (`_delta_log/`), and schema integrity against FDA 21 CFR Part 11 parameters.
- [ ] **Model Context Protocol (MCP) Clinical Data Server (`agentic-ai/mcp_server.py`)**
  - Expose FastMCP tools for querying OMOP CDM concept hierarchies, vocabulary relationships, and pipeline execution state.

---

## 🛡️ Phase 8: Data Contract Failure & GxP Quarantine Routines

- [ ] **Dead-Letter Delta Lake Quarantine Sinks (`analytical-layer/medallion/quarantine.py`)**
  - Implement dedicated Delta Lake quarantine table sinks (`quarantine_conditions`, `quarantine_measurements`, `quarantine_patients`) to isolate non-compliant records with verbatim raw JSON payloads, failure timestamps, and MLflow run IDs.
- [ ] **Standardized Clinical Failure Taxonomy & Error Codes**
  - Implement structured clinical failure codes (`SCHEMA_VIOLATION`, `UNMAPPED_TERMINOLOGY`, `OUT_OF_BOUNDS_LAB`, `TEMPORAL_ANOMALY`, `ORPHAN_FOREIGN_KEY`) with deterministic error reason attribution.
- [ ] **Batch Quality Threshold & GxP Breach Enforcement Gate**
  - Compute batch quarantine rejection ratios ($\frac{\text{Quarantined Rows}}{\text{Total Ingestion Rows}}$); abort downstream Gold persistence and log compliance breach events in MLflow when exceeding configurable tolerance limits (e.g., $>2\%$).
- [ ] **Idempotent Quarantine Remediation & Replay Engine**
  - Develop a clinical data remediation utility allowing data stewards to re-evaluate quarantined records against updated vocabulary mappings and promote corrected rows into Silver tiers without data loss.

---

## 📊 Phase 9: Gold-Tier Analytical Cohorts & Translational Endpoints

- [ ] **Configurable OHDSI Phenotyping Engine (`analytical-layer/cohorts/builder.py`)**
  - Implement temporal inclusion/exclusion rules (index date $T_0$, baseline lookback windows, biomarker cutoffs, multi-omics variant criteria) outputting standard OHDSI `COHORT` structures (`cohort_definition_id`, `subject_id`, `cohort_start_date`, `cohort_end_date`).
- [ ] **HIPAA Safe Harbor De-Identification Transformer (`analytical-layer/cohorts/deid.py`)**
  - Build deterministic patient pseudonymization, salt-seeded date shifting ($\pm \Delta$ days preserving longitudinal event intervals), age 89+ capping, and ZIP3 masking.
- [ ] **Time-to-Event (TTE) & Survival Analysis Marts (`analytical-layer/cohorts/survival.py`)**
  - Generate Overall Survival (OS) and Time-to-Progression (TTP) analytical frames (time, event indicator, covariates) stratified by genomic biomarkers for Kaplan-Meier modeling.
- [ ] **ML-Ready Patient Feature Store Projections (`analytical-layer/cohorts/features.py`)**
  - Construct wide longitudinal feature matrices with rolling comorbidity counts, Charlson Comorbidity Index (CCI), latest biomarker observations, and variant indicator features.

---

## 🧬 Phase 10: Nextflow Multi-Omics to OMOP Workflow

- [ ] **End-to-End DSL2 Multi-Omics Pipeline (`pipelines/multi_omics_omop.nf`)**
  - Construct modular Nextflow DSL2 workflow chaining raw sequencing QC (`FASTQC`), VCF variant annotation (`BCFTOOLS`), and PySpark Medallion OMOP CDM ingestion.
- [ ] **Pinned Biocontainers & Multi-Target Execution Profiles (`pipelines/nextflow.config`)**
  - Pin immutable Docker containers for bioinformatics tools; configure execution profiles for `local_dev`, `aws_batch` (Spot compute), and cloud S3 staging.
- [ ] **GxP Provenance Manifest & MultiQC Reporting**
  - Generate cryptographic execution manifests recording input file SHA-256 hashes, tool container digests, and target Delta Lake transaction commit IDs.

---

## 📈 Phase 11: End-to-End Analytical Showcase & Demonstration

- [ ] **Interactive Clinical & Multi-Omics Showcase Notebook**
  - Provide a standalone, documented Jupyter/Databricks showcase demonstrating Bronze ingestion $\to$ Silver GxP assertion $\to$ Gold cohort extraction $\to$ LangGraph agentic lineage audit with visual survival curve plots.

---

## 🛠️ Background Utilities & Nice-to-Haves

- [ ] **Automated Changelog & SemVer Release Gate (`.github/workflows/release-changelog.yml`)** *(Nice-to-Have)*
  - Automate `CHANGELOG.md` updates based on git commit history upon pull request merge to production (`main`).
  - Calculate incremental SemVer versioning (`patch`, `minor`, `major`) and publish tagged GitHub releases for auditable GxP provenance.
- [ ] **HL7 FHIR R4 to OMOP Ingestion Connector (`analytical-layer/omop_cdm_v54/connectors.py`)** *(Nice-to-Have)*
  - Lightweight connector parsing synthetic FHIR JSON Bundles (`Patient`, `Condition`, `Observation`) into OMOP CDM Silver tables.
- [ ] **PySpark & Delta Lake Performance Benchmark Suite (`tests/benchmarks/`)** *(Nice-to-Have)*
  - Automated benchmarking harness comparing Delta Lake Liquid Clustering (`CLUSTER BY`) versus Z-Ordering and Parquet baseline on scaled synthetic cohorts (100k+ records).
- [ ] **Automated PR Management & Human-in-the-Loop (HITL) Gate (`.github/dependabot.yml` & `.github/workflows/auto-merge.yml`)** *(Nice-to-Have)*
  - Dependabot vulnerability tracking and automated dependency update PRs with mandatory Human-in-the-Loop (HITL) review gates and electronic sign-offs before merge.
