# 🗺️ Data Engineering & Clinical Analytics Roadmap

This document tracks active development phases and engineering priorities for the **Enterprise Life Sciences Data Platform Blueprint**.

## 📊 Platform Roadmap & Implementation Status

| Phase | Subsystem / Focus Area | Primary Domain | Status |
| :--- | :--- | :--- | :--- |
| **Phase 1** | Medallion Storage & Infrastructure Core | `terraform/` · S3 WORM | `✅ COMPLETED` |
| **Phase 2** | GxP Governance & Data Integrity Gates | `governance/` · Great Expectations · MLflow | `✅ COMPLETED` |
| **Phase 3** | Base Clinical Normalization Ring (OMOP CDM) | `analytical-layer/omop_cdm_v54/` | `✅ COMPLETED` |
| **Phase 4** | Data Lakehouse & Clinical Normalization Engine | `analytical-layer/omop_cdm_v54/` · `medallion/` | `✅ COMPLETED` |
| **Phase 5** | Automated Testing & Quality Assurance Suite | `tests/unit/` · `tests/integration/` | `✅ COMPLETED` |
| **Phase 6** | Production DataOps & CI/CD Pipeline Automation | `.github/workflows/` | `✅ COMPLETED` |
| **Phase 7** | Agentic Lineage & MLOps Infrastructure | `agentic-ai/` · FastMCP · LangGraph | `✅ COMPLETED` |
| **Phase 8** | Data Contract Failure & GxP Quarantine Routines | `analytical-layer/medallion/quarantine.py` | `✅ COMPLETED` |
| **Phase 9** | Gold-Tier Analytical Cohorts & Translational Endpoints | `analytical-layer/cohorts/` · HIPAA De-ID | `✅ COMPLETED` |
| **Phase 10** | Nextflow Multi-Omics to OMOP Workflow | `pipelines/multi_omics_omop.nf` | `📋 PLANNED` |
| **Phase 11** | Target Discovery Data Products (Discovery Lakehouse) | `analytical-layer/discovery/` · DMTA Mart | `📋 PLANNED` |
| **Phase 12** | Agentic DMTA Target Triage & AI Discovery | `agentic-ai/dmta_target_steward.py` | `📋 PLANNED` |
| **Phase 13** | GxP-Validated Feature Store & Drift Monitoring Engine | `governance/drift_monitor.py` · `cohorts/` | `📋 PLANNED` |
| **Phase 14** | End-to-End Analytical Showcase & Demonstration | Interactive Notebook & Databricks Demo | `📋 PLANNED` |

---

## 🏗️ Phase 1: Medallion Storage & Infrastructure Core `[✅ COMPLETED]`

- [x] **Declarative Cloud Infrastructure & S3 WORM Storage (`terraform/`)**
  - Declarative Terraform IaC infrastructure with cryptographic S3 WORM Object Locking (`COMPLIANCE` retention mode in `prod`).
  - Isolated Amazon ECS compute cluster topology and AWS Batch execution environments for episodic, containerized workflow execution.
  - Automated DataOps CI/CD linting gate (`.github/workflows/tf-lint.yml`) validating HCL syntax and Nextflow configurations.

---

## 🛡️ Phase 2: GxP Governance & Data Integrity Gates `[✅ COMPLETED]`

- [x] **Data Quality Rules & Cryptographic Provenance Tracking (`governance/`)**
  - Programmatic data quality suite using Great Expectations (`governance/rules.json`) enforcing FDA 21 CFR Part 11 electronic records integrity.
  - Automated execution lineage, SHA-256 cryptographic file tracking, and metric logging via MLflow (`governance/mlflow_tracker.py`).

---

## 🧬 Phase 3: Base Clinical Normalization Ring (OMOP CDM) `[✅ COMPLETED]`

- [x] **Base PySpark Semantic Mapping (`analytical-layer/omop_cdm_v54/`)**
  - PySpark semantic mapping package translating unstructured genomic and clinical fields into standard OHDSI OMOP CDM v5.4 `PERSON`, `CONDITION_OCCURRENCE`, and `MEASUREMENT` structures.

---

## 🧪 Phase 4: Data Lakehouse & Clinical Normalization Engine `[✅ COMPLETED]`

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

## 🧪 Phase 5: Automated Testing & Quality Assurance Suite `[✅ COMPLETED]`

- [x] **PySpark Unit Testing Suite (`tests/unit/`)**
  - Construct isolated `pytest` unit tests for each domain transformer (`person.py`, `condition_occurrence.py`, `measurement.py`, `genomic_variants.py`, `vocabularies.py`, `test_data_contracts.py`).
  - Validate string normalization, ICD-10 code mapping, LOINC code resolution, dynamic dictionary lookup expressions, and explicit OMOP CDM v5.4 type casting.
- [x] **End-to-End Integration Testing Suite (`tests/integration/`)**
  - Construct `pytest-spark` integration tests verifying full Medallion pipeline execution (`--mode demo` and `--mode remote`).
  - Validate Great Expectations rule enforcement and MLflow SHA-256 cryptographic lineage tracking (`governance/mlflow_tracker.py`).

---

## ⚡ Phase 6: Production DataOps & CI/CD Pipeline Automation `[✅ COMPLETED]`

- [x] **DataOps CI/CD Gate Expansion (`.github/workflows/tf-lint.yml`)**
  - Configured multi-job GitHub Actions workflow to execute Terraform IaC syntax checks, Nextflow stub evaluation, `ruff` linter/formatter, `mypy` strict static type verification, and PySpark unit/integration test suites with `pytest-cov` reporting on all feature branch pull requests.
  - Standardized toolchain configurations in `pyproject.toml` (`[tool.ruff]`, `[tool.mypy]`, `[tool.pytest.ini_options]`, `[tool.coverage]`).
- [x] **Enterprise GxP Pull Request Template (`.github/pull_request_template.md`)**
  - Established a standardized PR template requiring Conventional Commit classifications, FDA 21 CFR Part 11 / data contract compliance checklists, DataOps test verification sign-offs, and security declarations.

---

## 🤖 Phase 7: Agentic Lineage & MLOps Infrastructure `[✅ COMPLETED]`

- [x] **LangGraph Delta Lake Lineage Auditor (`agentic-ai/graph_auditor.py`)**
  - Built 6-node LangGraph state graph evaluating MLflow lineage trees (`governance/mlflow_tracker.py`), Delta Lake transaction commit logs (`_delta_log/`), and OMOP CDM schema integrity against FDA 21 CFR Part 11 parameters.
  - Implemented Human-in-the-Loop (HITL) review gates with FDA 21 CFR §11.50 Electronic Signatures and automated MLflow GxP Audit Certificate logging (`audit_receipts/gxp_audit_certificate.json`).
  - Integrated centralized cryptographic verification via [`governance/crypto.py`](governance/crypto.py).
  - Added comprehensive unit test suites (`tests/unit/test_graph_auditor.py`, `tests/unit/test_crypto.py`) with 100% pass rate.
- [x] **Model Context Protocol (MCP) Clinical Data Server (`agentic-ai/mcp_server.py`)**
  - Exposed 11 FastMCP tools for querying OMOP CDM concept hierarchies (ICD-10 to SNOMED, LOINC labs, demographics, ClinVar variants), table schema definitions, Great Expectations data contracts, Delta Lake transaction commit logs, and MLflow GxP lineage auditing.
  - Implemented dual-transport architecture supporting Standard I/O (`stdio`) for AI agent desktop clients and Server-Sent Events (`sse`) for network microservices.
  - Built comprehensive unit test suite (`tests/unit/test_mcp_server.py`) with 100% pass rate.

---

## 🛡️ Phase 8: Data Contract Failure & GxP Quarantine Routines `[✅ COMPLETED]`

- [x] **Dead-Letter Delta Lake Quarantine Sinks (`analytical-layer/medallion/quarantine.py`)**
  - Implement dedicated Delta Lake quarantine table sinks (`quarantine_conditions`, `quarantine_measurements`, `quarantine_patients`) to isolate non-compliant records with verbatim raw JSON payloads, failure timestamps, and MLflow run IDs.
- [x] **Standardized Clinical Failure Taxonomy & Error Codes**
  - Implement structured clinical failure codes (`SCHEMA_VIOLATION`, `UNMAPPED_TERMINOLOGY`, `OUT_OF_BOUNDS_LAB`, `TEMPORAL_ANOMALY`, `ORPHAN_FOREIGN_KEY`) with deterministic error reason attribution.
- [x] **Batch Quality Threshold & GxP Breach Enforcement Gate**
  - Compute batch quarantine rejection ratios ($\frac{\text{Quarantined Rows}}{\text{Total Ingestion Rows}}$); abort downstream Gold persistence and log compliance breach events in MLflow when exceeding configurable tolerance limits (e.g., $>2\%$).
- [x] **Idempotent Quarantine Remediation & Replay Engine**
  - Develop a clinical data remediation utility allowing data stewards to re-evaluate quarantined records against updated vocabulary mappings and promote corrected rows into Silver tiers without data loss.

---

## 📊 Phase 9: Gold-Tier Analytical Cohorts & Translational Endpoints `[✅ COMPLETED]`

- [x] **Configurable OHDSI Phenotyping Engine (`analytical-layer/cohorts/builder.py`)**
  - Implement temporal inclusion/exclusion rules (index date $T_0$, baseline lookback windows, biomarker cutoffs, multi-omics variant criteria) outputting standard OHDSI `COHORT` structures (`cohort_definition_id`, `subject_id`, `cohort_start_date`, `cohort_end_date`).
- [x] **HIPAA Safe Harbor De-Identification Transformer (`analytical-layer/cohorts/deid.py`)**
  - Build deterministic patient pseudonymization, salt-seeded date shifting ($\pm \Delta$ days preserving longitudinal event intervals), age 89+ capping, and ZIP3 masking.
- [x] **Time-to-Event (TTE) & Survival Analysis Marts (`analytical-layer/cohorts/survival.py`)**
  - Generate Overall Survival (OS) and Time-to-Progression (TTP) analytical frames (time, event indicator, covariates) stratified by genomic biomarkers for Kaplan-Meier modeling.
- [x] **ML-Ready Patient Feature Store Projections (`analytical-layer/cohorts/features.py`)**
  - Construct wide longitudinal feature matrices with rolling comorbidity counts, Charlson Comorbidity Index (CCI), latest biomarker observations, and variant indicator features.

---

## 🧬 Phase 10: Nextflow Multi-Omics to OMOP Workflow `[✅ COMPLETED]`

- [x] **End-to-End DSL2 Multi-Omics Pipeline (`pipelines/multi_omics_omop.nf`)**
  - Construct modular Nextflow DSL2 workflow chaining raw sequencing QC (`FASTQC`), VCF variant annotation (`BCFTOOLS`), aggregated multi-tool quality report generation (`MULTIQC`), and PySpark Medallion OMOP CDM ingestion.
- [x] **Pinned Biocontainers & Multi-Target Execution Profiles (`pipelines/nextflow.config`)**
  - Pin immutable Docker containers for bioinformatics tools (`fastqc:0.12.1--hdfd78af_0`, `bcftools:1.19--h8b25389_1`, `multiqc:1.21--pyhdfd78af_0`); configure execution profiles for `local_dev`, `aws_batch` (Spot compute), and automated testing (`test`).
- [x] **GxP Provenance Manifest & MultiQC Reporting (`pipelines/provenance.py` & `pipelines/modules/provenance.nf`)**
  - Generate cryptographic execution manifests recording input file SHA-256 hashes, tool container digests, and target Delta Lake transaction commit IDs adhering to FDA 21 CFR Part 11.

---

## 🎯 Phase 11: Target Discovery Data Products (Discovery Lakehouse) `[✅ COMPLETED]`

- [x] **Target-to-Phenotype Evidence Mart (`analytical-layer/discovery/target_mart.py`)**
  - PySpark aggregation layer joining OMOP `MEASUREMENT` (ClinVar variant calls) and `CONDITION_OCCURRENCE` across longitudinal cohorts.
  - Calculate target tractability metrics: target mutation burden, biomarker correlation matrices, and phenotypic odds ratios across disease hierarchies.
  - Delta Lake Liquid Clustering persistence: `CLUSTER BY (target_gene_symbol, disease_concept_id)`.
- [x] **Declarative DMTA Data Product Contract (`governance/contracts/target_contract.json`)**
  - Great Expectations GxP contract enforcing semantic invariants on target entities: HGNC canonical symbol validation, permissible odds-ratio bounds, and target tractability score completeness.
  - Integration with `analytical-layer/medallion/quarantine.py` to route contract breaches to dead-letter sinks with failure code `TARGET_CONTRACT_VIOLATION`.
- [x] **Unit & Contract Verification Suites (`tests/unit/test_target_mart.py`)**
  - Unit tests verifying tractability aggregations, Odds Ratio calculations, and contract rejection dead-letter routing.

---

## 🤖 Phase 12: Agentic DMTA Target Triage & AI Discovery `[📋 PLANNED]`

- [ ] **Agentic DMTA Target Triage Graph (`agentic-ai/dmta_target_steward.py`)**
  - 4-node LangGraph state machine (`ParseHypothesis` -> `QueryTargetMart` -> `ValidateLineageAndContract` -> `SynthesizeValidationDossier`) executing autonomous target feasibility checks.
  - Integrate with `governance/crypto.py` to seal target dossier outputs with 21 CFR §11.50 cryptographic signatures and Delta commit SHAs.
- [ ] **FastMCP Discovery Tool Extensions (`agentic-ai/mcp_server.py`)**
  - Expose discovery-specific endpoints: `get_target_biomarker_profile(gene_symbol)` and `verify_target_lineage(dataset_version)` via FastMCP for programmatic agent tool use.
- [ ] **Agentic DMTA Unit & Integration Tests (`tests/unit/test_dmta_steward.py`)**
  - Unit tests verifying LangGraph multi-node execution, state transitions, and electronic signature generation.

---

## 🔍 Phase 13: GxP-Validated Feature Store & Drift Monitoring Engine `[📋 PLANNED]`

- [ ] **Point-in-Time Correctness & Feature Store Immutability (`analytical-layer/cohorts/point_in_time.py`)**
  - Implement distributed `as_of_join` engine enforcing zero future data leakage across longitudinal clinical events and multi-omics observations:
    $$T_{\text{event}} \in [T_{0, i} - \Delta_{\text{lookback}}, \, T_{0, i}]$$
  - Isolate and suppress post-index observation contamination from downstream ML feature matrices.
  - Generate cryptographic feature set manifests capturing upstream Delta Lake commit versions (`table.history()`), parameter metadata, and SHA-256 dataset hashes via `governance/crypto.py`.
- [ ] **Distributed Population & Covariate Drift Engine (`governance/drift_monitor.py`)**
  - Scalable PySpark calculation of Population Stability Index (PSI) over quantized deciles for continuous biomarkers (HbA1c, LDL-C, eGFR):
    $$\text{PSI} = \sum_{k=1}^{K} \left( P_k - Q_k \right) \ln\left(\frac{P_k}{Q_k}\right)$$
  - Implement two-sample Kolmogorov-Smirnov (KS) tests and Wasserstein Distance for continuous lab distribution shifts.
  - Compute Chi-Square goodness-of-fit and Total Variation Distance across high-cardinality categorical SNOMED condition codes and ClinVar variant indicator distributions.
- [ ] **GxP Telemetry Gate & MLflow Automated Pipeline Halting (`governance/mlflow_tracker.py`)**
  - Automatically stream drift metrics and statistical significance indicators to active MLflow experiment runs adhering to FDA Good Machine Learning Practice (GMLP).
  - Implement automated execution barrier: emit GxP warning tag for moderate drift ($0.10 \le \text{PSI} \le 0.25$); throw `GxPDriftBreachError`, halt downstream execution, and route batch manifests to `analytical-layer/medallion/quarantine.py` with failure code `COVARIATE_DRIFT_BREACH` when severe drift occurs ($\text{PSI} > 0.25$).
- [ ] **FastMCP MLOps Tool Extensions (`agentic-ai/mcp_server.py`)**
  - Expose MLOps governance endpoints: `get_feature_drift_report(cohort_definition_id, target_metric)` and `verify_feature_matrix_lineage(matrix_delta_version)` via FastMCP for agentic quality inspection.
- [ ] **Unit & Gating Verification Suites (`tests/unit/test_drift_monitor.py`, `tests/unit/test_point_in_time.py`)**
  - Unit tests verifying temporal isolation (assert zero record contamination for $T_{\text{event}} > T_0$), analytical PSI calculation precision ($\pm 10^{-4}$), and automated pipeline abort triggers on simulated out-of-distribution batches.

---

## 📈 Phase 14: End-to-End Analytical Showcase & Demonstration `[📋 PLANNED]`

- [ ] **Interactive Clinical & Multi-Omics Showcase Notebook**
  - Provide a standalone, documented Jupyter/Databricks showcase demonstrating Bronze ingestion $\to$ Silver GxP assertion $\to$ Gold cohort extraction $\to$ LangGraph agentic lineage audit with visual survival curve plots.

---

## 🛠️ Background Utilities & Nice-to-Haves

- [ ] **HL7 FHIR R4 to OMOP Ingestion Connector (`analytical-layer/omop_cdm_v54/connectors.py`)** *(Nice-to-Have)*
  - Lightweight connector parsing synthetic FHIR JSON Bundles (`Patient`, `Condition`, `Observation`) into OMOP CDM Silver tables.
- [ ] **Automated PR Management & Human-in-the-Loop (HITL) Gate (`.github/dependabot.yml` & `.github/workflows/auto-merge.yml`)** *(Nice-to-Have)*
  - Dependabot vulnerability tracking and automated dependency update PRs with mandatory Human-in-the-Loop (HITL) review gates and electronic sign-offs before merge.
