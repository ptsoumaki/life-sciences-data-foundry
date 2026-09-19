# Changelog

All notable changes to the Life Sciences Data Foundry project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Repository Agent Architecture & Guidelines (`AGENTS.md`)**:
  - Established centralized agent guidelines covering system architecture, OHDSI OMOP CDM v5.4 standards, FDA 21 CFR Part 11 / GxP constraints, zero data loss provability, and PySpark conventions.
- **GxP Workspace Skills (`.agents/skills/`)**:
  - Implemented 7 modular workspace agent skills: `omop-cdm-normalizer`, `gxp-quarantine-remediation`, `cohort-phenotyping-survival`, `gxp-compliance-auditor`, `databricks-bundle-ops`, `gxp-git-commit`, and `pr-creator`.
- **Connector Unit Test Suite (`tests/unit/test_connectors.py`)**:
  - Added unit test suite covering Synthea clinical diagnoses, demographics, lab measurements, and VCF parsing with schema column normalization.
- **Delta Writer Unit Test Suite (`tests/unit/test_writer.py`)**:
  - Added unit test suite covering `DeltaMedallionWriter` write operations, schema evolution options, and exception handling.
- **Engineering Roadmap (`ROADMAP.md`)**:
  - Renamed `TODO.md` to `ROADMAP.md` and added Phase 11 for GxP-Validated Feature Store & Drift Monitoring Engine.

### Fixed
- **Surrogate Key Derivation**: Expanded `xxhash64` collision space to signed 64-bit (`xxhash64(...).cast("long")`) across all OMOP domain transformers (`person.py`, `condition_occurrence.py`, `measurement.py`, `genomic_variants.py`) and analytical cohort builders without `abs()`.
- **Concept Lookup Performance**: Added module-level in-memory caching and synchronization verification to `build_concept_lookup()` in `vocabularies.py`.
- **Genomic Quarantine Tracking**: Integrated genomic variant quarantine tracking and metrics logging in `pipeline.py`.
- **PySpark Eager Action Elimination**: Replaced eager `df.rdd.isEmpty()` checks with non-eager single-partition checks (`df.limit(1).count() == 0`) across `deid.py` and `features.py`.
- **Synthea Code Resolution**: Added graceful column resolution supporting both `code` and `icd10_code` in `connectors.py`.
- **Exception Narrowing**: Replaced bare exceptions with specific `AnalysisException`, `Py4JJavaError`, and `OSError` in `writer.py`, and `MlflowException` in `mlflow_tracker.py`.
- **DABs Environment Isolation**: Updated `databricks.yml` target root paths to include target environment subdirectories (`/Workspace/Projects/life-sciences-data-foundry/${bundle.target}`).
- **Configuration Hardening**: Added `agentic-ai` to `packages` discovery in `pyproject.toml` and documented `LSDF_DEID_SALT` in `.env.example`.

## [0.3.0] - 2026-09-15

### Added
- **Configurable OHDSI Phenotyping Engine (`analytical-layer/cohorts/builder.py`)**:
  - `OHDSICohortBuilder` executing declarative `CohortCriteria` rules (index event selection `FIRST`/`LAST`, continuous prior observation lookback windows, age and gender demographic filters, clinical exclusion conditions, baseline biomarker cutoffs, multi-omics ClinVar pathogenic variant criteria) against Gold-tier OMOP CDM v5.4 DataFrames.
  - Standard OHDSI `COHORT` table output schema (`cohort_definition_id`, `subject_id`, `cohort_start_date`, `cohort_end_date`) with Delta Lake Liquid Clustering persistence.
  - Three reference phenotype factory functions: `get_type_2_diabetes_cohort_definition()`, `get_hypertension_cohort_definition()`, `get_genomic_oncology_cohort_definition()`.
- **HIPAA Safe Harbor De-Identification Transformer (`analytical-layer/cohorts/deid.py`)**:
  - Deterministic keyed pseudonymization (`xxhash64`) with configurable salt via `LSDF_DEID_SALT` environment variable.
  - Patient-specific date shifting (±Δ days) preserving exact longitudinal event intervals and survival durations.
  - Age 89+ capping (HIPAA §164.514(b)(2)) and ZIP3 geographic masking with restricted prefix enforcement.
- **Time-to-Event (TTE) & Survival Analysis Marts (`analytical-layer/cohorts/survival.py`)**:
  - `SurvivalMartBuilder` producing individual-level TTE frames for Overall Survival (OS), Time-to-Progression (TTP), and Event-Free Survival (EFS) endpoints with administrative and observational right-censoring.
  - Distributed non-parametric Kaplan-Meier product-limit estimator (`compute_kaplan_meier_summary`) with Greenwood standard errors, stratified by genomic biomarker status.
- **ML-Ready Patient Feature Store (`analytical-layer/cohorts/features.py`)**:
  - `PatientFeatureStore` generating wide, numerically-encoded feature matrices with weighted Charlson Comorbidity Index (CCI) with hierarchical suppression rules, multi-window rolling condition counts, longitudinal baseline biomarker aggregations (latest, mean, min, max, missingness indicators), and ClinVar pathogenic variant embeddings.
- **Unit Test Suite (`tests/unit/test_cohort_builder.py`, `test_deid.py`, `test_survival.py`, `test_features.py`)**:
  - 25 unit tests verifying phenotyping criteria, exclusion logic, biomarker cutoffs, genomic carrier filtering, HIPAA pseudonymization determinism, date-shift interval preservation, KM product-limit estimation, Greenwood SE, CCI hierarchical suppression, rolling lookback windows, and biomarker missingness imputation.

### Fixed
- **`builder.py` (B1)**: Empty `df_cond`/`df_measurement` fallback DataFrames now use domain-correct minimal schemas (`condition_concept_id`, `measurement_concept_id`, `value_*`) instead of `df_person.schema`, preventing `AnalysisException` on downstream column references.
- **`deid.py` (B3)**: `HIPAADeIdentifier.__init__` now emits `warnings.warn` when falling back to the insecure demo salt (`LSDF_DEID_SALT` unset), surfacing a GxP/HIPAA compliance notice at instantiation.
- **`deid.py` (B4)**: Replaced `df.count() == 0` empty-check guards with `df.rdd.isEmpty()` in all three public methods, eliminating three unnecessary full Spark jobs per call.
- **`deid.py` (B5)**: `birth_datetime` is now nulled with `lit(None).cast(original_dataType)`, preserving the column's declared type and Delta Lake merge schema contracts.
- **`survival.py` (B6)**: EFS `first_event_date` corrected from `least(coalesce(prog, death), coalesce(death, prog))` to `coalesce(least(prog, death), prog, death)`, correctly capturing a single non-null date when one operand is null (`least(x, null) = null` in Spark SQL).
- **`features.py` (B8)**: Rolling condition count aggregation now driven by `config.lookback_windows_days` instead of hardcoded `30/180/365` constants, ensuring schema consistency between the live and empty-cohort paths.
- **`features.py` (B9)**: `has_pathogenic_variant` aggregation expression corrected from `lit(1)` (non-aggregate) to `spark_max(lit(1))` (valid aggregate expression across all Spark versions).

### Changed
- **`builder.py`**: `save_cohort` exception handler migrated from bare `print()` to `logging.getLogger(__name__).warning()`, consistent with the platform logging standard.
- **`builder.py`**: `df_condition_occurrence` parameter documented as a legacy alias for `df_condition`.
- **`survival.py`**: Added `__init__` docstring to `SurvivalMartBuilder`; added inline comment clarifying lazy Column evaluation semantics of `is_study_end_expr`.
- **`features.py`**: Added `Args`/`Returns` docstring sections to `_attach_charlson_and_condition_counts`, `_attach_biomarker_features`, and `_attach_genomic_features`.

---

## [0.2.10] - 2026-09-13

### Added
- **Dead-Letter Quarantine Sinks & Forensic Audit Taxonomy (`analytical-layer/medallion/writer.py`)**:
  - Implemented dedicated Delta Lake dead-letter quarantine sinks (`quarantine_patients`, `quarantine_conditions`, `quarantine_measurements`) isolating invalid records from Silver analytical tables.
  - Implemented `format_quarantine_dataframe()` standardizing quarantine schemas with `quarantine_id` (UUID), `raw_payload` (verbatim raw JSON preserving original untransformed payloads for ALCOA+ forensic auditability), `failure_code`, `failure_reason`, `quarantine_timestamp` (UTC), `mlflow_run_id`, and `status` (`QUARANTINED` / `REMEDIATED`).
  - Standardized `ClinicalFailureCode` enum establishing an audit taxonomy across 5 failure categories: `SCHEMA_VIOLATION`, `UNMAPPED_TERMINOLOGY`, `OUT_OF_BOUNDS_LAB`, `TEMPORAL_ANOMALY`, and `ORPHAN_FOREIGN_KEY`.
- **GxP Batch Quality Threshold Gate (`analytical-layer/omop_cdm_v54/pipeline.py`)**:
  - Integrated `evaluate_batch_quarantine_threshold()` into the pipeline execution stream, calculating empirical batch rejection ratios ($\frac{\text{Quarantined}}{\text{Quarantined} + \text{Silver Valid}}$).
  - Enforced automated quality halting: aborts pipeline execution with `GxPBreachError` when rejection ratio exceeds the configured threshold (default 5%), preventing contaminated downstream Gold data products.
  - Automatically logs `rejection_ratio`, `quarantine_count`, `silver_valid_count`, and `quarantine_threshold` metrics to active MLflow runs.
- **Quarantine Remediation & Replay Engine (`analytical-layer/medallion/quarantine.py`)**:
  - Implemented `QuarantineRemediationEngine` providing automated, audit-trailed re-evaluation of quarantined records against updated vocabulary mappings (ICD-10 to SNOMED, LOINC).
  - Promotes remediated records into Silver Delta tables via idempotent Delta MERGE SCD Type 1 upserts (`upsert_silver_table()`).
  - Updates quarantine records to `REMEDIATED` status with UTC timestamps and emits remediation metrics (`remediated_count`, `remediation_rate`) to MLflow runs.
- **End-to-End Quarantine Integration & Unit Test Suites (`tests/integration/test_quarantine_integration.py`, `tests/unit/test_quarantine.py`)**:
  - Added 3 end-to-end integration tests validating dead-letter sink routing with raw JSON payload preservation, batch quality threshold enforcement and `GxPBreachError` abort, and idempotent remediation replay into Silver.
  - Added unit test suite covering `ClinicalFailureCode`, `format_quarantine_dataframe()`, and threshold evaluation logic.
- **Documentation & DataOps Guidance**:
  - Updated `README.md` with dead-letter quarantine architecture details and FDA 21 CFR Part 11 / ALCOA+ compliance mapping.
  - Expanded `docs/quality/testing-and-dataops.md` with testing commands and operational architecture for quarantine workflows.

### Changed
- **Spark Executor Memory Optimization (`analytical-layer/omop_cdm_v54/pipeline.py`)**:
  - Explicitly unpersisted cached Bronze parsed DataFrames (`df_clinical_parsed`, `df_diag_parsed`, `df_labs_parsed`) immediately after Silver/Quarantine count materialization to release Spark executor memory before executing Gold transformations.
- **Cross-Platform Parquet & Delta Fallback Resilience**:
  - Hardened PySpark & PyArrow fallback routines in `QuarantineRemediationEngine` and `MedallionWriter` to seamlessly support local development and Windows environments without native Hadoop `winutils.exe`/`hadoop.dll` binaries while preserving Delta Lake MERGE as the enterprise production standard.
  - Enforced timezone-naive UTC timestamp handling across PySpark DataFrame conversions to prevent Py4J/PySpark `TimestampType` conversion errors.
- **Flexible Ingestion Column Schema Resolution**:
  - Enhanced clinical condition and measurement remediation parsers to handle schema variations gracefully (e.g. `icd10_code` vs `code`, `numeric_value` vs `value`, and dash-stripped LOINC variants).

---

## [0.2.9] - 2026-08-20

### Added
- **LangGraph GxP Compliance State Graph Auditor (`agentic-ai/graph_auditor.py`)**:
  - Implemented 6-node autonomous state graph auditing MLflow run lineage, Delta Lake transaction commit logs (`_delta_log/`), and OMOP CDM schema conformance against FDA 21 CFR Part 11 parameters.
  - Built Human-in-the-Loop (HITL) review gates using LangGraph `interrupt()` and `Command(resume=...)` for qualified 21 CFR §11.50 Electronic Signatures and deviation justifications.
  - Direct Delta Lake commit log parser verifying commit sequence continuity (`DLT_SEQ_001`), timestamp monotonicity (`DLT_TIME_002`), and table metadata.
- **Automated MLflow GxP Audit Certificates**:
  - Automatically attaches signed `audit_receipts/gxp_audit_certificate.json` directly to audited MLflow runs with cryptographic SHA-256 receipts (`gxp_audit_receipt_sha256`) and regulatory status tags (`gxp_audit_status`).
- **Model Context Protocol (FastMCP) Clinical Data Server (`agentic-ai/mcp_server.py`)**:
  - Implemented `FoundryMCPServer` exposing 11 FastMCP tools for OMOP CDM v5.4 concept lookups (ICD-10 to SNOMED, LOINC labs, demographics, ClinVar variants), table schema definitions (`PERSON`, `CONDITION_OCCURRENCE`, `MEASUREMENT`, `COHORT`), Great Expectations data contracts, Delta Lake transaction commit logs, and MLflow GxP lineage auditing.
  - Dual-transport architecture supporting Standard I/O (`stdio`) for local AI agent desktop clients and Server-Sent Events (`sse`) for microservice integration.
- **Centralized Cryptographic Utility Module (`governance/crypto.py`)**:
  - Single source of truth for high-performance SHA-256 file streaming (4 KB blocks), in-memory string/byte hashing, and 64-character hexadecimal digest verification.
- **Comprehensive Unit Testing Suites (`tests/unit/test_graph_auditor.py`, `tests/unit/test_mcp_server.py`, `tests/unit/test_crypto.py`)**:
  - 26 unit tests across the agentic AI tier verifying state graph compilation, compliant/non-compliant runs, Delta transaction logs, HITL review, cryptographic hashing, and all 11 FastMCP clinical tools.
- **Vocabulary drift detection**: `_warn_on_fallback_drift()` added to `vocabularies.py`; surfaces any Python/JSON concept ID divergence as a `[VOCABULARY WARNING]` on startup.

### Changed
- **Unified Project Dependency Configuration (`pyproject.toml`)**:
  - Consolidated `langgraph` and `mcp` directly into core project dependencies.
  - Registered `network` pytest marker; remote GitHub/S3 integration tests are opt-in via `LSDF_NETWORK_TESTS=1`.
- **Expanded CI Quality Gates (`.github/workflows/tf-lint.yml`)**:
  - Integrated `agentic-ai` into Mypy static type checking and Pytest `--cov` coverage reporting in CI.
- **`README.md`**: rewritten as a concise entry-point; prerequisites and setup delegated to `CONTRIBUTING.md`.
- **`CONTRIBUTING.md`**: corrected Python/JDK version labels, pipeline invocation updated to `python -m omop_cdm_v54.pipeline`, network test opt-in documented.
- **`SECURITY.md`**: reformatted security controls as a table; added vulnerability reporting pointer.

---

## [0.2.8] - 2026-08-19

### Added
- **Production DataOps CI/CD Gate Expansion (`.github/workflows/tf-lint.yml`)**:
  - Multi-tier GitHub Actions continuous integration workflow executing on all `main` and `dev` branch pushes and pull requests.
  - Dedicated `infrastructure-validation` job verifying Terraform 1.5.0 IaC (`fmt -check`, `validate`) and Nextflow DSL2 dry-run stub execution.
  - Dedicated `python-quality-gate` job running `ruff check .`, `ruff format --check .`, and `mypy` strict static type verification.
  - Dedicated `pyspark-dataops-test-suite` job running `pytest` with OpenJDK 17 + Python 3.11, generating comprehensive `pytest-cov` terminal and XML coverage reports, and archiving build artifacts.
- **Standardized Toolchain Configuration (`pyproject.toml`)**:
  - Integrated explicit configurations for `[tool.ruff]`, `[tool.mypy]`, `[tool.pytest.ini_options]`, and `[tool.coverage]`.
- **Modular Documentation Hub (`docs/`)**:
  - Dedicated deep-dive guides for environment setup (`docs/setup/environment-setup.md`), cloud deployment & IaC (`docs/deployment/databricks-and-iac.md`), and testing/DataOps quality gates (`docs/quality/testing-and-dataops.md`).
- **Enterprise GxP Pull Request Template (`.github/pull_request_template.md`)**:
  - Established a standardized PR template requiring Conventional Commit classifications, FDA 21 CFR Part 11 / data contract compliance checklists, DataOps test verification sign-offs, and security declarations.

### Changed
- **Static Typing & Code Quality Hardening**:
  - Refined type hints across `analytical-layer/omop_cdm_v54/` (`connectors.py`, `pipeline.py`, `compat.py`, `vocabularies.py`), `analytical-layer/medallion/` (`writer.py`), and `governance/mlflow_tracker.py` to achieve 100% clean validation under `mypy` and `ruff`.
  - Added explicit non-null assertions across unit test suites to guarantee robust type safety on PySpark `Row` projections.
- **CI Supply Chain Security Hardening**:
  - Pinned all GitHub Actions steps in `.github/workflows/tf-lint.yml` to immutable commit SHAs for GxP supply chain security.
- **Executive README Architecture Blueprint (`README.md`)**:
  - Streamlined the platform blueprint with a pruned top-level architecture layout, direct documentation matrix links, and updated regulatory compliance mappings.

---

## [0.2.7] - 2026-08-14

### Added
- **Dynamic Vocabulary & Concept Mapping Engine (`analytical-layer/omop_cdm_v54/vocabularies.py`)**:
  - Externalized hardcoded ICD-10, LOINC, demographic, and ClinVar concept mappings into structured GxP-governed JSON specification (`governance/concept_mappings.json`) with full clinical descriptions.
  - Native PySpark `create_map` dynamic expression generator (`build_concept_lookup`) with automatic caching and zero-overhead column lookups.
  - Unit test suite (`tests/unit/test_vocabularies.py`) validating JSON vocabulary loading, metadata filtering, and PySpark map expressions.
- **Agentic Infrastructure Typed Interfaces (`agentic-ai/`)**:
  - Scaffolding of `GxPGraphAuditor` in `agentic-ai/graph_auditor.py` for Phase 6 LangGraph lineage evaluation.
  - Scaffolding of `FoundryMCPServer` in `agentic-ai/mcp_server.py` for Phase 6 FastMCP tool exposure.

### Changed
- **Package Configuration & Tooling (`pyproject.toml`)**:
  - Widened Python version support to `requires-python = ">=3.10, <3.13"`, supporting Python 3.12 environments.
  - Registered `governance` package in setuptools package discovery, eliminating ad-hoc `sys.path` test fixture hacks.
- **Silver Quality Filtering (`pipeline.py`)**:
  - Refactored clinical record filtering to use canonical complementary condition expressions (`valid_clinical_condition` and `~valid_clinical_condition`) with `df_clinical_parsed.cache()` for strictly mutually exclusive quarantine partitioning.
- **Centralized Path Resolution (`connectors.py` & `pipeline.py`)**:
  - Standardized dataset resolution via `resolve_data_dir()` helper with `LSDF_DATA_DIR` environment variable override support.

---

## [0.2.6] - 2026-08-13

### Added
- **End-to-End Integration Testing Suite (`tests/integration/`)** *(Merged PR #40)*:
  - Integration tests for full Medallion pipeline execution (`test_pipeline_execution.py`) verifying local synthetic datasets (`--mode demo`) with Delta Lake table persistence on disk, open data streaming (`--mode remote`), and inline Great Expectations data contract assertion enforcement.
  - Governance integration tests (`test_governance_integration.py`) asserting GxP runtime contract evaluation, `expectation_success_rate` metric calculation, SHA-256 cryptographic provenance hashing, and MLflow experiment logging.

---

## [0.2.5] - 2026-08-13

### Added
- **PySpark Unit Testing Suite (`tests/unit/`)** *(Merged PR #39)*:
  - Unit test suite covering domain transformers (`person.py`, `condition_occurrence.py`, `measurement.py`, `genomic_variants.py`).
  - Validation for gender/race/ethnicity normalization, birth date component parsing, ICD-10 to SNOMED CT concept resolution, LOINC code mappings, and ClinVar significance extraction from VCF `INFO` fields.
  - Session-scoped PySpark test fixture (`tests/conftest.py`) with Delta Spark extension integration (`DeltaSparkSessionExtension` & `DeltaCatalog`) and Windows Hadoop compatibility setup.

---

## [0.2.4] - 2026-08-13

### Added
- **Data Contract Runtime Enforcement Gate** *(Merged PR #35 & PR #37)*:
  - Great Expectations GxP data contract assertion gate (`evaluate_data_contract`) integrated directly into PySpark DataFrame Silver-to-Gold write streams in `pipeline.py`.

### Changed
- Upgraded surrogate key hashing from 32-bit `hash` to deterministic 64-bit `xxhash64`.
- Expanded gender string normalization to support single-letter codes (`M`/`F`).

---

## [0.2.3] - 2026-08-12

### Added
- **Delta Lake Storage & Performance Optimization** *(Merged PR #34)*:
  - PySpark Delta Lake Medallion writer (`analytical-layer/medallion/writer.py`) implementing Liquid Clustering (`CLUSTER BY (person_id, concept_id)`), Schema Evolution (`mergeSchema=True`), Deletion Vectors, Change Data Feed (CDF), and SCD Type 1 idempotent MERGE upserts (`DeltaTable.merge()`).
  - Configured Databricks Asset Bundles (`databricks.yml`) and Terraform Databricks workspace storage & serverless job orchestration IaC module (`terraform/databricks_medallion.tf`).

### Changed
- Moved `langgraph` and `mcp` to optional `[agentic]` installation extra in `pyproject.toml`.

---

## [0.2.2] - 2026-08-11

### Added
- **Modular PySpark OMOP CDM v5.4 Package Refactoring** *(Merged PR #32)*:
  - Refactored monolithic mapping script into modular PySpark domain packages (`analytical-layer/omop_cdm_v54/` with `person.py`, `measurement.py`, `condition_occurrence.py`, `genomic_variants.py`, `connectors.py`).
  - Added VCF v4.2 genomic variant call transformer (`genomic_variants.py`) mapping ClinVar annotations to standard OMOP `MEASUREMENT` concept IDs (`35917873`).
  - Added Open Data Dual-Ingestion Connectors (`connectors.py`) supporting `--mode demo` (local synthetic datasets) and `--mode remote` (streaming public open datasets from AWS Open Data S3 and NCBI HTTP endpoints with automatic memory guards and fallback).

---

## [0.2.1] - 2026-08-10

### Added
- **Documentation & Repository Governance** *(Merged PR #28, PR #29 & PR #31)*:
  - Added `CONTRIBUTING.md` with conventional commit specifications, GxP merge guidelines, and development setup *(PR #29)*.
  - Renamed repository to `life-sciences-data-foundry` and updated architectural documentation *(PR #31)*.
  - Added `ROADMAP.md` (originally `TODO.md`) engineering roadmap tracking document linked from main `README.md` *(PR #28)*.

---

## [0.2.0] - 2026-08-09

### Added
- Terraform GitHub governance module (`github_governance.tf`) with branch protection, secret scanning, and deployment environments *(Merged PR #14, #15, #16, #18)*
- Great Expectations GxP clinical validation suite (`governance/rules.json`) enforcing FDA 21 CFR Part 11 *(Merged PR #22)*
- MLflow lineage tracker (`governance/mlflow_tracker.py`) with SHA-256 cryptographic file hashing and multi-format dataset support *(Merged PR #22)*
- OHDSI OMOP CDM v5.4 PySpark Medallion normalization pipeline (`analytical-layer/omop_mapping.py`) mapping `PERSON` and `MEASUREMENT` tables *(Merged PR #23)*
- Nextflow DSL2 FastQC pipeline (`pipelines/main.nf`) with modular processes (`pipelines/modules/fastqc.nf`) and `aws_batch` queue targeting *(Merged PR #20)*
- Hardened PowerShell and Bash bootstrap scripts (`scripts/bootstrap.ps1`, `scripts/bootstrap.sh`) with auto-template fallback *(Merged PR #24)*
- `pyproject.toml` with Python dependency specifications

---

## [0.1.0] - 2026-05-27

### Added
- Initial Terraform IaC infrastructure with S3 WORM Object Locking and KMS encryption *(Merged PR #2, #11)*
- AWS ECS cluster and Batch compute environment with SPOT capacity optimization
- IAM execution roles and governance bypass policies for non-production tiers
- GitHub Actions CI/CD linting gate (`tf-lint.yml`) for HCL and Nextflow validation
- `.env.example` environment configuration template
