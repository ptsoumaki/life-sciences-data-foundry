# Changelog

All notable changes to the Life Sciences Data Foundry project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.8] - 2026-08-17

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

### Fixed
- **OMOP CDM v5.4 Measurement Payload Preservation (`analytical-layer/omop_cdm_v54/measurement.py`)**:
  - Preserved qualitative observation values in `value_source_value` while applying `try_cast` for numeric parsing into `value_as_number` to prevent data loss on non-numeric payloads.
- **Condition Occurrence Composite Primary Key (`analytical-layer/omop_cdm_v54/condition_occurrence.py`)**:
  - Generated deterministic composite primary keys for `condition_occurrence_id` and optimized dynamic vocabulary map evaluation.
- **Delta Lake Z-Ordering & Cloud Table Verification (`analytical-layer/medallion/writer.py`)**:
  - Corrected Delta table Z-Order invocation syntax and S3 cloud storage path detection.

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

### Fixed
- **MLflow Lineage Tracker Hardening (`governance/mlflow_tracker.py`)**:
  - Isolated temporary validation artifact writes within `tempfile.TemporaryDirectory()`, eliminating working tree clutter and concurrent race conditions.
  - Prevented nested `with mlflow.start_run()` contexts from prematurely closing parent experiment runs.
  - Added logging for unhandled Great Expectations assertion classes and exposed evaluation failure error states.
- **OMOP CDM v5.4 Specification Compliance**:
  - Standardized foreign key fields (`provider_id`, `visit_occurrence_id`, `stop_reason`) to `NULL` instead of `0` in `condition_occurrence.py`.
  - Preserved full timestamp precision in `measurement_datetime` across `measurement.py`.
  - Prevented multi-allelic variant hash collisions in `genomic_variants.py` by incorporating `col("alt")` into deterministic 64-bit surrogate keys.
- **Delta Lake Storage Multi-Platform Compatibility (`analytical-layer/medallion/writer.py`)**:
  - Normalized all Delta Lake table paths to forward slashes, preventing Windows Hadoop path parsing issues.
  - Prevented redundant liquid clustering fallback attempts when Unity Catalog is disabled.

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

### Fixed
- Added missing `coalesce` and `expr` function imports in `pipeline.py` preventing timestamp parsing errors.
- Guarded `ALTER TABLE` Liquid Clustering execution in `writer.py` to operate only when Unity Catalog is enabled, avoiding Windows local file locking.
- Resolved patient/sample identifier column resolution bug in `genomic_variants.py`.

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
  - Added `TODO.md` engineering roadmap tracking document linked from main `README.md` *(PR #28)*.

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
