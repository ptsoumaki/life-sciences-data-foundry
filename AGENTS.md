# Life Sciences Data Foundry — Agent Guidelines & Repository Architecture

Welcome to the **Life Sciences Data Foundry (LSDF)** codebase. This repository implements an enterprise-grade, GxP-compliant clinical and multi-omics normalization platform and Medallion Lakehouse adhering to OHDSI OMOP CDM v5.4, FDA 21 CFR Part 11, and HIPAA Safe Harbor standards.

---

## 1. System Architecture

The repository is structured into modular layers:

- **`analytical-layer/`**:
  - **`omop_cdm_v54/`**: Core PySpark Medallion engine (Bronze ingestion, Silver quality filtering, Gold OMOP transformation). Contains domain transformers: `person.py`, `condition_occurrence.py`, `measurement.py`, `genomic_variants.py`, and public open data connectors (`connectors.py`).
  - **`medallion/`**: Delta Lake persistence with Liquid Clustering, schema evolution contracts, Change Data Feed, and idempotent MERGE upserts (`writer.py`). Dead-letter quarantine sinks and remediation engine (`quarantine.py`).
  - **`cohorts/`**: Clinical phenotyping (`builder.py`), HIPAA Safe Harbor de-identification (`deid.py`), longitudinal survival analysis with Kaplan-Meier and Greenwood SE (`survival.py`), and multi-omics feature store with Charlson Comorbidity Index (`features.py`).
- **`governance/`**:
  - `concept_mappings.json`: Single source of truth for standard OMOP vocabulary cross-references (ICD-10 to SNOMED, LOINC, ClinVar, Gender, Race, Ethnicity).
  - `rules.json`: Great Expectations data contract specifications.
  - `mlflow_tracker.py`: GxP data contract evaluation with SHA-256 provenance checksums and MLflow run logging.
  - `crypto.py`: Cryptographic utilities (SHA-256 file/string hashing).
- **`agentic-ai/`**:
  - `graph_auditor.py`: LangGraph state graph auditor for Delta commit log auditing, MLflow lineage verification, and Human-in-the-Loop (HITL) QA sign-offs (FDA 21 CFR §11.50 / §11.200).
  - `mcp_server.py`: FastMCP clinical data server exposing OMOP CDM v5.4 lookups and governance telemetry to LLM assistants.
- **`terraform/` & `databricks.yml`**:
  - AWS cloud infrastructure (VPC, S3, KMS, IAM) and Databricks Asset Bundles (DABs) for `dev`, `staging`, and `prod` targets.
- **`tests/`**:
  - `tests/unit/`: Comprehensive PySpark and Python unit tests.
  - `tests/integration/`: End-to-end Medallion pipeline integration tests.
- **`notebooks/`**:
  - Interactive clinical and multi-omics analytical showcase notebooks (`clinical_multiomics_showcase.py`, `.ipynb`, `README.md`) executable via Databricks Workspaces, VS Code Interactive, or headless CLI.

---

## 2. Enterprise & GxP Constraints (Always Follow)

1. **Zero Data Loss Provability**:
   - Never silently drop records that fail schema validation, terminology mapping, or physiological bounds.
   - Route invalid records to dedicated Delta Lake dead-letter quarantine tables (`quarantine_patients`, `quarantine_conditions`, `quarantine_measurements`) via `format_quarantine_dataframe`, preserving the verbatim raw row payload as JSON, failure timestamp, and MLflow run ID.
2. **Audit Traceability (FDA 21 CFR Part 11)**:
   - Every pipeline run and data contract evaluation must compute SHA-256 digests of input datasets and configuration rules, logged to MLflow.
   - All Delta Lake writes must support Change Data Feed (`delta.enableChangeDataFeed = true`).
3. **HIPAA Safe Harbor Compliance**:
   - Longitudinal cohorts and published datasets must be de-identified:
     - Person IDs pseudonymized using HMAC-SHA256 with `LSDF_DEID_SALT`.
     - Patient event dates shifted by a deterministic patient-specific day offset preserving longitudinal intervals.
     - Age capped at 89 for individuals aged ≥ 90 (year of birth capped at `reference_year - 89`).
     - Specific `birth_datetime` timestamps cleared.
     - Geographic ZIP codes truncated to 3 digits (ZIP3) with restricted prefixes masked to `"000"`.
4. **OHDSI OMOP CDM v5.4 Standards**:
   - Primary and foreign keys (`person_id`, `condition_occurrence_id`, `measurement_id`) must use full 64-bit signed integers via `xxhash64(...).cast("long")` without `abs()`.
   - Unmapped source codes must resolve to concept ID `0`.
   - Standard vocabulary mapping lookups must load dynamically from `governance/concept_mappings.json` with fallback synchronization.

---

## 3. Code Quality & PySpark Conventions

- **Python Version**: Target Python ≥ 3.11 (`requires-python = ">=3.11, <3.13"`).
- **Avoid Eager PySpark Actions**:
  - Never call `df.rdd.isEmpty()` or unconditional `df.count()` inside transformation helper functions. Use single-partition checks like `df.limit(1).count() == 0` for early returns.
- **Exception Handling**:
  - Do not use bare `except Exception: pass`. Catch specific exceptions (`AnalysisException`, `Py4JJavaError`, `OSError`, `MlflowException`, etc.) and log warnings or re-raise.
  - For Delta storage operations on Windows local environments, account for native `hadoop.dll` limitations with clean fallbacks.
- **Library Import Best Practices**:
  - **Default to module-level imports**: Always place imports at the top of the module file (module header) following PEP 8 grouping (standard library, third-party packages, local application modules). This ensures fail-fast dependency validation and dependency transparency for GxP auditability.
  - **Avoid in-function imports**: In-function or inner-scope imports must not be used for routine code organization.
  - **Permitted exceptions**: In-function or deferred imports are strictly restricted to breaking unavoidable circular dependencies, `if TYPE_CHECKING:` typing guards (PEP 484), or isolating heavy optional CLI dependencies with explicit rationale.
- **Production-Ready Documentation**:
  - Never check in comments referencing bugs, review items, hacks, or temporary workarounds. All comments must be professional, accurate documentation of production behavior.

---

## 4. Testing & Verification

- Always use the project virtual environment for tests and linting:
  ```powershell
  .\.venv\Scripts\pytest.exe tests/unit/ -v --tb=short
  .\.venv\Scripts\ruff.exe check .
  .\.venv\Scripts\ruff.exe format --check .
  ```
- Any new features, transformers, or connectors must be accompanied by comprehensive unit tests under `tests/unit/`.
- When creating commits, adhere to Conventional Commits and always use bullet points (`- `) in the commit description (body) whenever more than one concept or change is presented.

---

## 5. Changelog & Release Management

- **Version-to-Version Delta Tracking**:
  - The `CHANGELOG.md` tracks externally visible deltas from one release version to the next, adhering to [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
  - Never include intra-version fixes (problems or bugs that emerged and were resolved during the development of a feature for that specific version). The changelog is a consumer- and auditor-facing release document, not a verbatim replay of git commit history.
  - Git commit messages document atomic technical steps via Conventional Commits, while the changelog synthesizes release-level capabilities, breaking changes, and external bug fixes.
