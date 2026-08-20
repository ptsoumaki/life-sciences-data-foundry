# Governance & Quality Layer 🔒

This component implements the FDA 21 CFR Part 11 compliant data quality and execution lineage tracking layer for the Life Sciences Platform.

---

## Components

### `crypto.py` — Centralized Cryptographic Hashing Utilities (21 CFR §11.10(e))

Provides canonical, high-performance SHA-256 cryptographic hashing and verification for data files, in-memory payloads, and audit receipts.

**Public API:**
* `compute_sha256(target: str | bytes | Path) -> str`: Computes SHA-256 hex digest for file paths (streamed in 4 KB blocks) or in-memory content.
* `is_valid_sha256(hash_str: str | None) -> bool`: Regex validation of standard 64-character hexadecimal digests.

```python
from governance.crypto import compute_sha256, is_valid_sha256

digest = compute_sha256("data/raw/person.csv")
assert is_valid_sha256(digest) is True
```

### `rules.json` — Great Expectations GxP Validation Suite

A decoupled JSON expectation suite defining data quality contracts for OMOP CDM v5.4 clinical record ingestion. Designed to run independently of Databricks, enabling validation across Nextflow, AWS Batch, and local environments.

**Validated Expectations:**

| Expectation | Column | Severity | Purpose |
| --- | --- | --- | --- |
| `expect_column_values_to_not_be_null` | `person_id` | CRITICAL_FATAL | Enforces primary key integrity for audit traceability |
| `expect_column_values_to_match_regex` | `birth_datetime` | ERROR | Validates ISO-8601 UTC timestamp format |
| `expect_column_values_to_be_in_set` | `gender_concept_id` | WARNING | Checks alignment with OMOP vocabulary concepts |
| `expect_table_columns_to_match_set` | *(all)* | CRITICAL_FATAL | Guarantees presence of required clinical columns before Gold Delta persistence |

### `mlflow_tracker.py` — MLflow Lineage & Audit Tracker

Orchestrates Great Expectations suite execution, computes SHA-256 cryptographic file hashes for provenance tracking via `governance.crypto`, and logs all metrology to MLflow.

**Features:**
- Multi-format dataset ingestion (CSV, TSV, Parquet, JSON)
- Automatic synthetic OMOP CDM v5.4 test data generation
- SHA-256 cryptographic hash provenance for data and rule files
- MLflow experiment logging with governance contract artifacts

### `sample_clinical.csv` — Synthetic Test Dataset

Contains synthetic OMOP CDM v5.4 `PERSON` table records for local testing and CI/CD validation.

---

## Usage

```bash
# Run with default synthetic dataset
python governance/mlflow_tracker.py

# Run with custom data and rules
python governance/mlflow_tracker.py /path/to/data.csv governance/rules.json
```

---

## Dependencies

- `great-expectations >= 1.0.0`
- `mlflow >= 2.10.0`
- `pandas`
