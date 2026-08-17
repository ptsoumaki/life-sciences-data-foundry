# Automated Testing, Quality Gates & DataOps CI/CD 🧪

This document describes the automated testing strategy, GxP data contract validation gates, and CI/CD pipelines implemented across the Life Sciences Data Platform.

---

## 🏗️ Testing Architecture Overview

```text
┌─────────────────────────────────────────────────────────────┐
│ 1. PySpark Unit Tests (tests/unit/)                         │
│    ├── Demographics & OMOP PERSON mapping                   │
│    ├── ICD-10 to SNOMED CT condition concept mapping        │
│    ├── LOINC laboratory measurement transformations         │
│    ├── VCF v4.2 genomic variant feature extraction          │
│    └── Dynamic PySpark vocabulary dictionary lookups        │
├─────────────────────────────────────────────────────────────┤
│ 2. End-to-End Integration Tests (tests/integration/)        │
│    ├── Full Medallion pipeline execution (Demo & Remote)    │
│    ├── Delta Lake ACID persistence & Liquid Clustering      │
│    └── Runtime Great Expectations assertion enforcement     │
├─────────────────────────────────────────────────────────────┤
│ 3. GxP Compliance & Lineage Auditing (governance/)          │
│    ├── Decoupled JSON data contracts (rules.json)           │
│    └── MLflow SHA-256 cryptographic provenance tracking     │
├─────────────────────────────────────────────────────────────┤
│ 4. Workflow & IaC Validation (pipelines/ & terraform/)      │
│    ├── Nextflow DSL2 dry-run stub execution                 │
│    └── Terraform syntax, linting, and provider validation   │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Running Test Suites Locally

### 1. PySpark Unit Tests
Executes unit tests verifying domain transformers, vocabulary resolution, and data contract assertions:

```bash
pytest tests/unit/ -v
```

### 2. End-to-End Integration Tests
Runs the complete Medallion pipeline using `pytest-spark` fixtures, validating Delta Lake writes and data contract enforcement:

```bash
pytest tests/integration/ -v
```

### 3. Generate Code Coverage Report
Calculates branch and line test coverage across analytical modules:

```bash
pytest tests/ --cov=analytical-layer --cov=governance --cov-report=term-missing
```

### 4. GxP Data Contract & MLflow Lineage Audit
Executes Great Expectations assertions against OMOP CDM records and logs SHA-256 provenance hashes to MLflow:

```bash
python governance/mlflow_tracker.py
```

### 5. Nextflow Pipeline Stub Verification
Validates Nextflow workflow orchestration using container stub mode:

```bash
mkdir -p mock_data && touch mock_data/sample_1.fastq
nextflow run pipelines/main.nf -profile local_dev -stub --raw_input "mock_data/*.fastq" --outdir "mock_data/out"
```

### 6. Static Code Quality & Type Checking
Runs `ruff` and `mypy` static type checking configured in [`pyproject.toml`](../../pyproject.toml):

```bash
# Code formatting & linting
ruff check .
ruff format --check .

# Strict static type checking
mypy analytical-layer/omop_cdm_v54 governance/mlflow_tracker.py
```

### 7. Terraform IaC Validation
```bash
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform fmt -check
terraform -chdir=terraform validate
```

---

## 🛡️ CI/CD Pipeline Automation (`.github/workflows/tf-lint.yml`)

The repository enforces a multi-tier DataOps CI/CD gate on every pull request targeting `dev` or `main`:

| Job Name | Steps Executed | GxP Integrity Objective |
| :--- | :--- | :--- |
| **`infrastructure-validation`** | `terraform validate`, Nextflow stub run | Prevents broken IaC and invalid workflow DAGs |
| **`python-quality-gate`** | `ruff check`, `ruff format`, `mypy` | Enforces zero lint regressions and strict type safety |
| **`pyspark-dataops-test-suite`** | `pytest` (Unit + Integration) with OpenJDK 17 | Guarantees transformation correctness and Delta Lake contract compliance |
