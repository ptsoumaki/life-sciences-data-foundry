# Automated Testing, Quality Gates & DataOps CI/CD 🧪

This document describes the automated testing strategy, GxP data contract validation gates, and CI/CD pipelines implemented across the Life Sciences Data Platform.

---

## 🏗️ Testing Architecture Overview

```text
┌─────────────────────────────────────────────────────────────┐
│ 1. PySpark & Domain Unit Tests (tests/unit/)                │
│    ├── Demographics & OMOP PERSON mapping                   │
│    ├── ICD-10 to SNOMED CT condition concept mapping        │
│    ├── LOINC laboratory measurement transformations         │
│    ├── VCF v4.2 genomic variant feature extraction          │
│    ├── Dynamic PySpark vocabulary dictionary lookups        │
│    ├── GxP dead-letter quarantine (test_quarantine.py)      │
│    ├── Centralized SHA-256 crypto tests (test_crypto.py)    │
│    ├── LangGraph GxP auditor tests (test_graph_auditor.py)  │
│    ├── OHDSI cohort phenotyping (test_cohort_builder.py)    │
│    ├── HIPAA Safe Harbor de-id (test_deid.py)               │
│    ├── Survival analysis marts (test_survival.py)           │
│    └── Patient feature store & CCI (test_features.py)       │
├─────────────────────────────────────────────────────────────┤
│ 2. End-to-End Integration Tests (tests/integration/)        │
│    ├── Full Medallion pipeline execution (Demo & Remote)    │
│    ├── Delta Lake ACID persistence & Liquid Clustering      │
│    ├── Quarantine routing & breach gates (test_quarantine)  │
│    ├── Runtime Great Expectations assertion enforcement     │
│    └── Analytical cohort lifecycle (test_cohorts)           │
├─────────────────────────────────────────────────────────────┤
│ 3. GxP Compliance & Lineage Auditing (governance/ & AI)     │
│    ├── Decoupled JSON data contracts (rules.json)           │
│    ├── MLflow SHA-256 cryptographic provenance tracking     │
│    └── LangGraph state graph auditor with HITL sign-offs    │
├─────────────────────────────────────────────────────────────┤
│ 4. Workflow & IaC Validation (pipelines/ & terraform/)      │
│    ├── Nextflow DSL2 dry-run stub execution                 │
│    └── Terraform syntax, linting, and provider validation   │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Running Test Suites Locally

### 1. Unit Tests (PySpark, Governance & Agentic AI)
Executes unit tests verifying domain transformers, vocabulary resolution, cryptographic digests, dead-letter quarantine, LangGraph audit state machines, and analytical cohorts:

```bash
pytest tests/unit/ -v

# Run analytical cohort and survival unit suites specifically
pytest tests/unit/test_cohort_builder.py tests/unit/test_deid.py tests/unit/test_survival.py tests/unit/test_features.py -v

# Run quarantine unit suite specifically
pytest tests/unit/test_quarantine.py -v
```

### 2. End-to-End Integration Tests
Runs the complete Medallion pipeline using `pytest-spark` fixtures, validating Delta Lake writes, quarantine breach thresholds, data contract enforcement, and analytical cohorts:

```bash
pytest tests/integration/ -v

# Run cohort and feature store integration tests specifically
pytest tests/integration/test_cohorts_integration.py -v

# Run quarantine & remediation integration tests specifically
pytest tests/integration/test_quarantine_integration.py -v
```

### 3. Generate Code Coverage Report
Calculates branch and line test coverage across analytical, governance, and agentic AI modules:

```bash
pytest tests/ --cov=analytical-layer --cov=governance --cov=agentic-ai --cov-report=term-missing
```

### 4. GxP Data Contract & MLflow Lineage Audit
Executes Great Expectations assertions against OMOP CDM records and logs SHA-256 provenance hashes to MLflow:

```bash
python governance/mlflow_tracker.py
```

### 5. LangGraph GxP State Graph Auditor
Executes autonomous lineage audit across Delta Lake commit logs and MLflow runs:

```bash
python agentic-ai/graph_auditor.py --run-id <RUN_ID> --tracking-uri sqlite:///mlflow.db
```

### 6. Nextflow Pipeline Stub Verification
Validates Nextflow workflow orchestration using container stub mode:

```bash
mkdir -p mock_data && touch mock_data/sample_1.fastq
nextflow run pipelines/main.nf -profile local_dev -stub --raw_input "mock_data/*.fastq" --outdir "mock_data/out"
```

### 7. Static Code Quality & Type Checking
Runs `ruff` and `mypy` static type checking configured in [`pyproject.toml`](../../pyproject.toml):

```bash
# Code formatting & linting
ruff check .
ruff format --check .

# Strict static type checking across all packages
mypy --explicit-package-bases --ignore-missing-imports analytical-layer/omop_cdm_v54 analytical-layer/medallion analytical-layer/cohorts governance agentic-ai tests
```

### 8. Terraform IaC Validation
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
| **`"Lint & Validate Structural Blueprint"`** | `terraform validate`, Nextflow stub run | Prevents broken IaC and invalid workflow DAGs |
| **`"Lint & Static Type Verification Gate"`** | `ruff check`, `ruff format`, `mypy` | Enforces zero lint regressions and strict type safety across all tiers |
| **`"PySpark DataOps & Contract Quality Gate"`** | `pytest` (Unit + Integration) with OpenJDK 17 | Guarantees transformation correctness, Delta Lake contract compliance, and code coverage |
