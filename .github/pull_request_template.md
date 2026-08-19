## 📋 Pull Request Description

### 🎯 Summary & Architectural Intent
<!-- Provide a concise summary of the changes and the architectural rationale behind this PR. -->

### 🏷️ Change Classification (Conventional Commits)
- [ ] `feat`: New capability, schema transformer, or OMOP CDM mapping target
- [ ] `fix`: Bug fix, error resolution, or data contract patch
- [ ] `docs`: Documentation, architecture specifications, or roadmap updates
- [ ] `style`: Formatting, whitespace, or linting cleanup (no code logic changes)
- [ ] `refactor`: Structural code improvement without functional or contract changes
- [ ] `test`: Adding or updating unit, integration, or quality contract tests
- [ ] `ci`: GitHub Actions DataOps workflows, toolchain, or pipeline CI gates
- [ ] `perf`: Delta Lake Liquid Clustering, query optimization, or throughput scaling
- [ ] `chore`: Build backend, dependencies, or package maintenance

### 🧩 Impacted Subsystems
- [ ] `analytical-layer/` (PySpark OMOP CDM v5.4 normalizer, Medallion Delta Lakehouse)
- [ ] `governance/` (Great Expectations data contracts, MLflow 21 CFR Part 11 lineage)
- [ ] `agentic-ai/` (FastMCP server, LangGraph compliance state auditor)
- [ ] `pipelines/` (Nextflow DSL2 workflow orchestration, container profiles)
- [ ] `terraform/` (Declarative cloud IaC, S3 WORM Object Locking, Databricks Asset Bundles)
- [ ] `tests/` (PySpark unit & end-to-end integration test suites)
- [ ] `docs/` (Platform documentation hub & component deep dives)

---

## 🛡️ Regulatory & GxP Compliance Checklist (FDA 21 CFR Part 11)

- [ ] **Data Contract Integrity**: Schema modifications conform to Great Expectations contracts in [`governance/rules.json`](governance/rules.json).
- [ ] **Cryptographic Lineage**: SHA-256 provenance hashing and MLflow audit tracking remain intact ([`governance/mlflow_tracker.py`](governance/mlflow_tracker.py)).
- [ ] **Immutable Records (WORM)**: No bypass of S3 Object Locking (`COMPLIANCE` retention mode) or Delta Lake transaction log auditability.
- [ ] **Deterministic Transformations**: Transformations utilize deterministic 64-bit surrogate keys (`xxhash64`) and explicit type casting.

---

## 🧪 DataOps Quality & Verification Gates

Please confirm all required automated validation gates pass cleanly before requesting review:

- [ ] **Static Analysis & Type Verification**:
  ```bash
  ruff check .
  ruff format --check .
  mypy --explicit-package-bases --ignore-missing-imports analytical-layer/omop_cdm_v54 analytical-layer/medallion governance tests
  ```
- [ ] **PySpark Unit & Integration Testing Suite**:
  ```bash
  pytest tests/unit/ -v
  pytest tests/integration/ -v
  ```
- [ ] **Local Medallion Normalization & Governance Lineage**:
  ```bash
  python analytical-layer/omop_cdm_v54/pipeline.py --mode demo --save_delta
  python governance/mlflow_tracker.py
  ```
- [ ] **Declarative IaC Syntax & Formatting (Terraform)**:
  ```bash
  terraform -chdir=terraform fmt -check
  terraform -chdir=terraform validate
  ```
- [ ] **Workflow Engine Stub Verification (Nextflow)**:
  ```bash
  nextflow run pipelines/main.nf -profile local_dev -stub
  ```

---

## 🔒 Security & Secret Protection

- [ ] Verified that no secrets, API tokens, cloud credentials, or `.env` files are committed.
- [ ] Verified that all local execution artifacts (`mock_data/`, `mlruns/`, `metastore_db/`, `spark-warehouse/`) are excluded by `.gitignore`.
- [ ] Adheres to branch protection rules and peer review governance ([`terraform/github_governance.tf`](terraform/github_governance.tf)).
