# 📋 Platform Engineering Backlog & TODO List

This document tracks upcoming milestones, roadmap phases, and operational hardening tasks for the **Enterprise Life Sciences Platform Blueprint**.

---

## 🤖 Phase 4: Agentic Compliance Auditing Tier (Roadmap Milestone)

- [ ] **LangGraph Multi-Agent Audit Loop (`agentic-ai/graph_auditor.py`)**
  - Implement state graph evaluating Terraform IaC state, S3 WORM object locks, and MLflow audit lineage against FDA 21 CFR Part 11 parameters.
- [ ] **Model Context Protocol Server (`agentic-ai/mcp_server.py`)**
  - Implement FastMCP server exposing GxP audit validation, Great Expectations rule checking, and MLflow lineage query tools.

---

## ⚡ Phase 5: GxP Hardening & Operational Excellence

### 1. DataOps CI/CD & Automated Testing
- [ ] **Python Pipeline CI Gate (`.github/workflows/tf-lint.yml`)**
  - Add Python code quality checks (`ruff`, `mypy`, `pytest`, `python -m py_compile`) to the GitHub Actions workflow.
- [ ] **Automated Integration Test Suite (`tests/`)**
  - Create `tests/` directory with `pytest` unit/integration tests covering `governance/mlflow_tracker.py`, `analytical-layer/omop_mapping.py`, and bootstrap scripts.

### 2. GxP Security & Audit Infrastructure
- [ ] **S3 Server Access Logging (`terraform/storage_and_compute.tf`)**
  - Provision an S3 audit log bucket (`aws_s3_bucket.audit_logs`) and attach `aws_s3_bucket_logging` to `raw_data` and `processed_data` buckets for FDA 21 CFR Part 11 access auditability.
- [ ] **Explicit KMS Key Access Policy (`terraform/storage_and_compute.tf`)**
  - Replace default KMS key policy with explicit IAM principal access restrictions to enforce strict key isolation.

### 3. Analytical Engine & Clinical Normalization
- [ ] **Delta Lake Output Persistence (`analytical-layer/omop_mapping.py`)**
  - Add configurable Delta Lake output writing (`df.write.format("delta").mode("overwrite").save(...)`) for local PySpark and Databricks Lakehouse execution.
- [ ] **OMOP `CONDITION_OCCURRENCE` Schema Mapping (`analytical-layer/omop_mapping.py`)**
  - Map clinical diagnosis phenotypes to SNOMED standard concept IDs (`201826`, `316866`).

### 4. Governance & Developer Experience
- [ ] **Flexible ISO-8601 Timestamp Validation (`governance/rules.json`)**
  - Update regex pattern to support ISO-8601 timestamps with millisecond precision (`.SSSZ`) and space separators.
- [ ] **Environment Cleanup Utilities (`scripts/clean.ps1` / `scripts/clean.sh`)**
  - Add cleanup script to reset local execution artifacts (`mock_data/`, `mlruns/`, `metastore_db/`, `spark-warehouse/`).
