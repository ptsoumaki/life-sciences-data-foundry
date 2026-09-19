---
name: pr-creator
description: >-
  Create and format GitHub Pull Requests adhering to the repository's GxP and FDA 21 CFR Part 11
  review template. Use when preparing PR descriptions, populating compliance checklists,
  verifying DataOps verification gates, or submitting PRs via GitHub CLI (gh).
---

# Pull Request Creator & Regulatory Review Guide

Runbook for drafting, populating, and submitting Pull Requests compliant with the repository's GxP review template (`.github/pull_request_template.md`).

## 1. Branch Naming Conventions

All branches must follow standard prefixed naming:
- `feat/<feature-description>`
- `fix/<bug-or-issue-description>`
- `refactor/<subsystem-description>`
- `docs/<documentation-topic>`
- `ci/<workflow-update>`

## 2. PR Preparation & Automated Verification Gates

Before opening a PR, run and confirm all required verification gates defined in `.github/pull_request_template.md`:

```powershell
# 1. Static Analysis & Type Verification
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .

# 2. PySpark Unit & Integration Testing Suite
.\.venv\Scripts\pytest.exe tests/unit/ -v --tb=short
.\.venv\Scripts\pytest.exe tests/integration/ -v --tb=short

# 3. Local Medallion Normalization & Governance Lineage
.\.venv\Scripts\python.exe analytical-layer/omop_cdm_v54/pipeline.py --mode demo --save_delta
.\.venv\Scripts\python.exe governance/mlflow_tracker.py

# 4. Declarative IaC Syntax & Formatting (Terraform, if applicable)
terraform -chdir=terraform fmt -check
terraform -chdir=terraform validate
```

## 3. Populating the PR Template

The PR body must follow `.github/pull_request_template.md` and complete all relevant sections:

### Section 1: Summary & Architectural Intent
Provide a clear, high-level summary of the changes and the architectural rationale behind them.

### Section 2: Change Classification (Conventional Commits)
Check the applicable boxes:
```markdown
- [x] `feat`: New capability, schema transformer, or OMOP CDM mapping target
- [ ] `fix`: Bug fix, error resolution, or data contract patch
...
```

### Section 3: Impacted Subsystems
Check all affected modules:
```markdown
- [x] `analytical-layer/` (PySpark OMOP CDM v5.4 normalizer, Medallion Delta Lakehouse)
- [x] `governance/` (Great Expectations data contracts, MLflow 21 CFR Part 11 lineage)
...
```

### Section 4: Regulatory & GxP Compliance Checklist (FDA 21 CFR Part 11)
Confirm compliance with regulatory standards:
- [x] **Data Contract Integrity**: Conforms to `governance/rules.json`.
- [x] **Cryptographic Lineage**: SHA-256 provenance and MLflow audit tracking intact.
- [x] **Immutable Records (WORM)**: S3 Object Locking and Delta Lake transaction log auditability preserved.
- [x] **Deterministic Transformations**: Deterministic 64-bit surrogate keys (`xxhash64`) and explicit casting.

### Section 5: DataOps Quality & Verification Gates
Check all validation gates that were executed and passed.

### Section 6: Security & Secret Protection
Confirm that no secrets, `.env` files, or local execution artifacts (`mock_data/`, `mlruns/`, `spark-warehouse/`) are committed.

## 4. Submitting the PR via GitHub CLI (`gh`)

### Option A: Using a Temporary Markdown Body File
```powershell
# 1. Draft the PR description into a temporary file
# 2. Create the PR using gh
gh pr create `
  --title "feat(omop): add genomic variant transformer and ClinVar mapping" `
  --body-file .pr_body.md `
  --base main `
  --head feat/genomic-variants
```

### Option B: Interactive PR Creation
```powershell
gh pr create --template .github/pull_request_template.md
```

## 5. Review & Regulatory Sign-Off Requirements

Under FDA 21 CFR Part 11 and repository governance (`terraform/github_governance.tf`):
- All PRs require at least one approving review from a code owner / peer reviewer.
- Automated CI status checks must pass before merging is permitted.
- Squash-merging or rebase-merging must preserve the Conventional Commit format for the final merge commit.
