---
name: gxp-git-commit
description: >-
  Execute GxP-compliant Git commits following Conventional Commits, atomic staging, and
  regulatory audit traceability. Use when staging changes, creating commit messages,
  verifying pre-commit quality gates (ruff, pytest), or ensuring no PHI/PII or secrets are staged.
---

# GxP-Compliant Git Commit Workflow

Standards and procedures for creating atomic, auditable, and GxP-compliant Git commits within the Life Sciences Data Foundry repository.

## 1. Conventional Commits Standard

All commit messages must adhere to the Conventional Commits specification:

```
<type>(<scope>): <concise imperative subject>

<detailed architectural and functional rationale>

Regulatory-Traceability: <standards or compliance references>
```

### Multi-Concept Commit Description Rule
**Always use bullet points (`- `)** in the commit description (body) whenever more than one concept or change is presented:

```
<type>(<scope>): <concise imperative subject>

- <First concept, architectural rationale, or functional change>
- <Second concept, architectural rationale, or functional change>
- <Third concept, architectural rationale, or functional change>

Regulatory-Traceability: <standards or compliance references>
```

### Allowed Types
- `feat`: New capability, domain transformer, or OMOP mapping target.
- `fix`: Bug fix, error resolution, or data contract patch.
- `docs`: Documentation, architecture specifications, or skill guides.
- `style`: Formatting, whitespace, or linting cleanup (no functional changes).
- `refactor`: Structural code improvement without contract or functional changes.
- `test`: Adding or updating unit, integration, or quality contract tests.
- `ci`: CI/CD workflows, toolchain configurations, or pipeline gates.
- `perf`: Query optimization, Liquid Clustering, or throughput scaling.
- `chore`: Dependency updates, build configuration, or maintenance.

### Standard Scopes
- `(omop)`: OMOP CDM v5.4 normalizers (`person`, `condition_occurrence`, `measurement`, `genomic_variants`).
- `(medallion)`: Delta Lake persistence, Liquid Clustering, quarantine engine.
- `(cohorts)`: Phenotype builder, HIPAA de-identification, survival analysis, feature store.
- `(governance)`: Concept mappings, Great Expectations rules, MLflow provenance tracker.
- `(agentic-ai)`: FastMCP server, LangGraph compliance auditor.
- `(dabs)`: Databricks Asset Bundles and workflow job specs.
- `(terraform)`: Cloud infrastructure as code, S3 WORM, IAM policies.
- `(tests)`: PySpark unit or integration test suites.

## 2. Pre-Commit Quality Gates

Before staging or committing any code, execute and verify all required quality gates:

```powershell
# 1. Static analysis & linting
.\.venv\Scripts\ruff.exe check .

# 2. Code formatting check
.\.venv\Scripts\ruff.exe format --check .

# 3. Unit test suite
.\.venv\Scripts\pytest.exe tests/unit/ -v --tb=short
```

All gates must pass with zero errors before proceeding to commit.

## 3. Secret, PHI & Artifact Prevention Checklist

Before running `git commit`, inspect staged changes (`git diff --staged`) to confirm:
- [ ] No API keys, cloud credentials, tokens, or `.env` files are staged.
- [ ] No local runtime artifacts (`mlruns/`, `spark-warehouse/`, `metastore_db/`, `.pytest_cache/`) are staged.
- [ ] No real patient Protected Health Information (PHI) or unencrypted clinical payloads are present.
- [ ] All comments in changed files are production-ready: no references to bug tracker IDs, temporary workarounds, review notes, or draft markers.

## 4. Conceptual Chunking & Atomic Staging

1. **One Logical Change per Commit**: Group related files together. Never combine unrelated changes (e.g., refactoring a PySpark transformer, updating Terraform, and writing documentation) in a single commit.
2. **Explicit Staging**:
   ```powershell
   # Stage specific files
   git add analytical-layer/omop_cdm_v54/person.py tests/unit/test_person.py

   # Check staged diff
   git diff --staged
   ```
3. **Commit Execution**:
   ```powershell
   git commit -m "feat(omop): support dotted and dotless ICD-10-CM codes in condition transformer" -m "Normalize condition source values by stripping decimal points before vocabulary lookup. Ensures complete alignment with SNOMED concept cross-references."
   ```

## 5. Changelog & Release Documentation Rules

1. **Version-to-Version Tracking**: The `CHANGELOG.md` records user- and auditor-facing changes between releases adhering to [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
2. **No Intra-Version Fixes**: Never document intra-version fixes (bugs or intermediate problems that emerged and were resolved during the implementation of a feature within that specific version release).
3. **Commit History vs. Changelog**: Conventional Commit messages record individual atomic commits; the changelog summarizes release-level features, external fixes, and breaking changes.
