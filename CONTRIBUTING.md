# Contributing to Enterprise Life Sciences Data Foundry 🧬

Thank you for contributing to **Enterprise Life Sciences Data Foundry**. This project provides an enterprise-grade, GxP-compliant data foundry for high-throughput biological sequencing and clinical record normalization.

To maintain strict regulatory integrity (FDA 21 CFR Part 11), zero-trust security, and high-quality software engineering standards, all contributions must adhere to the workflows and guidelines described in this document.

---

## 📋 Table of Contents

1. [Strategic Vision & GxP Quality Principles](#-strategic-vision--gxp-quality-principles)
2. [Prerequisites & Development Toolchain](#-prerequisites--development-toolchain)
3. [Environment Setup & Synchronization](#-environment-setup--synchronization)
4. [Git Branching & Workflow Model](#-git-branching--workflow-model)
5. [Conventional Commit Specifications](#-conventional-commit-specifications)
6. [Code Style & Formatting Guidelines](#-code-style--formatting-guidelines)
7. [Local Validation & Testing Gate](#-local-validation--testing-gate)
8. [Pull Request (PR) & Code Review Process](#-pull-request-pr--code-review-process)
9. [Security & Secret Protection](#-security--secret-protection)

---

## 🛡️ Strategic Vision & GxP Quality Principles

Every modification to this platform impacts data governance, compliance auditability, and clinical pipeline stability. Contributors must uphold:

- **Immutable Lineage & Auditability**: System changes must preserve SHA-256 cryptographic provenance tracking and MLflow audit logs.
- **Declarative Infrastructure**: Manual cloud infrastructure provisioning via web consoles is strictly prohibited. All AWS/GitHub resources must be managed via Terraform IaC.
- **Decoupled Validation Gates**: Data quality contracts must remain decoupled from specific execution engines using Great Expectations (`governance/rules.json`).
- **Containerized Process Isolation**: Compute workloads must execute in containerized environments (Docker/AWS Batch) with pinned versions.

---

## 🛠️ Prerequisites & Development Toolchain

Ensure your local development station has the following required toolchain versions installed:

| Tool | Required Version | Purpose |
| --- | --- | --- |
| **Python** | `>=3.10, <3.13` (3.12 supported) | Governance validation, lineage tracking, and PySpark OMOP CDM mapping |
| **Java / JDK** | `17` (LTS; `>=11` minimum) | JVM runtime engine required by Nextflow and PySpark |
| **Terraform** | `>=1.5.0` | Declarative IaC infrastructure provisioning and governance |
| **Nextflow** | `>=23.04.0` | Episodic containerized DSL2 workflow orchestration |
| **Docker Engine** | Latest Stable | Local process execution context for Nextflow pipelines |
| **AWS CLI** | `v2` | AWS infrastructure authentication and deployment verification |

> 📖 For comprehensive environment installation and Java configuration, refer to the [**Environment Setup Guide**](docs/setup/environment-setup.md).

---

## ⚙️ Environment Setup & Synchronization

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/ptsoumaki/life-sciences-data-foundry.git
   cd life-sciences-data-foundry
   ```

2. **Configure Local Environment File:**
   Copy the `.env.example` template to `.env` in the repository root:
   ```bash
   cp .env.example .env
   ```

3. **Bootstrap Environment Variables:**
   Run the platform bootstrap script to export parameters into your process scope:

   - **POSIX (Linux / macOS / WSL):**
     ```bash
     source scripts/bootstrap.sh
     ```

   - **PowerShell (Windows):**
     ```powershell
     .\scripts\bootstrap.ps1
     ```

   The bootstrap script automatically sets `ENVIRONMENT`, `AWS_REGION`, `TF_VAR_environment`, and `TF_VAR_aws_region`.

---

## 🌿 Git Branching & Workflow Model

This repository follows a structured **Feature Branching Model** built around `main` and `dev` branches:

- `main`: Production-ready, locked state. Direct pushes and force pushes are blocked.
- `dev`: Active integration branch for upcoming releases.
- `feature/<short-description>`: New platform capability or module.
- `fix/<short-description>`: Bug fixes and patch corrections.
- `docs/<short-description>`: Documentation, architecture specs, and diagram updates.
- `chore/<short-description>`: Tooling, dependency updates, and maintenance.

### Standard Branch Workflow

1. Always branch from `dev`:
   ```bash
   git checkout dev
   git pull origin dev
   git checkout -b feature/omop-condition-mapping
   ```

2. Develop changes, commit following Conventional Commits, and push to your remote branch:
   ```bash
   git push -u origin feature/omop-condition-mapping
   ```

3. Open a Pull Request targeting the `dev` branch.

---

## 📝 Conventional Commit Specifications

All commit messages must adhere to the [Conventional Commits](https://www.conventionalcommits.org/) specification (`<type>(<scope>): <short summary>`):

### Format

```text
<type>(<scope>): <short descriptive summary in imperative mood>

[optional body giving technical context or regulatory rationale]

[optional footer(s), e.g., Closes #123]
```

### Allowed Types

- `feat`: A new feature or capability (e.g., adding a new OMOP CDM target table).
- `fix`: A bug fix or patch.
- `docs`: Documentation changes only (e.g., updating README or component specs).
- `style`: Formatting, missing semi-colons, whitespace fixes (no code logic changes).
- `refactor`: Code change that neither fixes a bug nor adds a feature.
- `test`: Adding missing tests or correcting existing test suites.
- `chore`: Maintenance, build scripts, or dependency updates.
- `ci`: Changes to GitHub Actions workflows (`.github/workflows/`).
- `perf`: Code changes that improve pipeline performance or resource utilization.

### Allowed Scopes

- `terraform` (or `iac`): Terraform modules and infrastructure declarations.
- `pipelines` (or `nextflow`): DSL2 pipeline scripts, config, or templates.
- `governance`: Great Expectations rules, MLflow tracking, or audit scripts.
- `analytical`: PySpark OMOP CDM mapping scripts and Delta Lake layer.
- `agentic`: LangGraph state graphs and MCP server code.
- `scripts`: PowerShell or POSIX bootstrap/utility scripts.
- `deps`: Dependency updates in `pyproject.toml` or lockfiles.

### Examples

- **Valid:** `feat(analytical): add CONDITION_OCCURRENCE schema mapping for SNOMED concepts`
- **Valid:** `fix(terraform): resolve S3 WORM retention governance policy syntax`
- **Valid:** `docs(readme): add contributing guide link and correct nextflow execution profile`
- **Invalid:** `updated code` (lacks type, scope, and imperative format)
- **Invalid:** `FIX: fixed bug in script` (incorrect casing and structure)

### Merge Commit Specifications

To satisfy GxP / FDA 21 CFR Part 11 auditability standards, explicit merge commits are enforced (`allow_merge_commit = true`). Squash and rebase merges are disabled in repository settings (`github_governance.tf`).

- **Standard GitHub Merge Format (Default):**
  - **Subject:** `Merge pull request #<PR_NUMBER> from <branch-name>` (or `Merge branch '<feature-branch>' into <target-branch>`)
  - **Body:** Summary of changes merged via the PR.
- **Custom / Conventional Merge Format:**
  - **Subject:** `<type>(<scope>): merge branch '<feature-branch>' into <target-branch>`
  - **Body:** Details on technical context, risk assessment, or regulatory rationale, ending with issue links (e.g., `Closes #123`).


---

## 🎨 Code Style & Formatting Guidelines

### 1. Terraform / HCL (`terraform/`)
- Run `terraform fmt` on all `.tf` files before committing.
- Ensure all variable declarations in `variables.tf` include explicit `type`, `description`, and appropriate `default` or validation rules.
- Set `sensitive = true` on any variable containing secrets or keys.

### 2. Python (`governance/`, `analytical-layer/`, `agentic-ai/`)
- Follow PEP 8 style conventions.
- Include explicit type annotations for function signatures.
- Avoid hardcoded file paths or inline magic strings; use `.env` parameters or CLI arguments.
- Do not use print statements for production logging; use standard `logging` or MLflow metric logging.

### 3. Nextflow DSL2 (`pipelines/`)
- Maintain process modularity by placing process definitions inside `pipelines/modules/`.
- Keep environment-dependent parameters inside `pipelines/nextflow.config`.
- Specify explicit biocontainer image tags for all process definitions (`container 'biocontainers/fastqc:v0.11.9_cv8'`).

---

## 🧪 Local Validation & Testing Gate

Before opening a Pull Request, run the platform validation suite locally:

1. **PySpark Unit & Integration Tests:**
   ```bash
   pytest tests/unit/ -v
   pytest tests/integration/ -v
   # Network tests (GitHub/S3) are opt-in; set LSDF_NETWORK_TESTS=1 to enable:
   LSDF_NETWORK_TESTS=1 pytest tests/integration/ -m network
   ```

2. **Governance & Audit Tracker Verification:**
   ```bash
   python governance/mlflow_tracker.py
   ```

3. **Analytical PySpark Normalization Execution:**
   ```bash
   python -m omop_cdm_v54.pipeline --mode demo --save-delta
   ```

4. **Nextflow Pipeline Stub Verification:**
   ```bash
   mkdir -p mock_data && touch mock_data/sample_1.fastq
   nextflow run pipelines/main.nf -profile local_dev -stub --raw_input "mock_data/*.fastq" --outdir "mock_data/out"
   ```

5. **Terraform Format & Syntax Validation:**
   ```bash
   cd terraform
   terraform init -backend=false
   terraform fmt -check
   terraform validate
   cd ..
   ```

> 📖 For full CI/CD specifications and code quality details, see the [**Testing & DataOps Guide**](docs/quality/testing-and-dataops.md).

---

## 🔄 Pull Request (PR) & Code Review Process

1. **Target Branch:** All feature PRs must target `dev` (never `main` directly).
2. **CI Checks:** The automated DataOps CI/CD workflow (`.github/workflows/tf-lint.yml`) must pass cleanly.
3. **PR Checklist:**
   - [ ] Conventional Commit message standards followed.
   - [ ] Local linting and stub verification checks passed.
   - [ ] Component documentation updated if new parameters or files were introduced.
   - [ ] No secrets, `.env` files, or temporary credentials committed.
4. **Approval:** At least one peer review approval is required before merging.

---

## 🔒 Security & Secret Protection

- **Secret Protection:** Never commit API keys, cloud credentials, tokens, or private endpoints.
- **Git Exclusions:** Ensure `.env`, `*.tfstate`, `.nextflow/`, `mlruns/`, and temporary output directories are listed in `.gitignore`.
- **Secret Scanning:** Repository push protection is active via Terraform GitHub Governance (`github_governance.tf`). Commits containing detected secrets will be automatically blocked.
- **Reporting Vulnerabilities:** For sensitive security disclosures, refer to [SECURITY.md](SECURITY.md).
