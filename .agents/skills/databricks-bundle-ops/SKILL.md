---
name: databricks-bundle-ops
description: >-
  Manage, validate, and deploy Databricks Asset Bundles (DABs) for the Life Sciences Data Foundry.
  Use when updating workflow definitions (resources/omop_pipeline_job.yml), configuring targets
  (dev, staging, prod) in databricks.yml, deploying jobs, or validating bundle integrity.
---

# Databricks Asset Bundles (DABs) Operations

Runbook for configuring, validating, deploying, and executing Databricks Asset Bundles (DABs) across development, staging, and production environments.

## 1. Bundle Architecture & Target Environments

The bundle is defined in `databricks.yml` and targets the following tiers:

| Target | Mode | Root Path | Intended Usage |
| :--- | :--- | :--- | :--- |
| `dev` | `development` | `/Workspace/Projects/life-sciences-data-foundry/dev` | Interactive engineering, feature branches, isolated test runs. |
| `staging` | `production` | `/Workspace/Projects/life-sciences-data-foundry/staging` | Pre-release validation, automated CI integration, synthetic data runs. |
| `prod` | `production` | `/Workspace/Projects/life-sciences-data-foundry/prod` | Production Medallion Lakehouse ingestion and GxP regulatory workloads. |

### Configuration (`databricks.yml`)
- **Bundle Name**: `life-sciences-data-foundry`
- **Includes**: `resources/*.yml`
- **Host Variable**: `databricks_host` (overridable via `--var databricks_host=...` or `BUNDLE_VAR_databricks_host` environment variable).

## 2. Resource Definitions

Jobs and workflow tasks are declared in `resources/`:

- **`resources/omop_pipeline_job.yml`**:
  - Job Name: `[${bundle.target}] OMOP CDM v5.4 Medallion Pipeline`
  - Task Key: `execute_omop_normalization`
  - Python Script: `${workspace.root_path}/files/analytical-layer/omop_cdm_v54/pipeline.py`
  - Default Parameters:
    - `--mode demo`
    - `--save_delta`
    - `--output_dir /dbfs/FileStore/omop_warehouse/`

## 3. Operational CLI Commands

All commands are executed using the Databricks CLI (`databricks bundle`):

### 1. Validation
Validate bundle syntax, variable substitution, and schema correctness:
```powershell
databricks bundle validate -t dev
databricks bundle validate -t staging
databricks bundle validate -t prod
```

### 2. Deployment
Deploy bundle assets (scripts, job definitions, configuration) to the target workspace:
```powershell
# Development deployment
databricks bundle deploy -t dev

# Production deployment (requires appropriate IAM/workspace permissions)
databricks bundle deploy -t prod
```

### 3. Execution & Monitoring
Trigger a run of the deployed pipeline job:
```powershell
# Run the deployed OMOP normalization workflow
databricks bundle run -t dev omop_cdm_medallion_pipeline

# View running job status
databricks jobs list-runs --job-id <JOB_ID> --active-only
```

### 4. Teardown
Remove deployed bundle resources from a development environment:
```powershell
databricks bundle destroy -t dev
```

## 4. GxP Compliance & Security Controls

1. **Workspace Isolation**: Never deploy unvalidated code directly to `staging` or `prod`. Production deployments must originate from tagged releases via CI/CD service principals.
2. **Secret Management**: Never hardcode Databricks personal access tokens (PATs) or client secrets in `databricks.yml` or resource files. Use Databricks Secret Scopes (`{{secrets/scope/key}}`) or Databricks CLI authentication profiles (`--profile <profile_name>`).
3. **Change Tracking**: DABs configuration changes must be reviewed and merged through peer-reviewed pull requests adhering to repository GxP change management policies.
