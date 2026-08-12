# ==============================================================================
# DATABRICKS MEDALLION LAKEHOUSE & OMOP CDM REPOSITORY INFRASTRUCTURE
# ==============================================================================
# Manages Databricks workspace infrastructure primitives via Terraform:
#   - Secret Scope (credential vault, least-privilege GxP compliant)
#   - Workspace Project Directory
#   - Pipeline Script file deployment into the Project Directory
#
# Pipeline job orchestration (scheduling, compute, parameters) is owned by
# Databricks Asset Bundles (DABs) in resources/omop_pipeline_job.yml.
# Separating infrastructure from orchestration follows the Databricks-recommended
# IaC pattern: Terraform manages primitives, DABs manages pipeline lifecycle.
# ==============================================================================

resource "databricks_secret_scope" "life_sciences_vault" {
  count = var.enable_secret_scope ? 1 : 0
  name  = "life-sciences-vault-${var.environment}"
  # initial_manage_principal omitted: defaults to admin-only manage rights (least-privilege, GxP compliant)
}

# 1. Project Directory in Databricks Workspace
resource "databricks_directory" "project_dir" {
  path = "/Workspace/Projects/life-sciences-data-foundry"
}

# 2. Pipeline script deployed into the Project Directory.
#    The DABs job in resources/omop_pipeline_job.yml references this path
#    via ${workspace.root_path}/files/analytical-layer/omop_cdm_v54/pipeline.py.
resource "databricks_workspace_file" "pipeline_script" {
  content_base64 = filebase64("${path.module}/../analytical-layer/omop_cdm_v54/pipeline.py")
  path           = "${databricks_directory.project_dir.path}/omop_cdm_pipeline.py"
}

# NOTE: databricks_job resource removed.
# Pipeline orchestration (job definition, compute environment, task parameters)
# is managed exclusively by Databricks Asset Bundles (DABs) to avoid dual-IaC
# conflicts. Deploy and run the pipeline via:
#   databricks bundle deploy --target <dev|staging|prod>
#   databricks bundle run omop_cdm_medallion_pipeline
