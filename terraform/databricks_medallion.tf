# ==============================================================================
# DATABRICKS MEDALLION LAKEHOUSE & OMOP CDM REPOSITORY INFRASTRUCTURE
# ==============================================================================
# Configures Databricks workspace resources via Terraform:
# - Workspace Project Directory (/Workspace/Projects/life-sciences-data-foundry)
# - Script Deployment inside Project Directory
# - Automated PySpark OMOP CDM v5.4 Pipeline Job Orchestration (Serverless Compute)
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

# 2. Pipeline Script deployment inside Project Directory
resource "databricks_workspace_file" "pipeline_script" {
  content_base64 = filebase64("${path.module}/../analytical-layer/omop_cdm_v54/pipeline.py")
  path           = "${databricks_directory.project_dir.path}/omop_cdm_pipeline.py"
}

# 3. Serverless Compute Pipeline Job
resource "databricks_job" "omop_cdm_medallion_pipeline" {
  name = "omop-cdm-v54-medallion-pipeline-${var.environment}"

  environment {
    environment_key = "default"
    spec {
      client = "1"
    }
  }

  task {
    task_key        = "execute_omop_normalization"
    environment_key = "default"

    spark_python_task {
      python_file = databricks_workspace_file.pipeline_script.path
      parameters = [
        "--mode", "demo",
        "--save_delta",
        "--output_dir", "/dbfs/FileStore/omop_warehouse/"
      ]
    }
  }

  description = "Orchestrates PySpark Medallion Delta Lake ingestion with Liquid Clustering & Schema Evolution contracts."
}
