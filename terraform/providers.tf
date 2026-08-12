terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    github = {
      source  = "integrations/github"
      version = "~> 6.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Environment        = var.environment
      ManagedBy          = "Terraform"
      Project            = "Life-Sciences-Data-Foundry"
      Repository         = "life-sciences-data-foundry"
      ComplianceStandard = "FDA_21_CFR_Part_11"
    }
  }
}

provider "github" {
  token = var.github_token
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}