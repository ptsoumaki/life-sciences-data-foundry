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
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Environment = var.environment
      ManagedBy   = "Terraform"
      Project     = "Life-Sciences-Platform"
      Repository  = "life-sciences-platform-blueprint"
    }
  }
}

provider "github" {
  # Authenticates via Personal Access Token (PAT) or GitHub App token
  token = var.github_token
}



