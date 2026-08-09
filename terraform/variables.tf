variable "environment" {
  type        = string
  description = "Deployment environment suffix (dev, staging, prod). Provide via TF_VAR_environment or -var flag. No default to enforce explicit selection."

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "The environment variable must be exactly 'dev', 'staging', or 'prod'."
  }
}

variable "is_solo_developer" {
  type        = bool
  default     = true
  description = "Toggle to false when onboarding team members to enforce multi-party peer reviews."
}

variable "github_token" {
  type        = string
  sensitive   = true
  description = "GitHub Personal Access Token with repo administration scopes."
}

variable "aws_region" {
  type        = string
  description = "AWS region for infrastructure deployment. Provide via TF_VAR_aws_region or -var flag. No default to enforce explicit selection."
}

variable "subnet_ids" {
  type        = list(string)
  description = "Target VPC private subnet IDs for AWS Batch compute environments."
}

variable "security_group_id" {
  type        = string
  description = "Security group ID allowing egress for Nextflow container execution tasks."
}