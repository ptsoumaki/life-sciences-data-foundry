variable "aws_region" {
  type        = string
  description = "AWS region for infrastructure deployment. Provide via TF_VAR_aws_region or -var flag. No default to enforce explicit selection."
}

variable "environment" {
  type        = string
  description = "Deployment environment suffix (dev, staging, prod). Provide via TF_VAR_environment or -var flag. No default to enforce explicit selection."

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "The environment variable must be exactly 'dev', 'staging', or 'prod'."
  }
}