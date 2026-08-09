# ==============================================================================
# ROOT MODULE ENTRY POINT & DATA DISCOVERY
# ==============================================================================

# Dynamic Account & Region Discovery
data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

# Local Computed Variables & Normalized Metadata
locals {
  name_prefix = "life-sciences-platform-${var.environment}"

  # Centralized Tagging Matrix for GxP Lineage & FinOps Cost Allocation
  common_tags = {
    Environment        = var.environment
    ManagedBy          = "Terraform"
    Project            = "Life-Sciences-Platform"
    Repository         = "life-sciences-platform-blueprint"
    ComplianceStandard = "FDA_21_CFR_Part_11"
    DataClassification = "GxP_Restricted"
    Owner              = "Vivi Tsoumaki"
    AccountID          = data.aws_caller_identity.current.account_id
  }
}