# ==============================================================================
# 1. GxP COMPLIANT ISOLATED STORAGE BOUNDARIES WITH COMPLIANCE WORM LOCKING
# ==============================================================================

# Raw Ingestion Zone Bucket
resource "aws_s3_bucket" "raw_data" {
  bucket              = "life-sciences-platform-raw-${var.environment}"
  force_destroy       = var.environment == "prod" ? false : true # Protect prod from accidental teardowns
  object_lock_enabled = true
}

# Standalone Versioning Resource (Fixes nested block deprecation warnings)
resource "aws_s3_bucket_versioning" "raw_versioning" {
  bucket = aws_s3_bucket.raw_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Strict Compliance WORM Guardrail: Prevents deletion of raw data and clinical metadata for 90 days
resource "aws_s3_bucket_object_lock_configuration" "raw_lock" {
  bucket = aws_s3_bucket.raw_data.id

  # Ensures versioning is enabled before adding object locks
  depends_on = [aws_s3_bucket_versioning.raw_versioning]

  rule {
    default_retention {
      # Ternary operation: if environment is 'prod', lock with COMPLIANCE. Otherwise, use GOVERNANCE.
      mode = var.environment == "prod" ? "COMPLIANCE" : "GOVERNANCE"
      days = 90
    }
  }
}

# Processed Analytics Bucket
resource "aws_s3_bucket" "processed_data" {
  bucket        = "life-sciences-platform-processed-${var.environment}"
  force_destroy = var.environment == "prod" ? false : true
}

resource "aws_s3_bucket_versioning" "processed_versioning" {
  bucket = aws_s3_bucket.processed_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

# ==============================================================================
# 2. COMPUTE INFRASTRUCTURE - CONTAINERIZED REUSABLE ARCHITECTURE
# ==============================================================================

resource "aws_ecs_cluster" "batch_cluster" {
  name = "life-sciences-platform-ecs-cluster-${var.environment}"
}

# ==============================================================================
# 3. IDENTITY ACCESS MANAGEMENT (IAM) POLICIES FOR PIPELINE EXECUTION
# ==============================================================================

resource "aws_iam_role" "batch_execution_role" {
  name = "life-sciences-platform-execution-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}


# ==============================================================================
# 4. CONDITIONAL GOVERNANCE BYPASS POLICY (DEV/STAGING ONLY)
# ==============================================================================

# Create the bypass policy only if the environment is NOT 'prod'
resource "aws_iam_policy" "s3_bypass_policy" {
  count       = var.environment == "prod" ? 0 : 1
  name        = "life-sciences-platform-s3-bypass-${var.environment}"
  description = "Allows bypassing S3 GOVERNANCE retention mode during development testing"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "s3:BypassGovernanceRetention",
          "s3:DeleteBucket",
          "s3:DeleteObject",
          "s3:DeleteObjectVersion"
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*",
          aws_s3_bucket.processed_data.arn,
          "${aws_s3_bucket.processed_data.arn}/*"
        ]
      }
    ]
  })
}

# Attach the bypass policy to your batch execution role conditionally
resource "aws_iam_role_policy_attachment" "attach_bypass" {
  count      = var.environment == "prod" ? 0 : 1
  role       = aws_iam_role.batch_execution_role.name
  policy_arn = aws_iam_policy.s3_bypass_policy[0].arn
}