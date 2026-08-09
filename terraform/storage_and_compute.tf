# ==============================================================================
# 1. ENCRYPTION & KEY MANAGEMENT (21 CFR PART 11 COMPLIANT)
# ==============================================================================

resource "aws_kms_key" "gxp_key" {
  description             = "Customer-managed KMS key for GxP data encryption at rest"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "gxp_key_alias" {
  name          = "alias/life-sciences-platform-${var.environment}"
  target_key_id = aws_kms_key.gxp_key.key_id
}

# ==============================================================================
# 2. RAW & PROCESSED STORAGE TIERS (WORM IMMUTABILITY & ENCRYPTION)
# ==============================================================================

resource "aws_s3_bucket" "raw_data" {
  bucket              = "life-sciences-platform-raw-${var.environment}"
  force_destroy       = var.environment == "prod" ? false : true
  object_lock_enabled = true
}

resource "aws_s3_bucket_versioning" "raw_versioning" {
  bucket = aws_s3_bucket.raw_data.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_public_access_block" "raw_private" {
  bucket                  = aws_s3_bucket.raw_data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_enc" {
  bucket = aws_s3_bucket.raw_data.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.gxp_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_object_lock_configuration" "raw_lock" {
  bucket     = aws_s3_bucket.raw_data.id
  depends_on = [aws_s3_bucket_versioning.raw_versioning]

  rule {
    default_retention {
      mode = var.environment == "prod" ? "COMPLIANCE" : "GOVERNANCE"
      days = 90
    }
  }
}

resource "aws_s3_bucket" "processed_data" {
  bucket        = "life-sciences-platform-processed-${var.environment}"
  force_destroy = var.environment == "prod" ? false : true
}

resource "aws_s3_bucket_versioning" "processed_versioning" {
  bucket = aws_s3_bucket.processed_data.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_public_access_block" "processed_private" {
  bucket                  = aws_s3_bucket.processed_data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed_enc" {
  bucket = aws_s3_bucket.processed_data.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.gxp_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# ==============================================================================
# 3. HYBRID BATCH COMPUTE TOPOLOGY FOR NEXTFLOW (FINOPS OPTIMIZED)
# ==============================================================================

resource "aws_ecs_cluster" "batch_cluster" {
  name = "life-sciences-platform-ecs-cluster-${var.environment}"
}

resource "aws_batch_compute_environment" "nextflow_exec" {
  compute_environment_name = "nextflow-compute-${var.environment}"
  type                     = "MANAGED"
  service_role             = aws_iam_role.batch_service_role.arn

  compute_resources {
    type                = "SPOT"
    allocation_strategy = "SPOT_CAPACITY_OPTIMIZED"
    bid_percentage      = 100
    ec2_configuration {
      image_type = "ECS_AL2"
    }
    instance_type      = ["c6i.xlarge", "c6i.2xlarge", "m6i.2xlarge", "r6i.2xlarge"]
    max_vcpus          = 128
    min_vcpus          = 0
    subnets            = var.subnet_ids
    security_group_ids = [var.security_group_id]
  }

  depends_on = [aws_iam_role_policy_attachment.batch_service_attach]
}

resource "aws_batch_job_queue" "nextflow_queue" {
  name     = "nextflow-job-queue-${var.environment}"
  state    = "ENABLED"
  priority = 1

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.nextflow_exec.arn
  }
}

# ==============================================================================
# 4. IAM EXECUTION ROLES & GOVERNANCE POLICIES
# ==============================================================================

resource "aws_iam_role" "batch_service_role" {
  name = "life-sciences-platform-batch-service-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "batch.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "batch_service_attach" {
  role       = aws_iam_role.batch_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

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

resource "aws_iam_role_policy_attachment" "ecs_execution_attach" {
  role       = aws_iam_role.batch_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_policy" "batch_s3_kms_policy" {
  name        = "life-sciences-platform-task-policy-${var.environment}"
  description = "Grants Nextflow workers read/write access to encrypted S3 zones"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*",
          aws_s3_bucket.processed_data.arn,
          "${aws_s3_bucket.processed_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = ["kms:Decrypt", "kms:DescribeKey", "kms:Encrypt", "kms:GenerateDataKey*"]
        Resource = [aws_kms_key.gxp_key.arn]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_task_s3_kms" {
  role       = aws_iam_role.batch_execution_role.name
  policy_arn = aws_iam_policy.batch_s3_kms_policy.arn
}

resource "aws_iam_policy" "s3_bypass_policy" {
  count       = var.environment == "prod" ? 0 : 1
  name        = "life-sciences-platform-s3-bypass-${var.environment}"
  description = "Allows bypassing S3 GOVERNANCE retention mode during dev testing"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
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

resource "aws_iam_role_policy_attachment" "attach_bypass" {
  count      = var.environment == "prod" ? 0 : 1
  role       = aws_iam_role.batch_execution_role.name
  policy_arn = aws_iam_policy.s3_bypass_policy[0].arn
}