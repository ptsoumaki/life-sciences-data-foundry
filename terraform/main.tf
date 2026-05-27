# 1. Isolated Storage Boundaries with Object Locking for GxP Lineage
resource "aws_s3_bucket" "raw_data" {
  bucket        = "emop-genomics-raw-data-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket_object_lock_configuration" "raw_lock" {
  bucket = aws_s3_bucket.raw_data.id
  rule {
    default_retention {
      mode = "COMPLIANCE"
      days = 90
    }
  }
}

resource "aws_s3_bucket" "processed_data" {
  bucket        = "emop-genomics-processed-data-${var.environment}"
  force_destroy = true
}

# 2. Compute Infrastructure - ECS Cluster for Containerized Workflow Nodes
resource "aws_ecs_cluster" "batch_cluster" {
  name = "emop-genomics-compute-cluster-${var.environment}"
}

# 3. IAM Security Policies for Nextflow Cloud-Compute Execution
resource "aws_iam_role" "batch_execution_role" {
  name = "emop-batch-execution-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "storage_data_access" {
  name = "emop-storage-access-policy-${var.environment}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
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

resource "aws_iam_role_policy_attachment" "attach_storage" {
  role       = aws_iam_role.batch_execution_role.name
  policy_arn = aws_iam_policy.storage_data_access.arn
}

