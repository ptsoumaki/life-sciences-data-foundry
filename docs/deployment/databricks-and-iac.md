# Cloud Deployment & Infrastructure as Code (IaC) Guide ☁️

This guide covers cloud infrastructure deployment, Databricks Asset Bundles (DABs) orchestration, and declarative Terraform provisioning for the Enterprise Life Sciences Data Platform.

---

## 🚀 1. Databricks Asset Bundles (DABs) Deployment

The repository includes a production-grade Databricks Asset Bundle specification ([`databricks.yml`](../../databricks.yml)) targeting Databricks Serverless Compute.

### Prerequisites
- Databricks CLI (`>=v0.228.0`)
- Databricks workspace access with token or OAuth authentication

### Deployment Steps

1. **Verify Databricks CLI Installation:**
   ```bash
   databricks version
   ```

2. **Configure Workspace Authentication:**
   ```bash
   databricks configure --host https://<your-workspace-instance>.cloud.databricks.com --token <dapi-token>
   ```

3. **Validate Bundle Configuration:**
   ```bash
   databricks bundle validate
   ```

4. **Deploy Pipeline to Databricks Workspace:**
   ```bash
   # Deploy to target environment (dev, staging, or prod)
   databricks bundle deploy --target dev
   ```

5. **Trigger Serverless Medallion Pipeline Job:**
   ```bash
   databricks bundle run omop_cdm_medallion_pipeline --target dev
   ```

---

## 🏗️ 2. Terraform Cloud Infrastructure Provisioning

The [`terraform/`](../../terraform/) module provisions immutable cloud storage, encryption keys, and container execution clusters.

### Core Resources Provisioned
* **AWS S3 WORM Storage:** S3 bucket with Object Lock in `COMPLIANCE` retention mode for GxP data integrity.
* **AWS KMS:** Customer-Managed Keys (CMK) with automatic cryptographic key rotation.
* **AWS Batch & ECS:** Episodic SPOT compute environment for containerized Nextflow processes.
* **GitHub Repository Governance:** Automated branch protection rules, required CI status checks, and secret scanning push protection ([`github_governance.tf`](../../terraform/github_governance.tf)).
* **Databricks Workspace Integration:** Workspace directories, secret scopes, and job definitions ([`databricks_medallion.tf`](../../terraform/databricks_medallion.tf)).

### Infrastructure Provisioning Steps

```bash
# 1. Navigate to Terraform module directory
cd terraform

# 2. Initialize provider plugins and backend
terraform init

# 3. Validate HCL syntax and configuration
terraform validate

# 4. Plan infrastructure changes
terraform plan -var="environment=dev" -var="aws_region=eu-west-1"

# 5. Apply Databricks and storage resources
terraform apply -auto-approve \
  -target=databricks_directory.project_dir \
  -target=databricks_workspace_file.pipeline_script
```

---

## 🔒 3. GxP Compliance & Security Controls

* **21 CFR Part 11 Audit Trails:** All S3 writes in `prod` enforce cryptographic Object Locking that prevents record modification or deletion even by administrative accounts.
* **Zero-Trust Secrets Management:** No secrets or credentials are hardcoded. Cloud credentials utilize IAM instance profiles or temporary STS assume-role tokens.
