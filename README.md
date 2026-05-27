# multiomics-platform-blueprint

## Repo structure

/aws-batch-genomics-lakehouse
├── .github/
│   └── workflows/
│       └── tf-lint.yml     # Automated CI/CD syntax and policy gate
├── terraform/
│   ├── providers.tf        # AWS provider and version constraints
│   ├── variables.tf        # Environment and region abstractions
│   └── main.tf             # Core IaC resources (S3, ECS, IAM)
├── pipelines/
│   └── README.md           # Placeholder for Nextflow/Snakemake workflows
└── notebooks/
    └── README.md           # Placeholder for PySpark/Delta Lake ETL scripts

The layout decouples the infrastructure definitions (/terraform) from the data orchestration logic (/pipelines) and the analytical compute layers (/notebooks). This separation mirrors enterprise software development lifecycles (SDLC) required in regulated GxP environments.


## Execution Mechanism
- Object Lock Configuration: The aws_s3_bucket_object_lock_configuration resource locks raw sequencing inputs (FASTQ/CRAM) in compliance mode. This technical constraint guarantees data immutability, directly satisfying FDA 21 CFR Part 11 requirements for data integrity before secondary processing runs.

- IAM Isolation: The policy explicitly narrows execution permissions to the exact source and target buckets. This prevents broad access configuration patterns that trigger security rejections during enterprise security audits.

- Non-Provisioning Validation: Running terraform init -backend=false in the CI/CD pipeline allows GitHub Actions to evaluate the complete syntactic dependency graph of the cloud layout without connecting to an AWS account or generating infrastructure costs.


# Architectural Design Document: Multi-Omics Enterprise Platform
## System Topology, Design Constraints, and GxP Compliance Patterns

This document details the structural, security, and algorithmic design choices implemented within the **Minimum Viable Architecture (MVA)** of the Multi-Omics Enterprise Platform. 

---

## 1. Global Topology & Runtime Orchestration

```
                 [ DATAOPS CI/CD ENGINE ]
                            │
                            ▼ (Static Analysis / Format Linting)
                    ┌───────────────┐
                    │GitHub Actions │
                    └───────┬───────┘
                            │
                            ▼ (Declarative Deployment State)
                 [ INFRASTRUCTURE LAYER ]
                    ┌───────────────┐
                    │ Terraform IaC │
                    └───────┬───────┘
                            │
                            ▼ (Automated Resource Provisioning)
     ┌──────────────────────┴──────────────────────┐
     ▼                                             ▼
[ STORAGE TIER ]                              [ COMPUTE TIER ]
┌──────────────────────────────┐              ┌──────────────────────────────┐
│ AWS S3 (Raw / Object Locked)  │              │ AWS ECS Container Cluster    │
│ s3://abgl-raw-data-prod      │              │ abgl-ecs-cluster-prod        │
└──────────────┬───────────────┘              └──────────────┬───────────────┘
               │                                             │
               │      ┌──────────────────────────────┐       │
               └─────►│  Nextflow Workflow Runner    │◄──────┘
                      │  (Containerized Processors)  │
                      └──────────────┬───────────────┘
                                     │
                                     ▼ (Structured Parquet Transformation)
                        [ ANALYTICAL DATA ENGINE ]
                      ┌──────────────────────────────┐
                      │    Databricks Lakehouse      │
                      │    (Delta Lake Storage)      │
                      └──────────────────────────────┘
```

The system architecture enforces a strict physical separation between the **Infrastructure Layer** (managed declaratively through HashiCorp Configuration Language), the **Compute Cluster Tier** (containerized and episodic), and the **Analytical Query Tier** (structured columnar storage engines). 

---

## 2. Granular Architectural Choices & Strategic Justifications

### A. Modular Code Segmentation (`/terraform`, `/pipelines`, `/notebooks`)
* **The Choice:** The repository structure segregates infrastructure deployment blueprints from workflow definition manifests and analytical script packages.
* **Justification:** Regulated biomedical engineering frameworks demand strict software development lifecycle (SDLC) boundaries. Isolating infrastructure modifications from daily analytical code changes ensures that pipeline logic updates do not inadvertently trigger unauthorized modifications to underlying cloud assets. This structural design aligns with target-state change management workflows observed inside enterprise biopharma teams.

### B. Declarative Infrastructure via Locked Providers (`providers.tf`)
* **The Choice:** Explicitly pinning the Terraform binary runtime version (`>= 1.5.0`) and pinning the AWS cloud provider ecosystem to a distinct major version stream (`~> 5.0`).
* **Justification:** Production pipeline environments must remain immune to external dependency variations. By blocking auto-upgrades on API providers, we ensure that changes made by cloud vendors do not introduce unvetted deprecations or breaking runtime changes. This structural isolation guarantees repeatable environment replication across worldwide research instances.

### C. Write-Once-Read-Many (WORM) Storage Immutability (`main.tf`)
* **The Choice:** Implementing `aws_s3_bucket_object_lock_configuration` configured in `COMPLIANCE` retention mode for a strict 90-day execution window on raw data ingestion targets.
* **Justification:** Clinical development platforms must ensure absolute forensic traceability. Activating cryptographic immutability at the storage layer blocks any data modification attempts—including those from root or admin accounts. This technical pattern satisfies **FDA 21 CFR Part 11** and **Good Clinical Practice (GCP)** requirements regarding electronic data trace audatibility, proving that raw genome sequencing outputs have not been altered or deleted post-ingestion.

### D. Isolated Compute Topology via Elastic Container Services (`main.tf`)
* **The Choice:** Provisioning a standalone, logically bounded `aws_ecs_cluster` to act as the primary runtime scaling tier for workflow node microservices.
* **Justification:** Biological computing pipelines run heavy, resource-intensive operations that exhibit wide performance swings. Utilizing an isolated cluster topology allows orchestrators like Nextflow to launch specific, transient container images for distinct pipeline tasks (e.g., executing a Samtools or BWA container node). This design keeps your operational codebases clean, isolates dependencies within specific environments, and enables compute clusters to auto-scale on spot nodes before tearing down completely to eliminate idle cloud spend.

### E. Cryptographic Least-Privilege Identity Binding (`main.tf`)
* **The Choice:** Constructing explicit, narrowed IAM permission structures linked via discrete role attachments, avoiding wildcard entries (`*`) on data resources.
* **Justification:** To clear global enterprise security gates at organizations like Sanofi or Novartis, architectures must prevent over-permissive resource access patterns. Restricting pipeline execution identities down to the exact Amazon Resource Names (ARNs) of the raw ingestion and processed storage buckets eliminates cross-domain risk. This strategy ensures that even a compromised compute container cannot browse adjacent enterprise data zones.

### F. Non-Provisioning DataOps Quality Gates (`tf-lint.yml`)
* **The Choice:** Building a multi-tiered GitHub Actions workflow that executes validation configurations (`terraform validate`, `terraform fmt -check`) utilizing a detached backend flag (`-backend=false`).
* **Justification:** High-performing engineering pods demand automated validation loops that do not introduce high platform costs or network bottlenecks. By running static syntax checking against the logical infrastructure model without authenticating to live cloud networks, we catch architectural design anomalies, configuration typos, or unvalidated code adjustments immediately on code commit. This strategy saves development time and prevents broken architectures from reaching deployment states.

---

## 3. Reference Grid: Technical Component Mapping

| Architectural Component | Engineering Selection | Alternative Dismissed | Rationale for Dismissal |
| :--- | :--- | :--- | :--- |
| **Infrastructure Blueprint** | Terraform (HCL) | Manual Web Console | Manual setups lack repeatability, are error-prone, and fail corporate GxP configuration audit standards. |
| **Compute Execution Context** | Amazon ECS Clusters | Persistent EC2 Nodes | Fixed servers incur heavy idle runtime costs and introduce significant software version configuration drift over time. |
| **Data Integrity Layer** | S3 WORM Object Locking | Standard IAM Deny Rules | Administrative users can bypass IAM policies; WORM configurations introduce a strict cryptographic block that cannot be overwritten. |
| **Validation Framework** | GitHub Actions Pipeline | Manual Peer Validation | Human review is slow and subjective; automated DataOps pipelines ensure strict compliance checks on every git commit. |