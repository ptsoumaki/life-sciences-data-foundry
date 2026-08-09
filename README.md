# Enterprise Life Sciences Platform Blueprint 🏗️ (WIP)

[![DataOps CI/CD Gate](https://github.com/ptsoumaki/life-sciences-platform-blueprint/actions/workflows/tf-lint.yml/badge.svg)](https://github.com/ptsoumaki/life-sciences-platform-blueprint/actions/workflows/tf-lint.yml)

## 📋 Strategic Vision
This repository functions as an enterprise-grade, integrated data foundry designed to transform high-throughput raw biological data and real-world clinical records into queryable, GxP-compliant relational datasets.

---

## 🛠️ Prerequisites & Development Toolchain

To work with this platform blueprint, ensure the following core toolchains are installed:

| Tool | Required Version | Purpose |
| --- | --- | --- |
| **Python** | `>=3.10, <3.12` | Governance validation, lineage tracking, and PySpark OMOP CDM mapping |
| **Terraform** | `>=1.5.0` | Declarative IaC infrastructure provisioning |
| **Nextflow** | `>=23.04.0` | Episodic containerized workflow orchestration |
| **Docker Engine** | Latest Stable | Container execution context for Nextflow processes |
| **AWS CLI** | `v2` | AWS infrastructure authentication and operations |

---

## 🏗️ Platform Status & Engineering Roadmap

- [x] **Phase 1: Core Compute & Storage Foundry**
  - Declarative Terraform IaC infrastructure with cryptographic S3 WORM Object Locking (`COMPLIANCE` retention mode in `prod`).
  - Isolated Amazon ECS compute cluster topology for episodic, containerized workflow execution.
  - Conditional IAM governance bypass policy for development and staging tiers.
  - Automated DataOps CI/CD linting gate (`tf-lint.yml`) validating HCL syntax and Nextflow configurations.
- [x] **Phase 2: GxP Governance & Ingestion Integrity Gates**
  - Programmatic data quality suite using Great Expectations (`governance/rules.json`) enforcing FDA 21 CFR Part 11 electronic records integrity.
  - Automated execution lineage, SHA-256 cryptographic file tracking, and metric logging via MLflow (`governance/mlflow_tracker.py`).
- [x] **Phase 3: Clinical Normalization Ring (OMOP CDM)**
  - Databricks PySpark semantic mapping scripts (`analytical-layer/omop_mapping.py`) translating unstructured genomic and clinical fields into standard OHDSI OMOP CDM v5.4 structures.
- [ ] **Phase 4: Agentic Compliance Auditing Tier** (📅 *Planned*)
  - Model Context Protocol (MCP) server running LangGraph multi-agent loops to validate configurations and lineage against FDA regulatory parameters.

---

## 📐 Platform Architecture & Topology

```text
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
│ AWS S3 (Raw / Object Locked) │              │ AWS ECS Container Cluster    │
│ life-sciences-platform-raw-* │              │ life-sciences-platform-ecs-* │
└──────────────┬───────────────┘              └──────────────┬───────────────┘
               │                                             │
               │      ┌──────────────────────────────┐       │
               └─────►│  Nextflow Workflow Runner    │◄──────┘
                      │  (Containerized Processors)  │
                      └──────────────┬───────────────┘
                                     │
                                     ▼ (Data Integrity Runtime Gate)
                        [ GOVERNANCE GATEWAY ]
                      ┌──────────────────────────────┐
                      │  Great Expectations & MLflow │
                      │  (rules.json / tracker.py)   │
                      └──────────────┬───────────────┘
                                     │
                                     ▼ (OMOP CDM v5.4 Normalization)
                        [ ANALYTICAL DATA ENGINE ]
                      ┌──────────────────────────────┐
                      │    Databricks Lakehouse      │
                      │  (omop_mapping.py / Delta)   │
                      └──────────────┬───────────────┘
                                     │
                                     ▼ (Compliance & Clinical Agent)
                        [ AGENTIC INTELLIGENCE ]
                      ┌──────────────────────────────┐
                      │  LangGraph MCP Multi-Agent   │
                      │  (graph_auditor / mcp_server)│
                      └──────────────┬───────────────┘

```

---

## 📂 Repository Layout

```text
life-sciences-platform-blueprint/
├── .github/
│   └── workflows/
│       └── tf-lint.yml               # DataOps CI/CD linting & syntax gate
├── agentic-ai/
│   ├── graph_auditor.py              # LangGraph compliance multi-agent state loops
│   ├── mcp_server.py                 # Model Context Protocol (MCP) audit server
│   └── README.md                     # Agentic AI architecture specification
├── analytical-layer/
│   ├── omop_mapping.py               # PySpark clinical normalization to OMOP CDM v5.4
│   └── README.md                     # Analytical layer architecture specification
├── governance/
│   ├── mlflow_tracker.py             # MLflow lineage logging & SHA-256 audit tracking
│   ├── rules.json                    # Great Expectations GxP clinical validation suite
│   ├── sample_clinical.csv           # Synthetic OMOP CDM v5.4 test dataset
│   └── README.md                     # Governance & quality layer specification
├── pipelines/
│   ├── modules/                      # Modular Nextflow DSL2 process definitions
│   ├── templates/                    # Workflow execution script templates
│   ├── main.nf                       # Nextflow orchestration execution entry point
│   ├── nextflow.config               # Engine runtime configuration
│   └── README.md                     # Pipeline module specification
├── scripts/
│   ├── bootstrap.ps1                 # Windows PowerShell environment initializer
│   └── bootstrap.sh                  # POSIX shell environment initializer
├── terraform/
│   ├── main.tf                       # Storage, compute, WORM, and IAM resources
│   ├── providers.tf                  # AWS provider settings & default tags
│   ├── variables.tf                  # Environment variable validations
│   └── terraform.tfvars.example      # Example environment inputs template
├── .env                              # Active environment overrides
├── .env.example                      # Environment variable template
├── .gitignore                        # Git exclusion rules
├── CHANGELOG.md                      # Platform version release history
├── CONTRIBUTING.md                   # Development workflow & commit standards
├── LICENSE                           # Repository license
├── README.md                         # Main platform blueprint specification
└── SECURITY.md                       # Security controls & disclosure policy
```

---

## 📊 Architectural Trade-Off Analysis

| Architectural Component | Engineering Selection | Alternative Dismissed | Strategic Rationale for Selection / Rejection |
| --- | --- | --- | --- |
| **Infrastructure Blueprint** | Terraform (HCL) | Manual Web Console | Manual setups lack repeatability, are error-prone, and fail corporate GxP configuration audit standards. |
| **Compute Execution Context** | Amazon ECS Clusters | Persistent EC2 Nodes | Fixed servers incur heavy idle runtime costs and introduce significant software version configuration drift over time. |
| **Data Integrity Layer** | S3 WORM Object Locking | Standard IAM Deny Rules | Administrative users can bypass IAM policies; WORM configurations introduce a strict cryptographic block that cannot be overwritten. |
| **Schema Validation Engine** | Decoupled JSON Expectations (`rules.json`) | Inline DLT `@dlt.expect` Decorators | Decoupled JSON allows validation execution across non-Databricks orchestrators (Nextflow/AWS Batch) without engine vendor lock-in. |
| **DataOps Quality Control** | GitHub Actions Pipeline | Manual Peer Review | Human review is slow and subjective; automated DataOps pipelines ensure strict compliance checks on every git commit. |

---

## ⚙️ Configuration & Environment Synchronization

Synchronize runtime parameters across Terraform, Nextflow, and local execution environments using `.env`:

```bash
# Copy template and configure target parameters
cp .env.example .env

# Option A: Load environment on POSIX (Linux/macOS)
source scripts/bootstrap.sh

# Option B: Load environment on PowerShell (Windows)
.\scripts\bootstrap.ps1

# Deploy Infrastructure
cd terraform
terraform init
terraform plan
terraform apply

# Run Nextflow Orchestration
cd ..
nextflow run pipelines/main.nf -c pipelines/nextflow.config
```

### Required CI Secrets

Configure `ENVIRONMENT` (`dev`, `staging`, `prod`) and `AWS_REGION` (`eu-west-1` or target region) in GitHub Repository Secrets. The workflow in `.github/workflows/tf-lint.yml` utilizes these or fallback defaults to run non-interactive static validation.
