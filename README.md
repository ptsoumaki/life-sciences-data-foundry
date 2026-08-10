# Enterprise Life Sciences Data Engineering Foundry & Clinical Normalization Engine

[![DataOps CI/CD Gate](https://github.com/ptsoumaki/life-sciences-data-foundry/actions/workflows/tf-lint.yml/badge.svg)](https://github.com/ptsoumaki/life-sciences-data-foundry/actions/workflows/tf-lint.yml)
![Compliance Standard](https://img.shields.io/badge/Compliance-FDA%2021%20CFR%20Part%2011-blue)
![Data Architecture](https://img.shields.io/badge/Architecture-OMOP%20CDM%20v5.4%20%7C%20Medallion-orange)

## 📋 Strategic Vision & Architectural Intent

This repository is a production-grade, open-source Data Engineering proof-of-concept (PoC) designed to demonstrate end-to-end data lakehouse architecture, clinical schema normalization, and GxP regulatory compliance for **Life Sciences R&D and Clinical Analytics**.

* **Clinical Normalization Engine:** PySpark ETL/ELT pipelines converting heterogeneous real-world evidence (RWE), clinical trials data, and multi-omics variant metadata into standard OHDSI OMOP CDM v5.4 relational structures (`PERSON`, `MEASUREMENT`, `CONDITION_OCCURRENCE`, `EPISODE`).
* **Medallion Delta Lakehouse:** Multi-tier storage (Bronze raw ingestion, Silver clean/normalized, Gold aggregated analytical tables) utilizing Delta Lake ACID transactions, Liquid Clustering, and Z-Ordering on core cohort keys (`person_id`, `concept_id`).
* **Programmatic Data Contracts & GxP Lineage:** Decoupled Great Expectations rules (`governance/rules.json`) enforcing schema and quality contracts prior to Gold-tier write streams, paired with MLflow SHA-256 cryptographic run lineage auditing (`governance/mlflow_tracker.py`) to satisfy FDA 21 CFR Part 11 compliance.
* **Agentic Lineage & Audit Interface:** A Model Context Protocol (FastMCP) server running LangGraph multi-agent loops (`agentic-ai/`) to inspect Delta Lake transaction logs (`_delta_log/`), MLflow run metadata, and vocabulary mapping trees.
* **Workflow Orchestration & Background IaC:** Nextflow DSL2 process execution profiles backed by background Terraform modules provisioning immutable S3 WORM storage (`COMPLIANCE` retention mode).

---

## 🏛️ Standardization & Regulatory Compliance Matrix

| Standard / Domain | Platform Implementation | Strategic Purpose in Biopharma R&D |
| :--- | :--- | :--- |
| **OHDSI OMOP CDM v5.4** | `analytical-layer/omop_mapping.py` | Cross-institutional RWE analytics & standardized cohort building across clinical networks |
| **FDA 21 CFR Part 11** | `governance/rules.json` & `mlflow_tracker.py` | Electronic records integrity, SHA-256 cryptographic run lineage & programmatic data contracts |
| **Delta Lake ACID** | `analytical-layer/` | Transactional reliability, schema evolution, time-travel auditing & Liquid Clustering |
| **Model Context Protocol** | `agentic-ai/mcp_server.py` | FastMCP agentic interface for querying OMOP concept hierarchies & execution state |

---

## 🛠️ Data Engineering & Infrastructure Toolchain

To run the analytical pipelines, validation suites, and IaC deployment engines, ensure the following core tools are configured:

| Tool | Required Version | Strategic Purpose |
| --- | --- | --- |
| **Python** | `>=3.10, <3.12` | Core PySpark transformations, MLflow lineage tracking, and data contract engines |
| **Apache Spark / PySpark** | `>=3.5.0` | Distributed data engine for OMOP CDM mapping and Delta Lake persistence |
| **Delta Lake** | `>=3.1.0` | ACID storage engine enforcing Liquid Clustering and Z-Ordering |
| **Nextflow** | `>=23.04.0` | Episodic containerized workflow orchestration for data pipeline execution |
| **Java / JDK** | `>=11` (17 recommended) | Required JVM execution engine for Apache Spark and Nextflow runtimes |
| **Terraform** | `>=1.5.0` | Declarative IaC toolchain provisioning background AWS S3 WORM & Batch infrastructure |
| **Docker Engine** | Latest Stable | Container execution context for Nextflow processes |
| **AWS CLI** | `v2` | AWS infrastructure authentication and operational management |

---

## 🏗️ Platform Status & Engineering Roadmap

- [x] **Phase 1: Medallion Storage & Infrastructure Core**
  - Declarative Terraform IaC infrastructure with cryptographic S3 WORM Object Locking (`COMPLIANCE` retention mode in `prod`).
  - Isolated Amazon ECS compute cluster topology for episodic, containerized workflow execution.
  - Automated DataOps CI/CD linting gate (`tf-lint.yml`) validating HCL syntax and Nextflow configurations.
- [x] **Phase 2: GxP Governance & Data Integrity Gates**
  - Programmatic data quality suite using Great Expectations (`governance/rules.json`) enforcing FDA 21 CFR Part 11 electronic records integrity.
  - Automated execution lineage, SHA-256 cryptographic file tracking, and metric logging via MLflow (`governance/mlflow_tracker.py`).
- [x] **Phase 3: Base Clinical Normalization Ring (OMOP CDM)**
  - Databricks PySpark semantic mapping scripts (`analytical-layer/omop_mapping.py`) translating unstructured genomic and clinical fields into standard OHDSI OMOP CDM v5.4 `PERSON` and `MEASUREMENT` structures.
- [ ] **Phase 4: Data Lakehouse & Clinical Normalization Engine** (🚀 *Active Sprint*)
  - Refactor monolithic `omop_mapping.py` into modular domain packages (`analytical-layer/omop_cdm_v54/` with `person.py`, `measurement.py`, `condition_occurrence.py`, `genomic_variants.py`).
  - PySpark write streams with Delta Lake Liquid Clustering (`CLUSTER BY (person_id, concept_id)`) and schema evolution.
  - Runtime assertion hooks connecting Great Expectations rules (`governance/rules.json`) directly to Silver-to-Gold tier persistence.
- [ ] **Phase 5: Agentic Lineage & MLOps Infrastructure** (📅 *Planned*)
  - LangGraph state graph evaluator (`agentic-ai/graph_auditor.py`) auditing MLflow lineage trees (`governance/mlflow_tracker.py`) and Delta Lake transaction commit logs (`_delta_log/`) against GxP regulatory parameters.
  - Model Context Protocol (MCP) clinical server (`agentic-ai/mcp_server.py`) exposing FastMCP tools for OMOP CDM concept hierarchies and pipeline state.
- [ ] **Phase 6: Automated DataOps & Quality Engineering** (📋 *Backlog*)
  - Integration test suite (`tests/`) with `pytest` / `pytest-spark` covering OMOP mapping, Delta Lake schema enforcement, and MLflow tracking.
  - DataOps CI workflow expansion (`.github/workflows/tf-lint.yml`) running `ruff`, `mypy`, and `pytest` on PRs.

> 💡 For detailed upcoming tasks, security hardening items, and component backlogs, see **[TODO.md](TODO.md)**.

---

## 📐 Data Architecture & Topology

```text
                 [ DATAOPS CI/CD ENGINE ]
                            │
                            ▼ (Linting, Static Analysis & Code Quality)
                    ┌───────────────┐
                    │GitHub Actions │
                    └───────┬───────┘
                            │
                            ▼ (Declarative IaC & Pipeline Trigger)
               [ RAW CLINICAL & GENOMIC INGESTION ]
                            │
                            ▼
               ┌─────────────────────────┐
               │    Nextflow Pipeline    │
               │   Orchestration Engine  │
               └────────────┬────────────┘
                            │
                            ▼ (Bronze Tier Raw Ingestion Storage)
               ┌─────────────────────────┐
               │  AWS S3 WORM / Delta    │
               │       Bronze Tier       │
               └────────────┬────────────┘
                            │
                            ▼ (Programmatic Data Contract Gate)
               [ DATA GOVERNANCE GATEWAY ]
               ┌─────────────────────────┐
               │ Great Expectations &    │
               │ MLflow SHA-256 Lineage  │
               └────────────┬────────────┘
                            │
                            ▼ (Silver Tier OMOP CDM Normalization)
               [ CLINICAL ANALYTICAL ENGINE ]
               ┌─────────────────────────┐
               │  PySpark OMOP CDM v5.4  │
               │  Normalization Engine   │
               └────────────┬────────────┘
                            │
                            ▼ (Gold Tier Performance Optimized)
               ┌─────────────────────────┐
               │   Delta Lake Gold Tier  │
               │ (Z-Order / Clustering)  │
               └────────────┬────────────┘
                            │
                            ▼ (Agentic Audit & Discovery Interface)
               [ AGENTIC INTELLIGENCE LAYER ]
               ┌─────────────────────────┐
               │  LangGraph MCP Server   │
               │ (Data Lineage Auditor)  │
               └─────────────────────────┘
```

---

## 📂 Repository Layout

```text
life-sciences-data-foundry/
├── .github/
│   └── workflows/
│       └── tf-lint.yml               # DataOps CI/CD linting & static analysis gate
├── agentic-ai/                       # AGENTIC AUDIT INTERFACE: FastMCP & LangGraph Engine
│   ├── graph_auditor.py              # LangGraph compliance multi-agent state loops
│   ├── mcp_server.py                 # Model Context Protocol (MCP) clinical audit server
│   └── README.md                     # Agentic AI architecture specification
├── analytical-layer/                 # PRIMARY ENGINE: Analytical Data Engineering
│   ├── omop_mapping.py               # PySpark clinical normalization to OMOP CDM v5.4
│   └── README.md                     # Analytical layer architecture specification
├── governance/                       # DATA GOVERNANCE & GxP COMPLIANCE LAYER
│   ├── mlflow_tracker.py             # MLflow lineage logging & SHA-256 audit tracking
│   ├── rules.json                    # Great Expectations GxP clinical validation suite
│   ├── sample_clinical.csv           # Synthetic OMOP CDM v5.4 test dataset
│   └── README.md                     # Governance & quality layer specification
├── pipelines/                        # WORKFLOW ORCHESTRATION LAYER
│   ├── modules/
│   │   └── fastqc.nf                 # Modular Nextflow DSL2 process definitions
│   ├── templates/
│   │   └── qc_summary.sh             # Workflow execution script template
│   ├── main.nf                       # Nextflow orchestration execution entry point
│   ├── nextflow.config               # Engine runtime configuration
│   └── README.md                     # Pipeline module specification
├── scripts/                          # ENVIRONMENT BOOTSTRAP SCRIPTS
│   ├── bootstrap.ps1                 # Windows PowerShell environment initializer
│   └── bootstrap.sh                  # POSIX shell environment initializer
├── terraform/                        # [BACKGROUND UTILITY: Supporting Cloud Infrastructure]
│   ├── github_governance.tf          # GitHub repo governance, branch protection, & envs
│   ├── main.tf                       # Root module entry point & account/region discovery
│   ├── providers.tf                  # AWS/GitHub provider settings & default tags
│   ├── storage_and_compute.tf        # S3 WORM storage, KMS encryption, & AWS Batch topology
│   ├── variables.tf                  # Environment variable validations & defaults
│   └── terraform.tfvars.example      # Example environment inputs template
├── .env.example                      # Environment variable template
├── .gitignore                        # Git exclusion rules
├── CHANGELOG.md                      # Platform version release history
├── CONTRIBUTING.md                   # Development workflow & commit standards
├── LICENSE                           # Repository license
├── pyproject.toml                    # Python build backend & project dependencies
├── README.md                         # Main platform blueprint specification
├── SECURITY.md                       # Security controls & disclosure policy
└── TODO.md                           # Platform engineering backlog & TODO checklist
```

---

## 📚 Component Specifications & Documentation

| Component | Path | Focus Area |
| --- | --- | --- |
| **Analytical Engine** | [analytical-layer/README.md](analytical-layer/README.md) | PySpark Medallion Delta Lake pipeline & OMOP CDM v5.4 mapping |
| **Governance & GxP** | [governance/README.md](governance/README.md) | Great Expectations rules, MLflow lineage tracking, & synthetic data |
| **Nextflow Pipelines** | [pipelines/README.md](pipelines/README.md) | DSL2 process modules, AWS Batch queue targeting, & execution profiles |
| **Agentic Intelligence**| [agentic-ai/README.md](agentic-ai/README.md) | LangGraph multi-agent compliance auditor & MCP server interface |
| **Contribution Guide** | [CONTRIBUTING.md](CONTRIBUTING.md) | Feature branching, conventional commit standards, & PR rules |
| **Release History** | [CHANGELOG.md](CHANGELOG.md) | Semantic versioning release history |
| **Engineering Backlog**| [TODO.md](TODO.md) | Upcoming Phase 4/5 tasks and GxP hardening items |

---

## 📊 Architectural Trade-Off Analysis

| Architectural Component | Data Strategy Selection | Alternative Dismissed | Strategic Rationale for Selection / Rejection |
| --- | --- | --- | --- |
| **Clinical Storage Engine** | PySpark & Delta Lake | Traditional RDBMS (Postgres) | Relational DBs bottleneck on petabyte-scale clinical/genomic join queries; Delta Lake provides ACID transactions, Z-Ordering, and linear scaling. |
| **Schema Validation Engine** | Decoupled JSON Contracts (`rules.json`) | Inline DLT `@dlt.expect` Decorators | Decoupled contracts allow validation execution across non-Databricks orchestrators (Nextflow/Spark) without engine vendor lock-in. |
| **Clinical Standard Architecture** | OHDSI OMOP CDM v5.4 | Custom Proprietary Schemas | Custom schemas create siloed analytics; OMOP CDM enables standardized queries across global real-world evidence (RWE) networks. |
| **Compute Execution Context** | Amazon ECS & AWS Batch Spot | Persistent EC2 Nodes | Fixed servers incur heavy idle runtime costs (~70% higher) and introduce software version configuration drift over time. |
| **Data Integrity Layer** | S3 WORM Object Locking | Standard IAM Deny Rules | Administrative users can bypass IAM policies; WORM configurations introduce a strict cryptographic block that cannot be overwritten. |
| **DataOps Quality Control** | GitHub Actions Pipeline | Manual Peer Review | Human review is slow and subjective; automated DataOps pipelines ensure strict compliance checks on every git commit. |

---

## ⚙️ Execution & Pipeline Operations

Initialize runtime parameters and trigger core data engineering workflows:

```bash
# Copy template and configure target parameters
cp .env.example .env

# Option A: Load environment on POSIX (Linux/macOS)
source scripts/bootstrap.sh

# Option B: Load environment on PowerShell (Windows)
.\scripts\bootstrap.ps1

# 1. Run GxP Data Quality Contract & Lineage Audit Gate
python governance/mlflow_tracker.py

# 2. Run PySpark OMOP CDM v5.4 Clinical Normalization Engine
python analytical-layer/omop_mapping.py

# 3. Run Nextflow Dry-Run Pipeline Orchestration
nextflow run pipelines/main.nf -profile local_dev -stub

# 4. Run Terraform IaC Background Infrastructure Validation
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform validate
```

### Required CI Secrets

Configure `ENVIRONMENT` (`dev`, `staging`, `prod`) and `AWS_REGION` (`eu-west-1` or target region) in GitHub Repository Secrets. The workflow in `.github/workflows/tf-lint.yml` utilizes these or fallback defaults to run non-interactive static validation.
