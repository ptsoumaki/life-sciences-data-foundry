# Enterprise Life Sciences Data Engineering Foundry & Clinical Normalization Engine 🧬

[![DataOps CI/CD Gate](https://github.com/ptsoumaki/life-sciences-data-foundry/actions/workflows/tf-lint.yml/badge.svg)](https://github.com/ptsoumaki/life-sciences-data-foundry/actions/workflows/tf-lint.yml)
![Compliance Standard](https://img.shields.io/badge/Compliance-FDA%2021%20CFR%20Part%2011-blue)
![Data Architecture](https://img.shields.io/badge/Architecture-OMOP%20CDM%20v5.4%20%7C%20Medallion-orange)
![Storage Engine](https://img.shields.io/badge/Storage-Delta%20Lake%203.1-green)
![Python Version](https://img.shields.io/badge/Python-3.11-blue)

---

## 📋 Strategic Vision & Architecture

The **Enterprise Life Sciences Data Engineering Foundry** is a production-grade blueprint designed to solve real-world evidence (RWE) data fragmentation, multi-omics clinical trial harmonization, and regulatory compliance for **Biopharma R&D**.

* **Clinical Normalization Engine:** PySpark ETL/ELT pipelines converting heterogeneous EHR records, clinical trial observations, and multi-omics variant metadata into standard [OHDSI OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/cdm54.html) relational tables (`PERSON`, `MEASUREMENT`, `CONDITION_OCCURRENCE`).
* **Medallion Delta Lakehouse:** Multi-tier storage architecture utilizing Delta Lake ACID transactions, Liquid Clustering (`CLUSTER BY (person_id, concept_id)`), and Change Data Feed (CDF) for optimized cohort queries.
* **Programmatic Data Contracts & GxP Lineage:** Decoupled Great Expectations rule assertions ([`governance/rules.json`](governance/rules.json)) enforcing schema and quality contracts prior to Gold-tier persistence, coupled with MLflow SHA-256 cryptographic provenance tracking ([`governance/mlflow_tracker.py`](governance/mlflow_tracker.py)) and centralized crypto utilities ([`governance/crypto.py`](governance/crypto.py)) to satisfy **FDA 21 CFR Part 11**.
* **Agentic Lineage & Audit Interface:** LangGraph compliance state auditor ([`agentic-ai/graph_auditor.py`](agentic-ai/graph_auditor.py)) and FastMCP server ([`agentic-ai/mcp_server.py`](agentic-ai/mcp_server.py)) inspecting Delta Lake commit logs, MLflow run metadata, and generating automated GxP audit certificates with 21 CFR §11.50 Human-in-the-Loop review.
* **Cloud-Native IaC & Orchestration:** Databricks Asset Bundles ([`databricks.yml`](databricks.yml)) and Terraform IaC ([`terraform/`](terraform/)) provisioning immutable S3 WORM storage (`COMPLIANCE` retention mode).

---

## 🏛️ Standardization & Regulatory Compliance Matrix

| Standard / Domain | Platform Implementation | Strategic Purpose in Biopharma R&D |
| :--- | :--- | :--- |
| **OHDSI OMOP CDM v5.4** | [`analytical-layer/omop_cdm_v54/`](analytical-layer/omop_cdm_v54/) | Cross-institutional RWE analytics & standardized cohort building across global clinical networks |
| **FDA 21 CFR Part 11** | [`governance/rules.json`](governance/rules.json), [`mlflow_tracker.py`](governance/mlflow_tracker.py) & [`crypto.py`](governance/crypto.py) | Electronic records integrity, SHA-256 cryptographic run lineage & programmatic data contracts |
| **Delta Lake ACID** | [`analytical-layer/medallion/`](analytical-layer/medallion/) | Transactional reliability, schema evolution, time-travel auditing & Liquid Clustering |
| **Agentic GxP Audit & MCP** | [`agentic-ai/graph_auditor.py`](agentic-ai/graph_auditor.py) & [`mcp_server.py`](agentic-ai/mcp_server.py) | LangGraph autonomous lineage auditor with HITL sign-offs & FastMCP discovery interface |
| **AWS S3 Object Lock** | [`terraform/storage_and_compute.tf`](terraform/storage_and_compute.tf) | WORM storage enforcement preventing accidental or unauthorized clinical record deletion |

---

## 📐 Data Architecture & Medallion Topology

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
               │ (Liquid Clustering)     │
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
├── .github/              # CI/CD workflows & DataOps automated quality gates
├── agentic-ai/           # FastMCP server & LangGraph compliance state auditor
├── analytical-layer/     # PySpark OMOP CDM v5.4 normalization & Delta Lake Medallion engine
├── docs/                 # Platform documentation hub (setup, deployment, quality guides)
├── governance/           # Great Expectations contracts & MLflow SHA-256 GxP lineage tracking
├── pipelines/            # Nextflow DSL2 workflow orchestration & AWS Batch compute modules
├── scripts/              # Environment bootstrapping scripts (PowerShell & POSIX)
├── terraform/            # Cloud infrastructure IaC (AWS S3 WORM storage, KMS, IAM)
├── tests/                # Automated testing suite (PySpark unit & integration tests)
├── databricks.yml        # Databricks Asset Bundles (DABs) configuration
└── pyproject.toml        # Python build configuration & dependencies
```

---

## 📊 Architectural Decision & Trade-Off Analysis

| Component | Design Selection | Alternative Considered | Strategic Rationale |
| :--- | :--- | :--- | :--- |
| **Clinical Storage Engine** | PySpark & Delta Lake | Traditional RDBMS (Postgres) | Relational DBs bottleneck on petabyte-scale clinical/genomic join queries; Delta Lake provides ACID transactions, Liquid Clustering, and linear scaling. |
| **Schema Validation Engine** | Decoupled JSON Contracts (`rules.json`) | Inline DLT `@dlt.expect` Decorators | Decoupled contracts allow validation execution across non-Databricks orchestrators (Nextflow/Spark) without engine vendor lock-in. |
| **Clinical Standard Architecture** | OHDSI OMOP CDM v5.4 | Custom Proprietary Schemas | Custom schemas create siloed analytics; OMOP CDM enables standardized queries across global real-world evidence (RWE) networks. |
| **Compute Execution Context** | Amazon ECS & AWS Batch Spot | Persistent EC2 Nodes | Fixed servers incur heavy idle runtime costs (~70% higher) and introduce software version configuration drift over time. |
| **Data Integrity Layer** | S3 WORM Object Locking | Standard IAM Deny Rules | Administrative users can bypass IAM policies; WORM configurations introduce a strict cryptographic block that cannot be overwritten. |
| **DataOps Quality Control** | GitHub Actions Pipeline | Manual Peer Review | Human review is slow and subjective; automated DataOps pipelines ensure strict compliance checks on every git commit. |

---

## 🧪 Enterprise Quality Gates & Test Coverage

| Test Suite | Implementation | Focus Area |
| :--- | :--- | :--- |
| **Unit Tests** | `pytest tests/unit/ -v` | Validates demographics, ICD-10/SNOMED mapping, LOINC labs, and VCF parsing |
| **Integration Tests** | `pytest tests/integration/ -v` | End-to-end Medallion execution (`--mode demo` and `--mode remote`) with Delta writes |
| **GxP Governance Gate** | `python governance/mlflow_tracker.py` | Great Expectations contract validation and MLflow SHA-256 provenance tracking |
| **Static Quality Gate** | `ruff check .` & `mypy` | Strict static typing, formatting, and zero lint regressions |
| **CI/CD Pipeline** | [`.github/workflows/tf-lint.yml`](.github/workflows/tf-lint.yml) | Automated multi-job GitHub Actions gate triggered on all pull requests |

> 📖 For detailed testing instructions and coverage reporting, see [**Testing & DataOps Guide**](docs/quality/testing-and-dataops.md).

---

## 📚 Documentation Hub & Component Deep Dives

| Guide / Specification | Path | Scope & Focus |
| :--- | :--- | :--- |
| **Environment Setup** | [`docs/setup/environment-setup.md`](docs/setup/environment-setup.md) | Python 3.11 `.venv`, JDK 17, dependencies, and environment bootstrapping |
| **Cloud Deployment & IaC** | [`docs/deployment/databricks-and-iac.md`](docs/deployment/databricks-and-iac.md) | Databricks Asset Bundles (DABs), Terraform S3 WORM, and AWS Batch compute |
| **Testing & Quality Gates** | [`docs/quality/testing-and-dataops.md`](docs/quality/testing-and-dataops.md) | PySpark unit tests, integration test suites, and DataOps CI/CD workflows |
| **Analytical Layer** | [`analytical-layer/README.md`](analytical-layer/README.md) | OMOP CDM v5.4 domain packages, dynamic vocabularies, and Liquid Clustering |
| **Governance & GxP** | [`governance/README.md`](governance/README.md) | Great Expectations rules suite, MLflow 21 CFR Part 11 cryptographic auditing |
| **Workflow Pipelines** | [`pipelines/README.md`](pipelines/README.md) | Nextflow DSL2 process definitions, AWS Batch queue profiles, and FastQC |
| **Agentic AI Tier** | [`agentic-ai/README.md`](agentic-ai/README.md) | Model Context Protocol (FastMCP) server & LangGraph compliance state auditor |
| **Contribution Guide** | [`CONTRIBUTING.md`](CONTRIBUTING.md) | Git feature branching, Conventional Commits, and PR review workflow |
| **Engineering Backlog** | [`TODO.md`](TODO.md) | Active development phases and upcoming platform capabilities |
| **Security Policy** | [`SECURITY.md`](SECURITY.md) | Security controls, KMS encryption, and vulnerability disclosure |
| **Release History** | [`CHANGELOG.md`](CHANGELOG.md) | Semantic versioning changelog |
