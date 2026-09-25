# Enterprise Life Sciences Data Engineering Foundry 🧬

[![DataOps CI/CD Gate](https://github.com/ptsoumaki/life-sciences-data-foundry/actions/workflows/tf-lint.yml/badge.svg)](https://github.com/ptsoumaki/life-sciences-data-foundry/actions/workflows/tf-lint.yml)
![Version](https://img.shields.io/badge/version-0.5.0-informational)
![Compliance](https://img.shields.io/badge/Compliance-FDA%2021%20CFR%20Part%2011-blue)
![Architecture](https://img.shields.io/badge/Architecture-OMOP%20CDM%20v5.4%20%7C%20Medallion-orange)
![Storage](https://img.shields.io/badge/Storage-Delta%20Lake%203.1-green)
![Python](https://img.shields.io/badge/Python-3.11%20–%203.12-blue)
![License](https://img.shields.io/badge/License-Apache%202.0-lightgrey)

---

## Overview

A production-grade, GxP-compliant data engineering platform for Biopharma R&D — converting heterogeneous EHR records, clinical trial observations, and multi-omics variant calls into standard [OHDSI OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/cdm54.html) at scale.

| Capability | Implementation |
| :--- | :--- |
| **Clinical Normalization** | PySpark ETL/ELT producing OMOP CDM `PERSON`, `MEASUREMENT`, `CONDITION_OCCURRENCE` |
| **Medallion Delta Lakehouse** | ACID transactions, Liquid Clustering, Change Data Feed, SCD Type 1 upserts |
| **GxP Data Contracts** | Decoupled Great Expectations rules + MLflow SHA-256 provenance (FDA 21 CFR Part 11) |
| **GxP Quarantine & Remediation** | Dedicated Delta Lake dead-letter sinks, clinical failure taxonomy, batch breach gates, and idempotent replay |
| **Analytical Cohorts & Survival** | OHDSI phenotyping, HIPAA Safe Harbor de-identification, Kaplan-Meier TTE modeling, and ML-ready feature store with Charlson Comorbidity Index |
| **Multi-Omics Pipeline** | Nextflow DSL2 workflow chaining FastQC, bcftools, MultiQC, and OMOP CDM ingestion |
| **Target Discovery Lakehouse** | Target-to-phenotype evidence mart, tractability scoring, and Haldane-Anscombe phenotypic odds ratios |
| **Agentic DMTA & Audit** | LangGraph DMTA Target Steward + GxP state-graph auditor + FastMCP discovery/governance server |
| **Agent Guidelines & Skills** | Repository architecture instructions (`AGENTS.md`) and 7 GxP workspace skills (`.agents/skills/`) |
| **Cloud-Native IaC** | Databricks Asset Bundles + Terraform provisioning AWS S3 WORM (`COMPLIANCE` mode) |

> **Getting started**: see [CONTRIBUTING.md](CONTRIBUTING.md) for prerequisites, environment setup, and the local validation workflow.

---

## 🏛️ Compliance & Standardization Matrix

| Standard | Implementation | Purpose |
| :--- | :--- | :--- |
| **OHDSI OMOP CDM v5.4** | [`analytical-layer/omop_cdm_v54/`](analytical-layer/omop_cdm_v54/) | Cross-institutional RWE cohort analytics across global clinical networks |
| **FDA 21 CFR Part 11** | [`governance/rules.json`](governance/rules.json) · [`mlflow_tracker.py`](governance/mlflow_tracker.py) · [`crypto.py`](governance/crypto.py) | Electronic records integrity, SHA-256 cryptographic lineage, data contracts |
| **HIPAA Safe Harbor** | [`analytical-layer/cohorts/deid.py`](analytical-layer/cohorts/deid.py) | 45 CFR §164.514(b)(2) patient pseudonymization, date shifting, age 89+ capping, ZIP3 masking |
| **OHDSI Phenotyping & Survival** | [`analytical-layer/cohorts/builder.py`](analytical-layer/cohorts/builder.py) · [`survival.py`](analytical-layer/cohorts/survival.py) | Standard `COHORT` generation, Kaplan-Meier TTE survival curves with Greenwood SE |
| **Target Discovery & DMTA** | [`analytical-layer/discovery/`](analytical-layer/discovery/) | Target tractability, phenotypic odds ratios, ClinVar biomarker evidence mart |
| **Delta Lake ACID** | [`analytical-layer/medallion/`](analytical-layer/medallion/) | Transactional reliability, schema evolution, time-travel, Liquid Clustering |
| **GxP Quarantine & Remediation** | [`analytical-layer/medallion/quarantine.py`](analytical-layer/medallion/quarantine.py) | Dead-letter Delta sinks, ALCOA+ raw JSON preservation, batch quality gates, vocabulary replay |
| **Agentic DMTA & Audit / MCP** | [`agentic-ai/dmta_target_steward.py`](agentic-ai/dmta_target_steward.py) · [`agentic-ai/graph_auditor.py`](agentic-ai/graph_auditor.py) · [`agentic-ai/mcp_server.py`](agentic-ai/mcp_server.py) | Autonomous target feasibility triage, lineage audit with 21 CFR §11.50 sign-offs & FastMCP discovery interface |
| **AWS S3 Object Lock** | [`terraform/storage_and_compute.tf`](terraform/storage_and_compute.tf) | WORM storage preventing unauthorized deletion of clinical records |

---

## 📐 Architecture & Medallion Topology

```text
           [ DATAOPS CI/CD ENGINE ]
                    │
                    ▼  Lint · Static Analysis · Quality Gates
          ┌───────────────────┐
          │   GitHub Actions  │
          └────────┬──────────┘
                   │
                   ▼  Declarative IaC & Pipeline Trigger
      [ RAW CLINICAL & GENOMIC INGESTION ]
                   │
                   ▼
          ┌───────────────────┐
          │ Nextflow Pipeline │
          └────────┬──────────┘
                   │
                   ▼  Bronze · Raw Ingestion
          ┌───────────────────┐
          │ AWS S3 WORM /     │
          │ Delta Bronze Tier │
          └────────┬──────────┘
                   │
                   ▼  Programmatic GxP Contract Gate
          ┌───────────────────┐
          │ Great Expectations│
          │ MLflow SHA-256    │
          └────────┬──────────┘
                   │
                   ▼  Silver · OMOP CDM Normalization
          ┌───────────────────┐
          │ PySpark OMOP CDM  │
          │ v5.4 Normalizer   │
          └────────┬──────────┘
                   │
                   ▼  Gold · Liquid Clustering
          ┌───────────────────┐
          │ Delta Lake Gold   │
          │ (CLUSTER BY)      │
          └────────┬──────────┘
                   ├───────────────────────────────────┐
                   ▼                                   ▼
          ┌───────────────────┐               ┌───────────────────┐
          │ DMTA Steward /    │               │ Analytical Cohort │
          │ LangGraph Auditor │               │ & Survival Marts  │
          │ FastMCP Discovery │               │ (HIPAA De-ID/ML)  │
          └───────────────────┘               └───────────────────┘
```

---

## 📂 Repository Layout

```text
life-sciences-data-foundry/
├── .agents/              # Workspace skills & agent runbooks
├── .github/              # CI/CD workflows & automated quality gates
├── agentic-ai/           # FastMCP server, DMTA target triage steward & LangGraph GxP auditor
├── analytical-layer/     # PySpark OMOP CDM v5.4 normalization & Medallion engine
│   ├── cohorts/          # OHDSI phenotyping, HIPAA de-id, survival analysis & feature store
│   ├── discovery/        # Target discovery evidence mart & phenotypic odds ratio engine
│   ├── medallion/        # Delta Lake persistence, Liquid Clustering & quarantine engine
│   └── omop_cdm_v54/     # Domain transformers & open data connectors
├── docs/                 # Platform documentation hub
├── governance/           # Great Expectations contracts & MLflow GxP lineage tracking
├── notebooks/            # Interactive clinical & multi-omics showcase notebooks
├── pipelines/            # Nextflow DSL2 orchestration & AWS Batch compute modules
├── scripts/              # Environment bootstrapping (PowerShell & POSIX)
├── terraform/            # Cloud IaC (AWS S3 WORM, KMS, IAM)
├── tests/                # PySpark unit & integration test suites
├── AGENTS.md             # Agent guidelines & repository architecture
├── .env.example          # Environment variable template
├── databricks.yml        # Databricks Asset Bundles (DABs) configuration
└── pyproject.toml        # Python build, dependencies & tooling config
```

---

## 🧪 Quality Gates

| Gate | Command |
| :--- | :--- |
| Unit tests | `pytest tests/unit/ -v` |
| Integration tests (demo mode) | `pytest tests/integration/ -v` |
| Network integration tests (opt-in) | `LSDF_NETWORK_TESTS=1 pytest -m network` |
| Static analysis | `ruff check . && mypy --config-file pyproject.toml .` |
| Coverage | `pytest --cov --cov-report=html` |

> See [Testing & DataOps Guide](docs/quality/testing-and-dataops.md) for CI integration and coverage configuration.

---

## 📊 Architectural Decision Log

| Component | Selection | Alternative | Rationale |
| :--- | :--- | :--- | :--- |
| **Storage engine** | PySpark + Delta Lake | PostgreSQL | Petabyte-scale ACID with Liquid Clustering; relational DBs bottleneck on clinical/genomic join queries |
| **Validation** | Decoupled JSON contracts | Inline DLT `@dlt.expect` | Engine-agnostic — runs on Nextflow, AWS Batch, or local Spark without Databricks lock-in |
| **Clinical standard** | OMOP CDM v5.4 | Custom schema | OMOP enables standardized queries across global RWE networks; custom schemas silo analytics |
| **Compute** | AWS Batch Spot | Persistent EC2 | ~70% lower idle cost; no configuration drift on long-running nodes |
| **Data integrity** | S3 WORM Object Lock | IAM deny rules | IAM can be overridden by admin; WORM enforces immutability at the storage layer |
| **Quality control** | GitHub Actions | Manual review | Automated gates enforce compliance checks on every commit; human review is slow and inconsistent |

---

## 📚 Documentation

| Guide | Path |
| :--- | :--- |
| Environment Setup | [`docs/setup/environment-setup.md`](docs/setup/environment-setup.md) |
| Cloud Deployment & IaC | [`docs/deployment/databricks-and-iac.md`](docs/deployment/databricks-and-iac.md) |
| Testing & Quality Gates | [`docs/quality/testing-and-dataops.md`](docs/quality/testing-and-dataops.md) |
| Analytical Layer | [`analytical-layer/README.md`](analytical-layer/README.md) |
| Governance & GxP | [`governance/README.md`](governance/README.md) |
| Workflow Pipelines | [`pipelines/README.md`](pipelines/README.md) |
| Showcase Notebooks | [`notebooks/README.md`](notebooks/README.md) |
| Agentic AI Tier | [`agentic-ai/README.md`](agentic-ai/README.md) |
| Agent Guidelines & Skills | [`AGENTS.md`](AGENTS.md) · [`.agents/skills/`](.agents/skills/) |
| Contributing | [`CONTRIBUTING.md`](CONTRIBUTING.md) |
| Engineering Roadmap | [`ROADMAP.md`](ROADMAP.md) |
| Security Policy | [`SECURITY.md`](SECURITY.md) |
| Release History | [`CHANGELOG.md`](CHANGELOG.md) |

---

## 📄 License

Copyright © 2024–2026 Vivi Tsoumaki. Licensed under the [Apache License 2.0](LICENSE).

> This platform is a reference implementation. Validate all clinical data pipelines and GxP controls against your organisation's regulatory obligations before use in a regulated environment.
