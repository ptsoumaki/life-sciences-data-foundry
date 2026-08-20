# Enterprise Life Sciences Data Engineering Foundry 🧬

[![DataOps CI/CD Gate](https://github.com/ptsoumaki/life-sciences-data-foundry/actions/workflows/tf-lint.yml/badge.svg)](https://github.com/ptsoumaki/life-sciences-data-foundry/actions/workflows/tf-lint.yml)
![Version](https://img.shields.io/badge/version-0.2.9-informational)
![Compliance](https://img.shields.io/badge/Compliance-FDA%2021%20CFR%20Part%2011-blue)
![Architecture](https://img.shields.io/badge/Architecture-OMOP%20CDM%20v5.4%20%7C%20Medallion-orange)
![Storage](https://img.shields.io/badge/Storage-Delta%20Lake%203.1-green)
![Python](https://img.shields.io/badge/Python-3.10%20–%203.12-blue)
![License](https://img.shields.io/badge/License-Apache%202.0-lightgrey)

---

## Overview

A production-grade, GxP-compliant data engineering platform for Biopharma R&D — converting heterogeneous EHR records, clinical trial observations, and multi-omics variant calls into standard [OHDSI OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/cdm54.html) at scale.

| Capability | Implementation |
| :--- | :--- |
| **Clinical Normalization** | PySpark ETL/ELT producing OMOP CDM `PERSON`, `MEASUREMENT`, `CONDITION_OCCURRENCE` |
| **Medallion Delta Lakehouse** | ACID transactions, Liquid Clustering, Change Data Feed, SCD Type 1 upserts |
| **GxP Data Contracts** | Decoupled Great Expectations rules + MLflow SHA-256 provenance (FDA 21 CFR Part 11) |
| **Agentic Compliance Audit** | LangGraph state-graph auditor + FastMCP server with HITL 21 CFR §11.50 sign-off |
| **Cloud-Native IaC** | Databricks Asset Bundles + Terraform provisioning AWS S3 WORM (`COMPLIANCE` mode) |

> **Getting started**: see [CONTRIBUTING.md](CONTRIBUTING.md) for prerequisites, environment setup, and the local validation workflow.

---

## 🏛️ Compliance & Standardization Matrix

| Standard | Implementation | Purpose |
| :--- | :--- | :--- |
| **OHDSI OMOP CDM v5.4** | [`analytical-layer/omop_cdm_v54/`](analytical-layer/omop_cdm_v54/) | Cross-institutional RWE cohort analytics across global clinical networks |
| **FDA 21 CFR Part 11** | [`governance/rules.json`](governance/rules.json) · [`mlflow_tracker.py`](governance/mlflow_tracker.py) · [`crypto.py`](governance/crypto.py) | Electronic records integrity, SHA-256 cryptographic lineage, data contracts |
| **Delta Lake ACID** | [`analytical-layer/medallion/`](analytical-layer/medallion/) | Transactional reliability, schema evolution, time-travel, Liquid Clustering |
| **Agentic GxP Audit / MCP** | [`agentic-ai/graph_auditor.py`](agentic-ai/graph_auditor.py) · [`mcp_server.py`](agentic-ai/mcp_server.py) | Autonomous lineage audit with HITL electronic sign-offs & AI discovery interface |
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
                   │
                   ▼  Agentic Audit & Discovery
          ┌───────────────────┐
          │ LangGraph Auditor │
          │ FastMCP Server    │
          └───────────────────┘
```

---

## 📂 Repository Layout

```text
life-sciences-data-foundry/
├── .github/              # CI/CD workflows & automated quality gates
├── agentic-ai/           # FastMCP server & LangGraph GxP compliance auditor
├── analytical-layer/     # PySpark OMOP CDM v5.4 normalization & Medallion engine
├── docs/                 # Platform documentation hub
├── governance/           # Great Expectations contracts & MLflow GxP lineage tracking
├── pipelines/            # Nextflow DSL2 orchestration & AWS Batch compute modules
├── scripts/              # Environment bootstrapping (PowerShell & POSIX)
├── terraform/            # Cloud IaC (AWS S3 WORM, KMS, IAM)
├── tests/                # PySpark unit & integration test suites
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
| Agentic AI Tier | [`agentic-ai/README.md`](agentic-ai/README.md) |
| Contributing | [`CONTRIBUTING.md`](CONTRIBUTING.md) |
| Engineering Backlog | [`TODO.md`](TODO.md) |
| Security Policy | [`SECURITY.md`](SECURITY.md) |
| Release History | [`CHANGELOG.md`](CHANGELOG.md) |

---

## 📄 License

Copyright © 2024–2026 Vivi Tsoumaki. Licensed under the [Apache License 2.0](LICENSE).

> This platform is a reference implementation. Validate all clinical data pipelines and GxP controls against your organisation's regulatory obligations before use in a regulated environment.
