# Changelog

All notable changes to the Life Sciences Platform Blueprint are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- CONTRIBUTING.md with branching strategy, conventional commit specifications, and development setup
- CHANGELOG.md following Keep a Changelog format
- SECURITY.md repository security controls documentation
- Component-level README.md specifications for `governance/`, `analytical-layer/`, and `agentic-ai/`
- Prerequisites & Toolchain section in main README.md with required software versions
- DataOps CI/CD GitHub Actions status badge in main README.md
- Enhanced `.env.example` documentation with phase-by-phase variable integration guidance

### Changed
- Updated repository layout tree in README.md to reflect current platform component structure

---

## [0.2.0] - 2026-08-09

### Added
- Terraform GitHub governance module (`github_governance.tf`) with branch protection, secret scanning, and deployment environments
- Great Expectations GxP clinical validation suite (`governance/rules.json`) enforcing FDA 21 CFR Part 11
- MLflow lineage tracker (`governance/mlflow_tracker.py`) with SHA-256 cryptographic file hashing and multi-format dataset support
- OHDSI OMOP CDM v5.4 PySpark Medallion normalization pipeline (`analytical-layer/omop_mapping.py`) mapping `PERSON` and `MEASUREMENT` tables
- Nextflow DSL2 FastQC pipeline (`pipelines/main.nf`) with modular processes (`pipelines/modules/fastqc.nf`) and `aws_batch` queue targeting
- Hardened PowerShell and Bash bootstrap scripts (`scripts/bootstrap.ps1`, `scripts/bootstrap.sh`) with auto-template fallback
- `pyproject.toml` with Python dependency specifications

---

## [0.1.0] - 2026-05-27

### Added
- Initial Terraform IaC infrastructure with S3 WORM Object Locking and KMS encryption
- AWS ECS cluster and Batch compute environment with SPOT capacity optimization
- IAM execution roles and governance bypass policies for non-production tiers
- GitHub Actions CI/CD linting gate (`tf-lint.yml`) for HCL and Nextflow validation
- `.env.example` environment configuration template
