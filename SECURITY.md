# Security Policy

## Implemented Security Controls

| Control | Implementation |
| :--- | :--- |
| **Secret scanning** | GitHub push protection enabled via Terraform (`github_governance.tf`) |
| **Branch protection** | Force pushes and deletions blocked on `main`; CI status checks required before merge |
| **Sensitive variables** | All secrets marked `sensitive = true` in Terraform; excluded from state output |
| **`.env` exclusion** | `.env` git-ignored; only `.env.example` template is committed |
| **S3 WORM Object Lock** | Production data protected with `COMPLIANCE` retention mode |
| **KMS encryption** | All S3 data encrypted at rest with customer-managed KMS keys (automatic rotation) |
| **Cryptographic provenance** | SHA-256 digest tracking on all pipeline artefacts via `governance/crypto.py` |
| **Supply chain hardening** | GitHub Actions steps pinned to immutable commit SHAs |

---

## Reporting a Vulnerability

Report security issues via [GitHub Private Security Advisories](https://github.com/ptsoumaki/life-sciences-data-foundry/security/advisories/new). Do not open a public issue.
