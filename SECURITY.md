# Security Policy

## Security Measures in Place

This repository enforces the following security controls:

- **GitHub Secret Scanning**: Enabled with push protection via Terraform (`github_governance.tf`)
- **Branch Protection**: Force pushes and deletions blocked on `main`; CI status checks required
- **Sensitive Variables**: All secrets marked `sensitive = true` in Terraform and excluded from state output
- **`.env` Exclusion**: `.env` files are git-ignored; only `.env.example` templates are committed
- **S3 WORM Object Locking**: Production data is protected with `COMPLIANCE` retention mode preventing deletion or overwrite
- **KMS Encryption**: All S3 data encrypted at rest with customer-managed KMS keys with automatic rotation
