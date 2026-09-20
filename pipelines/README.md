# Decoupled Life Sciences Nextflow Multi-Omics Pipeline Foundry 🧬

This component provides containerized Nextflow DSL2 workflows designed for scalable, GxP-compliant processing of raw sequencing data, variant annotation, cross-tool quality metrics, and seamless ingestion into OHDSI OMOP CDM v5.4 Delta Lake tables.

---

## 🏗️ Architecture & Component Topology

```text
pipelines/
├── modules/
│   ├── fastqc.nf               # Modular DSL2 FastQC sequencing quality control
│   ├── bcftools.nf             # Modular DSL2 BCFtools variant filtering & stats
│   ├── multiqc.nf              # Modular DSL2 MultiQC report aggregation
│   ├── omop_ingest.nf          # Modular DSL2 PySpark OMOP CDM MEASUREMENT ingestion
│   └── provenance.nf           # Modular DSL2 FDA 21 CFR Part 11 provenance manifest
├── templates/
│   └── qc_summary.sh           # Process execution summary script template
├── ingest_omop.py              # CLI bridge transforming VCF to OMOP CDM Delta tables
├── provenance.py               # GxP SHA-256 cryptographic manifest generator
├── multi_omics_omop.nf         # End-to-end multi-omics to OMOP workflow orchestration
├── main.nf                     # Minimal sequencing QC workflow entry point
├── nextflow.config             # Engine configuration, biocontainers & execution profiles
└── README.md                   # Pipeline documentation
```

---

## 📦 Pinned Biocontainers

All bioinformatics tools run inside pinned, immutable Docker containers from Quay.io to ensure deterministic GxP reproducibility:

| Tool | Pinned Biocontainer URI | Purpose |
| --- | --- | --- |
| **FastQC** | `quay.io/biocontainers/fastqc:0.12.1--hdfd78af_0` | Raw FASTQ sequencing quality control |
| **BCFtools** | `quay.io/biocontainers/bcftools:1.19--h8b25389_1` | VCF variant normalization, filtering & statistics |
| **MultiQC** | `quay.io/biocontainers/multiqc:1.21--pyhdfd78af_0` | Aggregate FastQC & BCFtools multi-tool QC reports |

---

## 🚀 Execution Profiles

### 1. Local Development (`local_dev`)
Executes processes locally with Docker/Podman containerization:

```bash
export ENVIRONMENT=dev
nextflow run pipelines/multi_omics_omop.nf -profile local_dev \
    --raw_fastq "mock_data/*.fastq" \
    --raw_vcf "analytical-layer/data/genomic_variants.vcf" \
    --outdir "mock_data/out"
```

### 2. AWS Batch Managed Cloud Execution (`aws_batch`)
Submits container tasks directly to AWS Batch job queues backed by SPOT compute environments provisioned via Terraform:

```bash
export ENVIRONMENT=dev
export AWS_REGION=eu-west-1
nextflow run pipelines/multi_omics_omop.nf -profile aws_batch
```

### 3. Automated Testing / Headless Stub Mode (`test`)
Executes pipeline processes using Nextflow stub mode (`-stub`) without spinning up heavy containers:

```bash
nextflow run pipelines/multi_omics_omop.nf -profile test -stub
```

---

## ⚙️ Configuration Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `params.environment` | `string` | `$ENVIRONMENT` / `dev` | Target deployment environment tier (`dev`, `staging`, `prod`). |
| `params.aws_region` | `string` | `$AWS_REGION` / `eu-west-1` | Target AWS region for AWS Batch queue execution. |
| `params.raw_fastq` | `string` | `s3://${params.raw_bucket}/*.fastq` | Input pattern path for raw sequence data. |
| `params.raw_vcf` | `string` | `s3://${params.raw_bucket}/*.vcf` | Input pattern path for raw/annotated VCF variant calls. |
| `params.outdir` | `string` | `s3://${params.processed_bucket}/multi_omics_out` | Output destination for processed artifacts. |
| `params.multiqc_dir` | `string` | `${params.outdir}/multiqc` | Output destination for MultiQC reports and data. |
| `params.manifest_path` | `string` | `${params.outdir}/provenance_manifest.json` | Path to FDA 21 CFR Part 11 execution manifest. |
| `params.data_mode` | `string` | `demo` | Ingestion mode for PySpark OMOP transformer (`demo` or `remote`). |
| `params.save_delta` | `boolean` | `true` | Whether to persist outputs into Delta Lake tables. |

---

## 🔒 GxP Audit Trail & Provenance Verification

Every execution of `pipelines/multi_omics_omop.nf` triggers `PROVENANCE_MANIFEST`, which:
1. Computes SHA-256 cryptographic hashes for all input FASTQ and VCF files.
2. Records pinned container image digests and tool versions.
3. Captures output Delta Lake commit versions from `_delta_log`.
4. Seals the execution manifest (`provenance_manifest.json`) with an overall SHA-256 digest adhering to FDA 21 CFR §11.10 and §11.50.

To verify manifest integrity programmatically:
```python
from pipelines.provenance import validate_provenance_manifest

assert validate_provenance_manifest("mock_data/out/provenance_manifest.json") is True
```
