# Decoupled Life Sciences Nextflow Pipeline Foundry 🧬

This component provides containerized Nextflow DSL2 workflows designed for scalable, GxP-compliant processing of raw biological sequencing data.

---

## 🏗️ Architecture & Component Topology

```text
pipelines/
├── modules/
│   └── fastqc.nf            # Modular DSL2 FastQC quality control process
├── templates/
│   └── qc_summary.sh        # Process execution summary script template
├── main.nf                  # Workflow orchestration entry point
├── nextflow.config          # Engine configuration & execution profiles
└── README.md                # Pipeline module documentation
```

---

## 🚀 Execution Profiles

### 1. Local Development (`local_dev`)
Executes processes on local Docker/Podman engine without cloud infrastructure overhead:

```bash
export ENVIRONMENT=dev
nextflow run pipelines/main.nf -profile local_dev --raw_input "mock_data/*.fastq" --outdir "mock_data/out"
```

### 2. AWS Batch Managed Cloud Execution (`aws_batch`)
Submits episodic container tasks directly to AWS Batch job queues backed by SPOT compute environments provisioned via Terraform:

```bash
export ENVIRONMENT=dev
export AWS_REGION=eu-west-1
nextflow run pipelines/main.nf -profile aws_batch
```

---

## ⚙️ Configuration Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `params.environment` | `string` | `$ENVIRONMENT` / `dev` | Target deployment environment tier (`dev`, `staging`, `prod`). |
| `params.aws_region` | `string` | `$AWS_REGION` / `eu-west-1` | Target AWS region for AWS Batch queue execution. |
| `params.raw_input` | `string` | `s3://life-sciences-platform-raw-${params.environment}/*.fastq` | Input pattern path for raw sequence data. |
| `params.outdir` | `string` | `s3://life-sciences-platform-processed-${params.environment}/qc/` | Output directory destination for processed quality reports. |

---

## 🧪 Static Dry-Run & Verification

Run static validation and stub execution checks without launching real containers:

```bash
# Evaluate Nextflow runtime configuration resolution
nextflow config pipelines/main.nf -profile local_dev
nextflow config pipelines/main.nf -profile aws_batch

# Dry-run execution test using Nextflow stub mode (-stub)
mkdir -p mock_data && touch mock_data/sample_1.fastq
nextflow run pipelines/main.nf -profile local_dev -stub --raw_input "mock_data/*.fastq" --outdir "mock_data/out"
```
