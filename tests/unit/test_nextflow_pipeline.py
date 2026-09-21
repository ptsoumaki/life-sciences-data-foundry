"""Unit tests for Nextflow Multi-Omics to OMOP pipeline and GxP provenance generator."""

import json
import re
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pipelines.ingest_omop import ingest_vcf_to_omop
from pipelines.provenance import (
    DEFAULT_CONTAINERS,
    generate_provenance_manifest,
    resolve_default_pipeline_version,
    validate_provenance_manifest,
)


def test_generate_provenance_manifest_basic(tmp_path: Path):
    """Test generating a valid provenance manifest for input files."""
    # Create mock input files
    fastq_file = tmp_path / "sample_R1.fastq"
    fastq_file.write_text("@SEQ_ID_1\nGATCGATC\n+\n!''*((((***\n", encoding="utf-8")

    vcf_file = tmp_path / "sample_variants.vcf"
    vcf_file.write_text(
        "##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\nchr1\t100\trs1\tA\tT\n", encoding="utf-8"
    )

    summary_file = tmp_path / "ingestion_summary.json"
    summary_data = {
        "status": "SUCCESS",
        "variant_count": 1,
        "omop_measurement_count": 1,
    }
    summary_file.write_text(json.dumps(summary_data), encoding="utf-8")

    manifest_out = tmp_path / "manifest.json"

    manifest = generate_provenance_manifest(
        input_files=[str(fastq_file), str(vcf_file)],
        output_manifest_path=str(manifest_out),
        pipeline_version="0.4.0",
        workflow_session_id="test-session-123",
        summary_file=str(summary_file),
    )

    assert manifest["pipeline_name"] == "life-sciences-data-foundry-pipeline"
    assert manifest["pipeline_version"] == "0.4.0"
    assert manifest["workflow_session_id"] == "test-session-123"
    assert manifest["compliance"]["regulatory_standard"] == "FDA 21 CFR Part 11 (§11.10, §11.50)"
    assert manifest["compliance"]["hash_algorithm"] == "SHA-256"
    assert len(manifest["inputs"]) == 2
    assert manifest["inputs"][0]["filename"] == "sample_R1.fastq"
    assert len(manifest["inputs"][0]["sha256"]) == 64
    assert manifest["ingestion_summary"]["variant_count"] == 1
    assert "manifest_sha256" in manifest
    assert len(manifest["manifest_sha256"]) == 64

    # Verify manifest file on disk
    assert manifest_out.exists()
    assert validate_provenance_manifest(str(manifest_out)) is True


def test_resolve_default_pipeline_version():
    """Verifies that resolve_default_pipeline_version dynamically parses nextflow.config."""
    resolved = resolve_default_pipeline_version()
    assert resolved == "0.4.0"


def test_generate_provenance_manifest_default_version(tmp_path: Path):
    """Test generating manifest without specifying pipeline_version defaults dynamically to nextflow.config."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("dummy", encoding="utf-8")
    out_file = tmp_path / "manifest.json"

    manifest = generate_provenance_manifest(
        input_files=[str(test_file)],
        output_manifest_path=str(out_file),
    )
    assert manifest["pipeline_version"] == "0.4.0"


def test_validate_provenance_manifest_tampering(tmp_path: Path):
    """Test that tampering with any field invalidates the manifest seal."""
    input_file = tmp_path / "test.txt"
    input_file.write_text("sample content", encoding="utf-8")

    manifest_out = tmp_path / "manifest.json"
    generate_provenance_manifest(
        input_files=[str(input_file)],
        output_manifest_path=str(manifest_out),
    )

    # Initial check passes
    assert validate_provenance_manifest(str(manifest_out)) is True

    # Tamper with file
    with open(manifest_out, encoding="utf-8") as f:
        data = json.load(f)
    data["pipeline_version"] = "0.9.9-malicious"
    with open(manifest_out, "w", encoding="utf-8") as f:
        json.dump(data, f)

    assert validate_provenance_manifest(str(manifest_out)) is False


def test_validate_provenance_manifest_nonexistent():
    """Test validation on non-existent path returns False."""
    assert validate_provenance_manifest("/nonexistent/path/manifest.json") is False


def test_nextflow_config_biocontainers_and_profiles():
    """Test that nextflow.config contains required pinned biocontainers and execution profiles."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    config_path = repo_root / "pipelines" / "nextflow.config"
    assert config_path.exists(), "nextflow.config must exist"

    config_text = config_path.read_text(encoding="utf-8")

    # Verify pinned biocontainer images
    for tool, container in DEFAULT_CONTAINERS.items():
        assert container in config_text, (
            f"nextflow.config must pin container for {tool}: {container}"
        )

    # Verify execution profiles
    assert "local_dev" in config_text
    assert "aws_batch" in config_text
    assert "test" in config_text

    # Verify parameters
    assert "params.raw_fastq" in config_text
    assert "params.raw_vcf" in config_text
    assert "params.outdir" in config_text
    assert "params.multiqc_dir" in config_text


def test_pipeline_dsl2_files_exist():
    """Test that all required DSL2 workflow and module files exist and enable DSL2."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    pipelines_dir = repo_root / "pipelines"

    required_files = [
        pipelines_dir / "multi_omics_omop.nf",
        pipelines_dir / "modules" / "fastqc.nf",
        pipelines_dir / "modules" / "bcftools.nf",
        pipelines_dir / "modules" / "multiqc.nf",
        pipelines_dir / "modules" / "omop_ingest.nf",
        pipelines_dir / "modules" / "provenance.nf",
    ]

    for file_path in required_files:
        assert file_path.exists(), f"Required file missing: {file_path}"
        content = file_path.read_text(encoding="utf-8")
        assert "nextflow.enable.dsl=2" in content, f"{file_path.name} must enable Nextflow DSL2"


def test_ingest_vcf_to_omop(tmp_path: Path, spark: SparkSession):
    """Test end-to-end VCF ingestion into OMOP MEASUREMENT and Delta tables."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    vcf_path = repo_root / "analytical-layer" / "data" / "genomic_variants.vcf"
    assert vcf_path.exists(), "Sample genomic_variants.vcf must exist"

    delta_out = tmp_path / "delta"
    summary_out = tmp_path / "ingestion_summary.json"

    summary = ingest_vcf_to_omop(
        vcf_path=str(vcf_path),
        output_dir=str(delta_out),
        mode="demo",
        summary_out=str(summary_out),
        spark=spark,
    )

    assert summary["status"] == "SUCCESS"
    assert summary["variant_count"] == 5
    assert summary["omop_measurement_count"] == 5
    assert summary["write_mode"] == "append"
    assert len(summary["vcf_sha256"]) == 64
    assert summary_out.exists()
    assert Path(summary["silver_table_path"]).exists()
    assert Path(summary["gold_table_path"]).exists()


def test_provenance_manifest_delta_version_resolution(tmp_path: Path):
    """Test Delta log version extraction from table folder and summary fallback."""
    # 1. Test resolution from table folder containing _delta_log
    table_dir = tmp_path / "gold_measurement"
    delta_log = table_dir / "_delta_log"
    delta_log.mkdir(parents=True)
    (delta_log / "00000000000000000000.json").write_text("{}", encoding="utf-8")
    (delta_log / "00000000000000000003.json").write_text("{}", encoding="utf-8")

    manifest = generate_provenance_manifest(
        input_files=[],
        output_manifest_path=str(tmp_path / "m1.json"),
        delta_log_dir=str(table_dir),
    )
    assert manifest["target_delta_version"] == 3

    # 2. Test resolution fallback from summary_file
    summary_file = tmp_path / "summary.json"
    summary_file.write_text(json.dumps({"target_delta_version": 7}), encoding="utf-8")

    manifest2 = generate_provenance_manifest(
        input_files=[],
        output_manifest_path=str(tmp_path / "m2.json"),
        summary_file=str(summary_file),
    )
    assert manifest2["target_delta_version"] == 7


def test_provenance_stub_manifest_validity():
    """Test that the stub manifest in provenance.nf is cryptographically valid."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    provenance_nf = repo_root / "pipelines" / "modules" / "provenance.nf"
    assert provenance_nf.exists()

    content = provenance_nf.read_text(encoding="utf-8")
    # Extract JSON between cat << 'EOF' > provenance_manifest.json and EOF
    match = re.search(
        r"cat << 'EOF' > provenance_manifest\.json\s*(\{.*?\})\s*EOF", content, re.DOTALL
    )
    assert match is not None, "Stub manifest block not found in provenance.nf"

    stub_json = json.loads(match.group(1))

    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as f:
        json.dump(stub_json, f, indent=2)
        temp_path = f.name

    assert validate_provenance_manifest(temp_path) is True


def test_sample_fastq_exists():
    """Test that the sample FASTQ file exists and contains valid 4-line records."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    fastq_path = repo_root / "analytical-layer" / "data" / "sample.fastq"
    assert fastq_path.exists(), "sample.fastq must exist in analytical-layer/data"

    lines = [
        line.strip() for line in fastq_path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]
    assert len(lines) % 4 == 0, "FASTQ must consist of 4-line records"
    assert lines[0].startswith("@"), "First line must start with @"
    assert lines[2].startswith("+"), "Third line must start with +"


def test_provenance_manifest_multi_summary_resolution(tmp_path: Path):
    """Test that multiple ingestion summaries correctly resolve the latest Delta version."""
    s1 = tmp_path / "summary_1.json"
    s1.write_text(
        json.dumps(
            {
                "status": "SUCCESS",
                "target_delta_version": 2,
                "gold_table_path": str(tmp_path / "table1"),
            }
        ),
        encoding="utf-8",
    )

    s2 = tmp_path / "summary_2.json"
    s2.write_text(
        json.dumps(
            {
                "status": "SUCCESS",
                "target_delta_version": 5,
                "gold_table_path": str(tmp_path / "table2"),
            }
        ),
        encoding="utf-8",
    )

    manifest_out = tmp_path / "multi_summary_manifest.json"
    manifest = generate_provenance_manifest(
        input_files=[],
        output_manifest_path=str(manifest_out),
        summary_file=[str(s1), str(s2)],
    )

    assert manifest["target_delta_version"] == 5
    assert "summaries" in manifest["ingestion_summary"]
    assert len(manifest["ingestion_summary"]["summaries"]) == 2
    assert validate_provenance_manifest(str(manifest_out)) is True


def test_generate_provenance_manifest_remote_uris(tmp_path: Path):
    """Test generating provenance manifest with cloud storage URIs."""
    manifest_out = tmp_path / "remote_manifest.json"
    remote_inputs = ["s3://bucket/reads_1.fastq", "s3a://bucket/variants.vcf"]

    manifest = generate_provenance_manifest(
        input_files=remote_inputs,
        output_manifest_path=str(manifest_out),
    )

    assert len(manifest["inputs"]) == 2
    assert manifest["inputs"][0]["storage"] == "remote_uri"
    assert manifest["inputs"][0]["size_bytes"] is None
    assert len(manifest["inputs"][0]["sha256"]) == 64
    assert validate_provenance_manifest(str(manifest_out)) is True


def test_generate_provenance_manifest_missing_file_raises(tmp_path: Path):
    """Test that specifying a missing local input file raises FileNotFoundError."""
    missing_path = tmp_path / "nonexistent.fastq"
    with pytest.raises(
        FileNotFoundError, match="Input file specified for provenance hashing does not exist"
    ):
        generate_provenance_manifest(
            input_files=[str(missing_path)],
            output_manifest_path=str(tmp_path / "manifest.json"),
        )
