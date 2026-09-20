"""Unit tests for Nextflow Multi-Omics to OMOP pipeline and GxP provenance generator."""

import json
from pathlib import Path

from pipelines.provenance import (
    DEFAULT_CONTAINERS,
    generate_provenance_manifest,
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
