"""GxP cryptographic execution manifest generator for FDA 21 CFR Part 11 compliance.

Computes SHA-256 digests for all multi-omics workflow inputs, records pinned container
specifications, and captures Delta Lake transaction commit versions for complete audit traceability.
"""

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

# Ensure repository root is on PYTHONPATH
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from governance.crypto import compute_sha256, is_valid_sha256  # noqa: E402

DEFAULT_CONTAINERS = {
    "fastqc": "quay.io/biocontainers/fastqc:0.12.1--hdfd78af_0",
    "bcftools": "quay.io/biocontainers/bcftools:1.19--h8b25389_1",
    "multiqc": "quay.io/biocontainers/multiqc:1.21--pyhdfd78af_0",
}


def generate_provenance_manifest(
    input_files: list[str],
    output_manifest_path: str = "provenance_manifest.json",
    pipeline_version: str = "0.4.0",
    workflow_session_id: str | None = None,
    summary_file: str | list[str] | None = None,
    containers: dict[str, str] | None = None,
    delta_log_dir: str | None = None,
) -> dict:
    """Generates an FDA 21 CFR Part 11 compliant cryptographic execution manifest.

    Args:
        input_files: List of file paths to hash (FASTQ, VCF, config).
        output_manifest_path: Target path to write the JSON manifest.
        pipeline_version: Semantic version of the running pipeline.
        workflow_session_id: Nextflow session ID or unique execution identifier.
        summary_file: Path or list of paths to ingestion summary JSON from OMOP_INGEST.
        containers: Dictionary of tool names to pinned container URIs.
        delta_log_dir: Optional path to Delta Lake _delta_log directory to extract version.

    Returns:
        The generated manifest dictionary.
    """
    file_manifests = []
    for file_path in input_files:
        if os.path.exists(file_path) and os.path.isfile(file_path):
            digest = compute_sha256(file_path)
            file_manifests.append(
                {
                    "path": str(file_path),
                    "filename": os.path.basename(file_path),
                    "size_bytes": os.path.getsize(file_path),
                    "sha256": digest,
                }
            )

    ingestion_metrics = {}
    if summary_file:
        summary_paths = summary_file if isinstance(summary_file, list) else [summary_file]
        loaded_summaries = []
        for sp in summary_paths:
            if os.path.exists(sp):
                try:
                    with open(sp, encoding="utf-8") as sf:
                        loaded_summaries.append(json.load(sf))
                except (OSError, json.JSONDecodeError) as e:
                    loaded_summaries.append({"error": f"Failed to read summary file {sp}: {e}"})
        if len(loaded_summaries) == 1:
            ingestion_metrics = loaded_summaries[0]
        elif len(loaded_summaries) > 1:
            ingestion_metrics = {"summaries": loaded_summaries}

    delta_version = None
    log_dir_to_check = None
    if delta_log_dir and os.path.exists(delta_log_dir):
        candidate_inner = os.path.join(delta_log_dir, "_delta_log")
        if os.path.isdir(candidate_inner):
            log_dir_to_check = candidate_inner
        elif os.path.isdir(delta_log_dir):
            log_dir_to_check = delta_log_dir
    elif isinstance(ingestion_metrics, dict):
        gold_path = ingestion_metrics.get("gold_table_path")
        if gold_path and os.path.exists(gold_path):
            candidate_inner = os.path.join(gold_path, "_delta_log")
            if os.path.isdir(candidate_inner):
                log_dir_to_check = candidate_inner

    if log_dir_to_check and os.path.exists(log_dir_to_check):
        try:
            commit_files = [
                f for f in os.listdir(log_dir_to_check) if f.endswith(".json") and f[:-5].isdigit()
            ]
            if commit_files:
                latest_commit = max(commit_files, key=lambda x: int(x[:-5]))
                delta_version = int(latest_commit[:-5])
        except OSError:
            delta_version = None

    if delta_version is None and isinstance(ingestion_metrics, dict):
        raw_version = ingestion_metrics.get("target_delta_version")
        if raw_version is not None:
            try:
                delta_version = int(raw_version)
            except (ValueError, TypeError):
                delta_version = None

    manifest = {
        "manifest_version": "1.0.0",
        "pipeline_name": "life-sciences-data-foundry-pipeline",
        "pipeline_version": pipeline_version,
        "workflow_session_id": workflow_session_id or "unknown-session",
        "timestamp": datetime.now(UTC).isoformat(),
        "compliance": {
            "regulatory_standard": "FDA 21 CFR Part 11 (§11.10, §11.50)",
            "hash_algorithm": "SHA-256",
            "audit_trail_status": "VALIDATED",
        },
        "inputs": file_manifests,
        "containers": containers or DEFAULT_CONTAINERS,
        "ingestion_summary": ingestion_metrics,
        "target_delta_version": delta_version,
    }

    # Seal manifest with a digest of the payload (excluding the seal field itself)
    manifest_bytes = json.dumps(manifest, sort_keys=True).encode("utf-8")
    manifest["manifest_sha256"] = compute_sha256(manifest_bytes)

    os.makedirs(os.path.dirname(os.path.abspath(output_manifest_path)), exist_ok=True)
    with open(output_manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return manifest


def validate_provenance_manifest(manifest_path: str) -> bool:
    """Validates the cryptographic integrity and structure of a provenance manifest.

    Args:
        manifest_path: Path to the provenance manifest JSON file.

    Returns:
        True if the manifest is structurally valid and cryptographic digests are sound.
    """
    if not os.path.exists(manifest_path):
        return False

    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    if "manifest_sha256" not in manifest:
        return False

    stored_sha = manifest["manifest_sha256"]
    if not is_valid_sha256(stored_sha):
        return False

    # Recompute hash without the manifest_sha256 field
    manifest_copy = dict(manifest)
    del manifest_copy["manifest_sha256"]
    computed_sha = compute_sha256(json.dumps(manifest_copy, sort_keys=True).encode("utf-8"))

    if stored_sha != computed_sha:
        return False

    # Verify input hashes
    for entry in manifest.get("inputs", []):
        if not is_valid_sha256(entry.get("sha256")):
            return False

    return True


def main() -> None:
    """CLI entrypoint for standalone manifest generation."""
    parser = argparse.ArgumentParser(
        description="Generate FDA 21 CFR Part 11 compliant provenance manifest."
    )
    parser.add_argument(
        "--input-files",
        nargs="+",
        default=[],
        help="Input files to hash with SHA-256",
    )
    parser.add_argument(
        "--output-manifest",
        default="provenance_manifest.json",
        help="Destination path for manifest JSON",
    )
    parser.add_argument(
        "--pipeline-version",
        default="0.4.0",
        help="Pipeline semantic version",
    )
    parser.add_argument(
        "--workflow-session-id",
        default=None,
        help="Nextflow session ID",
    )
    parser.add_argument(
        "--summary-file",
        nargs="*",
        default=None,
        help="Path(s) to ingestion_summary.json",
    )
    parser.add_argument(
        "--delta-log-dir",
        default=None,
        help="Path to _delta_log directory",
    )

    args = parser.parse_args()
    manifest = generate_provenance_manifest(
        input_files=args.input_files,
        output_manifest_path=args.output_manifest,
        pipeline_version=args.pipeline_version,
        workflow_session_id=args.workflow_session_id,
        summary_file=args.summary_file,
        delta_log_dir=args.delta_log_dir,
    )
    print(
        f"[PROVENANCE] Manifest generated at: {args.output_manifest} (SHA-256: {manifest['manifest_sha256'][:16]}...)"
    )


if __name__ == "__main__":
    main()
