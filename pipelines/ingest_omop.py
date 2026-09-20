"""CLI bridge utility for ingesting filtered VCF genomic variants into OMOP CDM v5.4 Delta tables."""

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

# Ensure repository root and analytical-layer are present on PYTHONPATH
REPO_ROOT = Path(__file__).resolve().parent.parent
ANALYTICAL_LAYER = REPO_ROOT / "analytical-layer"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(ANALYTICAL_LAYER) not in sys.path:
    sys.path.insert(0, str(ANALYTICAL_LAYER))

from governance.crypto import compute_sha256  # noqa: E402
from medallion.writer import DeltaMedallionWriter  # noqa: E402
from omop_cdm_v54.connectors import parse_vcf_to_dataframe  # noqa: E402
from omop_cdm_v54.genomic_variants import transform_genomic_variants  # noqa: E402
from omop_cdm_v54.pipeline import create_spark_session  # noqa: E402


def ingest_vcf_to_omop(
    vcf_path: str,
    output_dir: str = "output/delta",
    mode: str = "demo",
    summary_out: str = "ingestion_summary.json",
) -> dict:
    """Parses a VCF file, transforms variants into OMOP CDM v5.4 MEASUREMENT records, and writes to Delta.

    Args:
        vcf_path: Path to the input VCF file.
        output_dir: Target base directory for Delta Lake tables.
        mode: Execution mode ('demo' or 'remote').
        summary_out: Destination path for the JSON ingestion summary.

    Returns:
        Dictionary containing ingestion metrics and SHA-256 provenance.
    """
    if not os.path.exists(vcf_path):
        raise FileNotFoundError(f"Input VCF file not found: {vcf_path}")

    vcf_sha256 = compute_sha256(vcf_path)

    spark = create_spark_session(mode=mode)
    try:
        df_silver = parse_vcf_to_dataframe(spark, vcf_path)
        variant_count = df_silver.count()

        df_measurement = transform_genomic_variants(df_silver)
        measurement_count = df_measurement.count()

        writer = DeltaMedallionWriter(spark, base_path=output_dir)
        writer.write_silver_table(df_silver, "genomic_variants")
        writer.write_gold_omop_table(df_measurement, "measurement")

        summary = {
            "status": "SUCCESS",
            "vcf_file": str(vcf_path),
            "vcf_sha256": vcf_sha256,
            "variant_count": variant_count,
            "omop_measurement_count": measurement_count,
            "output_dir": str(output_dir),
            "timestamp": datetime.now(UTC).isoformat(),
        }

        os.makedirs(os.path.dirname(os.path.abspath(summary_out)), exist_ok=True)
        with open(summary_out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        return summary
    finally:
        spark.stop()


def main() -> None:
    """CLI entrypoint for standalone Nextflow OMOP ingestion process."""
    parser = argparse.ArgumentParser(
        description="Ingest filtered VCF genomic variants into OMOP CDM v5.4 Delta tables."
    )
    parser.add_argument("--vcf", required=True, type=str, help="Path to input VCF file")
    parser.add_argument(
        "--output-dir",
        default="output/delta",
        type=str,
        help="Target base directory for Delta Lake tables",
    )
    parser.add_argument(
        "--mode",
        default="demo",
        type=str,
        choices=["demo", "remote"],
        help="Ingestion mode ('demo' or 'remote')",
    )
    parser.add_argument(
        "--summary-out",
        default="ingestion_summary.json",
        type=str,
        help="Path to write the execution summary JSON",
    )

    args = parser.parse_args()
    summary = ingest_vcf_to_omop(
        vcf_path=args.vcf,
        output_dir=args.output_dir,
        mode=args.mode,
        summary_out=args.summary_out,
    )
    print(
        f"[OMOP INGEST] Successfully transformed {summary['variant_count']} variants "
        f"into {summary['omop_measurement_count']} OMOP MEASUREMENT records."
    )


if __name__ == "__main__":
    main()
