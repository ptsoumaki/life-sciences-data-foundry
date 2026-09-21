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

from pyspark.sql import SparkSession  # noqa: E402

from governance.crypto import compute_sha256  # noqa: E402
from medallion.writer import DELTA_TELEMETRY_EXCEPTIONS, DeltaMedallionWriter  # noqa: E402
from omop_cdm_v54.connectors import parse_vcf_to_dataframe  # noqa: E402
from omop_cdm_v54.genomic_variants import transform_genomic_variants  # noqa: E402
from omop_cdm_v54.pipeline import create_spark_session  # noqa: E402


def ingest_vcf_to_omop(
    vcf_path: str,
    output_dir: str = "output/delta",
    mode: str = "demo",
    summary_out: str = "ingestion_summary.json",
    spark: SparkSession | None = None,
    write_mode: str = "append",
) -> dict:
    """Parses a VCF file, transforms variants into OMOP CDM v5.4 MEASUREMENT records, and writes to Delta.

    Args:
        vcf_path: Path to the input VCF file.
        output_dir: Target base directory for Delta Lake tables.
        mode: Execution mode ('demo' or 'remote').
        summary_out: Destination path for the JSON ingestion summary.
        spark: Optional active SparkSession instance to reuse.
        write_mode: Delta Lake write mode ('append' or 'overwrite', default: 'append').

    Returns:
        Dictionary containing ingestion metrics and SHA-256 provenance.
    """
    is_remote_vcf = vcf_path.startswith(("s3://", "s3a://", "gs://", "hdfs://"))
    if not is_remote_vcf and not os.path.exists(vcf_path):
        raise FileNotFoundError(f"Input VCF file not found: {vcf_path}")

    vcf_sha256 = compute_sha256(vcf_path)

    owns_spark = False
    if spark is not None:
        spark_to_use = spark
    else:
        active = SparkSession.getActiveSession()
        if active is not None:
            spark_to_use = active
        else:
            spark_to_use = create_spark_session(mode=mode)
            owns_spark = True

    try:
        df_silver = parse_vcf_to_dataframe(spark_to_use, vcf_path)
        variant_count = df_silver.count()

        df_measurement = transform_genomic_variants(df_silver)
        measurement_count = df_measurement.count()

        writer = DeltaMedallionWriter(spark_to_use, base_output_dir=output_dir)
        silver_path = writer.write_silver_table(df_silver, "genomic_variants", mode=write_mode)
        gold_path = writer.write_gold_omop_table(df_measurement, "measurement", mode=write_mode)

        target_delta_version = None
        try:
            telemetry = writer.get_table_telemetry(gold_path)
            recent_commits = telemetry.get("recent_commits", [])
            if recent_commits and "version" in recent_commits[0]:
                target_delta_version = recent_commits[0]["version"]
        except DELTA_TELEMETRY_EXCEPTIONS:
            target_delta_version = None

        summary = {
            "status": "SUCCESS",
            "vcf_file": str(vcf_path),
            "vcf_sha256": vcf_sha256,
            "variant_count": variant_count,
            "omop_measurement_count": measurement_count,
            "write_mode": write_mode,
            "output_dir": str(output_dir),
            "silver_table_path": silver_path,
            "gold_table_path": gold_path,
            "target_delta_version": target_delta_version,
            "timestamp": datetime.now(UTC).isoformat(),
        }

        os.makedirs(os.path.dirname(os.path.abspath(summary_out)), exist_ok=True)
        with open(summary_out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        return summary
    finally:
        if owns_spark:
            spark_to_use.stop()


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
        "--write-mode",
        default="append",
        type=str,
        choices=["append", "overwrite"],
        help="Delta Lake write mode ('append' or 'overwrite', default: 'append')",
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
        write_mode=args.write_mode,
    )
    print(
        f"[OMOP INGEST] Successfully transformed {summary['variant_count']} variants "
        f"into {summary['omop_measurement_count']} OMOP MEASUREMENT records (mode={args.write_mode})."
    )


if __name__ == "__main__":
    main()
