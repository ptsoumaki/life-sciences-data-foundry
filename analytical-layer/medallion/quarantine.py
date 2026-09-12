"""
Module: quarantine.py
Description: Enterprise GxP Dead-Letter Quarantine Sinks and Standardized Clinical Failure Taxonomy.
             Isolates non-compliant clinical, observational, and genomic records in dedicated Delta Lake
             quarantine tables preserving verbatim raw JSON payloads, failure timestamps, and MLflow run IDs
             for FDA 21 CFR Part 11 audit traceability.
Author: Vivi Tsoumaki
"""

import os
from enum import StrEnum

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    lit,
    struct,
    to_json,
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class ClinicalFailureCode(StrEnum):
    """
    Standardized clinical failure taxonomy and error codes for GxP data contract enforcement:
    - SCHEMA_VIOLATION: Missing required primary keys, malformed data types, nulls in mandatory fields.
    - UNMAPPED_TERMINOLOGY: Code mapping failure where clinical code cannot be resolved to standard OMOP concept.
    - OUT_OF_BOUNDS_LAB: Lab numerical measurements outside physiological or biological plausibility limits.
    - TEMPORAL_ANOMALY: Chronological sequence violation (e.g. event preceding birth, future timestamp).
    - ORPHAN_FOREIGN_KEY: Records referencing non-existent person_id or missing parent demographic entity.
    """

    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    UNMAPPED_TERMINOLOGY = "UNMAPPED_TERMINOLOGY"
    OUT_OF_BOUNDS_LAB = "OUT_OF_BOUNDS_LAB"
    TEMPORAL_ANOMALY = "TEMPORAL_ANOMALY"
    ORPHAN_FOREIGN_KEY = "ORPHAN_FOREIGN_KEY"


# Standard GxP Dead-Letter Quarantine Schema
QUARANTINE_RECORD_SCHEMA = StructType(
    [
        StructField("quarantine_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("raw_payload", StringType(), False),
        StructField("failure_code", StringType(), False),
        StructField("failure_reason", StringType(), False),
        StructField("failure_timestamp", TimestampType(), False),
        StructField("mlflow_run_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("remediation_timestamp", TimestampType(), True),
    ]
)

# Canonical Quarantine Delta Table Identifiers
QUARANTINE_TABLE_PATIENTS = "quarantine_patients"
QUARANTINE_TABLE_CONDITIONS = "quarantine_conditions"
QUARANTINE_TABLE_MEASUREMENTS = "quarantine_measurements"


def get_active_mlflow_run_id() -> str:
    """Safely retrieves the active MLflow run ID or returns 'untracked_run'."""
    try:
        import mlflow

        active_run = mlflow.active_run()
        if active_run and active_run.info and active_run.info.run_id:
            return str(active_run.info.run_id)
    except Exception:
        pass
    return "untracked_run"


def format_quarantine_dataframe(
    df: DataFrame,
    table_name: str,
    failure_code: ClinicalFailureCode | str,
    failure_reason: str | Column,
    mlflow_run_id: str | None = None,
) -> DataFrame:
    """
    Transforms a non-compliant PySpark DataFrame into the standardized GxP dead-letter quarantine structure.
    Preserves verbatim raw row payloads as a serialized JSON string to ensure zero data loss.

    Args:
        df: Input non-compliant PySpark DataFrame.
        table_name: Target domain name (e.g., 'quarantine_patients', 'quarantine_conditions').
        failure_code: ClinicalFailureCode enum member or corresponding string identifier.
        failure_reason: Deterministic human-readable explanation (string or PySpark Column expression).
        mlflow_run_id: MLflow run ID for FDA 21 CFR Part 11 cryptographic traceability.

    Returns:
        Standardized dead-letter quarantine DataFrame adhering to QUARANTINE_RECORD_SCHEMA.
    """
    if df.rdd.isEmpty():
        # Return an empty DataFrame matching the quarantine schema
        spark = df.sparkSession
        return spark.createDataFrame([], QUARANTINE_RECORD_SCHEMA)

    code_val = failure_code.value if isinstance(failure_code, ClinicalFailureCode) else str(failure_code)
    run_id = mlflow_run_id or get_active_mlflow_run_id()

    # Pack all existing columns into a verbatim raw JSON string
    all_cols = [col(c) for c in df.columns]
    raw_payload_expr = to_json(struct(*all_cols))

    reason_expr = failure_reason if isinstance(failure_reason, Column) else lit(str(failure_reason))

    quarantine_df = (
        df.withColumn("quarantine_id", expr("uuid()"))
        .withColumn("table_name", lit(table_name))
        .withColumn("raw_payload", raw_payload_expr)
        .withColumn("failure_code", lit(code_val))
        .withColumn("failure_reason", reason_expr)
        .withColumn("failure_timestamp", current_timestamp())
        .withColumn("mlflow_run_id", lit(run_id))
        .withColumn("status", lit("QUARANTINED"))
        .withColumn("remediation_timestamp", lit(None).cast("timestamp"))
        .select(
            "quarantine_id",
            "table_name",
            "raw_payload",
            "failure_code",
            "failure_reason",
            "failure_timestamp",
            "mlflow_run_id",
            "status",
            "remediation_timestamp",
        )
    )

    return quarantine_df


class QuarantineDeltaWriter:
    """
    Dedicated Delta Lake Quarantine Sink Writer.
    Persists dead-letter quarantine tables (`quarantine_patients`, `quarantine_conditions`, `quarantine_measurements`)
    with schema evolution and Change Data Feed enabled for complete GxP auditability.
    """

    def __init__(
        self,
        spark: SparkSession,
        base_output_dir: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        if base_output_dir is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.base_output_dir = os.path.join(base_dir, "data", "delta_warehouse")
        else:
            self.base_output_dir = base_output_dir

    def get_quarantine_table_path(self, table_name: str) -> str:
        """Constructs canonical file path for a quarantine Delta Lake table sink."""
        clean_name = table_name.lower().strip()
        if not clean_name.startswith("quarantine_"):
            clean_name = f"quarantine_{clean_name}"
        return os.path.join(self.base_output_dir, "quarantine", clean_name).replace("\\", "/")

    def _get_uc_table_name(self, table_name: str) -> str | None:
        """Constructs Unity Catalog 3-level namespace identifier if configured."""
        if self.catalog and self.schema:
            clean_name = table_name.lower().strip()
            return f"{self.catalog}.{self.schema}.{clean_name}"
        return None

    def write_quarantine_sink(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
    ) -> str:
        """
        Persists dead-letter quarantine records to a dedicated Delta Lake sink with Change Data Feed
        and schema evolution enabled.

        Args:
            df: Standardized quarantine DataFrame.
            table_name: Target quarantine table identifier.
            mode: Write mode ('append' default for dead-letter accumulator, 'overwrite' for staging).

        Returns:
            Resolved storage path of the persisted Delta Lake quarantine table.
        """
        path = self.get_quarantine_table_path(table_name)
        writer = (
            df.write.format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .option("delta.enableChangeDataFeed", "true")
        )

        uc_table = self._get_uc_table_name(table_name)
        if uc_table:
            writer.option("path", path).saveAsTable(uc_table)
            print(f"[DELTA QUARANTINE] Persisted dead-letter records to UC '{uc_table}' at {path}")
        else:
            writer.save(path)
            print(f"[DELTA QUARANTINE] Persisted dead-letter records to {path} (mode={mode})")

        return path

    def write_quarantine_patients(self, df: DataFrame, mode: str = "append") -> str:
        """Dedicated sink for non-compliant patient demographics."""
        return self.write_quarantine_sink(df, QUARANTINE_TABLE_PATIENTS, mode=mode)

    def write_quarantine_conditions(self, df: DataFrame, mode: str = "append") -> str:
        """Dedicated sink for non-compliant clinical conditions and diagnoses."""
        return self.write_quarantine_sink(df, QUARANTINE_TABLE_CONDITIONS, mode=mode)

    def write_quarantine_measurements(self, df: DataFrame, mode: str = "append") -> str:
        """Dedicated sink for non-compliant laboratory and genomic measurements."""
        return self.write_quarantine_sink(df, QUARANTINE_TABLE_MEASUREMENTS, mode=mode)

    def read_quarantine_table(self, table_name: str) -> DataFrame:
        """Reads a quarantine Delta Lake table by name."""
        path = self.get_quarantine_table_path(table_name)
        try:
            return self.spark.read.format("delta").load(path)
        except Exception:
            # Fallback for local Windows environments lacking native hadoop.dll;
            # Spark Parquet loader automatically skips hidden _delta_log metadata.
            return self.spark.read.parquet(path)
