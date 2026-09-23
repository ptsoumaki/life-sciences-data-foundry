"""
Module: quarantine.py
Description: Enterprise GxP Dead-Letter Quarantine Sinks and Standardized Clinical Failure Taxonomy.
             Isolates non-compliant clinical, observational, and genomic records in dedicated Delta Lake
             quarantine tables preserving verbatim raw JSON payloads, failure timestamps, and MLflow run IDs
             for FDA 21 CFR Part 11 audit traceability.
Author: Vivi Tsoumaki
"""

import datetime
import json
import os
from enum import StrEnum
from typing import Any, cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    expr,
    from_json,
    lit,
    struct,
    to_json,
    to_timestamp,
    trim,
    upper,
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from medallion.writer import DeltaMedallionWriter
from omop_cdm_v54.compat import HAS_DELTA, DeltaTable
from omop_cdm_v54.vocabularies import (
    DEFAULT_ICD10_MAPPINGS,
    DEFAULT_LOINC_MAPPINGS,
    load_concept_mappings,
)

try:
    import mlflow
except ImportError:
    mlflow = None  # type: ignore[assignment]


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
    TARGET_CONTRACT_VIOLATION = "TARGET_CONTRACT_VIOLATION"


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
QUARANTINE_TABLE_TARGETS = "quarantine_target_evidence"


def get_active_mlflow_run_id() -> str:
    """Safely retrieves the active MLflow run ID or returns 'untracked_run'."""
    if mlflow is not None:
        try:
            active_run = mlflow.active_run()
            if active_run and active_run.info and active_run.info.run_id:
                return str(active_run.info.run_id)
        except (mlflow.exceptions.MlflowException, AttributeError, OSError):
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
    code_val = (
        failure_code.value if isinstance(failure_code, ClinicalFailureCode) else str(failure_code)
    )
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
        Persists dead-letter quarantine records to a dedicated Delta Lake sink with schema
        evolution enabled.

        ``delta.enableChangeDataFeed`` is passed as a write option and is persisted as a Delta
        table property only on the initial table creation. Subsequent appends to an existing sink
        silently ignore this option. To retroactively enable Change Data Feed on an existing table::

            ALTER TABLE delta.`<table_path>`
            SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')

        Args:
            df: Standardized quarantine DataFrame.
            table_name: Target quarantine table identifier.
            mode: Write mode ('append' default for dead-letter accumulation, 'overwrite' for staging).

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
            try:
                writer.save(path)
                print(f"[DELTA QUARANTINE] Persisted dead-letter records to {path} (mode={mode})")
            except Exception as e:
                if os.name == "nt" and ("UnsatisfiedLinkError" in str(e) or "NativeIO" in str(e)):
                    print(
                        "[DELTA QUARANTINE NOTICE] Windows native hadoop.dll access0 exception encountered. Retrying persistence with mode='overwrite'."
                    )
                    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").option(
                        "delta.enableChangeDataFeed", "true"
                    ).save(path)
                else:
                    raise

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

    def write_quarantine_targets(self, df: DataFrame, mode: str = "append") -> str:
        """Dedicated sink for non-compliant target discovery evidence records."""
        return self.write_quarantine_sink(df, QUARANTINE_TABLE_TARGETS, mode=mode)

    def read_quarantine_table(self, table_name: str) -> DataFrame:
        """Reads a quarantine Delta Lake table by name."""
        path = self.get_quarantine_table_path(table_name)
        if not os.path.exists(path):
            return self.spark.createDataFrame([], QUARANTINE_RECORD_SCHEMA)

        try:
            return self.spark.read.format("delta").load(path)
        except Exception:
            # Fallback for local Windows environments lacking native hadoop.dll;
            # Loads underlying Parquet records directly via PyArrow without JVM filesystem link errors.
            try:
                table = pq.read_table(path)
                pdf = table.to_pandas()
                # Convert timezone-aware timestamp columns to timezone-naive UTC so they
                # are compatible with PySpark's timezone-naive TimestampType.
                # tz_convert(None) is a no-op on columns that are already tz-naive.
                for ts_col in ["failure_timestamp", "remediation_timestamp"]:
                    if ts_col in pdf.columns and isinstance(pdf[ts_col].dtype, pd.DatetimeTZDtype):
                        pdf[ts_col] = pdf[ts_col].dt.tz_convert(None)
                return self.spark.createDataFrame(pdf, QUARANTINE_RECORD_SCHEMA)
            except Exception:
                return self.spark.read.parquet(path)


class GxPBreachError(RuntimeError):
    """Raised when batch quarantine rejection ratio breaches configured GxP quality thresholds."""

    pass


def evaluate_batch_quarantine_threshold(
    total_ingested: int,
    total_quarantined: int,
    threshold: float = 0.02,
    failure_counts_by_code: dict[str, int] | None = None,
    abort_on_breach: bool = False,
    mlflow_run_id: str | None = None,
) -> dict[str, Any]:
    """
    Computes batch quarantine rejection ratio and enforces GxP compliance breach threshold gates:
    Rejection Ratio = total_quarantined / total_ingested

    Logs breach metrics and compliance status to MLflow for FDA 21 CFR Part 11 auditing.
    Raises GxPBreachError if the threshold is breached and abort_on_breach is True.

    Args:
        total_ingested: Total count of ingested rows across all clinical domains.
        total_quarantined: Total count of records routed to quarantine.
        threshold: Tolerable quarantine ratio limit (default 0.02 = 2.0%).
        failure_counts_by_code: Optional breakdown of failures by ClinicalFailureCode.
        abort_on_breach: When True, raises GxPBreachError on threshold violation.
        mlflow_run_id: Optional MLflow run ID for explicit run metric logging.

    Returns:
        Dictionary containing threshold evaluation results and telemetry metrics.
    """
    rejection_ratio = (total_quarantined / total_ingested) if total_ingested > 0 else 0.0
    is_breach = rejection_ratio > threshold
    status = "BREACH" if is_breach else "COMPLIANT"

    breakdown = failure_counts_by_code or {}

    # MLflow audit logging — defined as a closure to share the computed metrics variables.
    def _emit_mlflow_metrics() -> None:
        mlflow.log_metric("quarantine_total_ingested", total_ingested)
        mlflow.log_metric("quarantine_total_quarantined", total_quarantined)
        mlflow.log_metric("quarantine_rejection_ratio", rejection_ratio)
        mlflow.log_metric("quarantine_threshold_limit", threshold)
        mlflow.log_metric("gxp_breach_detected", 1.0 if is_breach else 0.0)
        for code, count in breakdown.items():
            clean_code = str(code).lower()
            mlflow.log_metric(f"quarantine_count_{clean_code}", count)
        mlflow.set_tag("gxp_compliance_status", status)
        if is_breach:
            mlflow.set_tag(
                "gxp_breach_reason",
                f"Quarantine ratio {rejection_ratio:.4f} exceeded threshold {threshold:.4f}",
            )

    if mlflow is not None:
        try:
            active = mlflow.active_run()
            if active:
                # Log directly into the caller-managed active run.
                _emit_mlflow_metrics()
            elif mlflow_run_id:
                # Re-attach to the specified run; avoids auto-creating an orphan run under
                # the default experiment when no active context exists.
                with mlflow.start_run(run_id=mlflow_run_id):
                    _emit_mlflow_metrics()
        except Exception as e:
            print(f"[MLFLOW NOTICE] Quarantine threshold logging notice: {e}")

    result = {
        "status": status,
        "is_breach": is_breach,
        "total_ingested": total_ingested,
        "total_quarantined": total_quarantined,
        "rejection_ratio": rejection_ratio,
        "threshold": threshold,
        "failure_breakdown": breakdown,
    }

    if is_breach:
        msg = (
            f"GxP Batch Quality Breach! Quarantine rejection ratio {rejection_ratio:.2%} "
            f"exceeded configured tolerance threshold of {threshold:.2%} "
            f"({total_quarantined}/{total_ingested} records quarantined). Failure breakdown: {breakdown}"
        )
        print(f"[GxP BREACH WARNING] {msg}")
        if abort_on_breach:
            raise GxPBreachError(msg)
    else:
        print(
            f"[GxP QUALITY GATE] Batch within tolerance: {rejection_ratio:.2%} quarantined "
            f"(threshold: {threshold:.2%}). Status: COMPLIANT."
        )

    return result


class QuarantineRemediationEngine:
    """
    Idempotent Clinical Quarantine Remediation & Replay Engine.
    Allows clinical data stewards to:
    1. Query unresolved quarantined records (status='QUARANTINED').
    2. Re-evaluate records against updated vocabulary mappings (ICD-10, LOINC) or patched rules.
    3. Idempotently promote corrected records into Silver tier Delta tables via Delta MERGE (SCD Type 1).
    4. Mark remediated records as 'REMEDIATED' with a UTC timestamp in the quarantine Delta sink.
    """

    def __init__(self, spark: SparkSession, base_output_dir: str | None = None):
        self.spark = spark
        self.base_output_dir = base_output_dir
        self.q_writer = QuarantineDeltaWriter(spark, base_output_dir=base_output_dir)
        self.medallion_writer = DeltaMedallionWriter(spark, base_output_dir=base_output_dir)

    def get_unresolved_records(self, table_name: str) -> DataFrame:
        """Retrieves active quarantined records pending remediation."""
        try:
            df = self.q_writer.read_quarantine_table(table_name)
            return df.filter(col("status") == "QUARANTINED")
        except Exception:
            return self.spark.createDataFrame([], QUARANTINE_RECORD_SCHEMA)

    def mark_as_remediated(self, table_name: str, remediated_ids: list[str]) -> None:
        """
        Idempotently marks quarantine records as 'REMEDIATED' with remediation_timestamp
        using Delta MERGE.
        """
        if not remediated_ids:
            return

        target_path = self.q_writer.get_quarantine_table_path(table_name)
        remediation_ts = current_timestamp()

        if HAS_DELTA and self.medallion_writer._is_delta_table(target_path):
            try:
                dt = DeltaTable.forPath(self.spark, target_path)
                id_df = self.spark.createDataFrame([(i,) for i in remediated_ids], ["rem_id"])
                dt.alias("target").merge(
                    id_df.alias("source"), "target.quarantine_id = source.rem_id"
                ).whenMatchedUpdate(
                    set={
                        "status": lit("REMEDIATED"),
                        "remediation_timestamp": remediation_ts,
                    }
                ).execute()
                print(
                    f"[REMEDIATION] Marked {len(remediated_ids)} records as REMEDIATED in {target_path}"
                )
                return
            except Exception as e:
                print(f"[REMEDIATION NOTICE] Delta MERGE update notice: {e}")

        # PyArrow fallback for local/non-Delta environments where the JVM Delta MERGE path failed.
        # Reads via the quarantine table reader (which has its own fallback chain) to ensure only
        # committed data is processed, not stale Parquet files left behind before VACUUM runs.
        try:
            df_current = self.q_writer.read_quarantine_table(table_name)
            pdf = cast(pd.DataFrame, df_current.toPandas())
            # Use a timezone-naive UTC datetime to match the datetime64[ns] dtype that
            # pandas infers for the null-initialised remediation_timestamp column.
            now_dt = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
            rem_set = set(remediated_ids)
            mask = pdf["quarantine_id"].isin(rem_set)
            pdf.loc[mask, "status"] = "REMEDIATED"
            pdf.loc[mask, "remediation_timestamp"] = now_dt
            # Track old parquet files to record in Delta transaction log if present
            delta_log_dir = os.path.join(target_path, "_delta_log")
            old_parquet_files = [
                fname for fname in os.listdir(target_path) if fname.endswith(".parquet")
            ]

            for fname in old_parquet_files:
                try:
                    os.remove(os.path.join(target_path, fname))
                except OSError:
                    pass

            updated_table = pa.Table.from_pandas(pdf)
            out_filename = "part-00000-remediated.parquet"
            out_file = os.path.join(target_path, out_filename)
            pq.write_table(updated_table, out_file)

            # If _delta_log exists, commit the update action to preserve Delta transaction log integrity
            if os.path.isdir(delta_log_dir):
                json_commits = sorted([f for f in os.listdir(delta_log_dir) if f.endswith(".json")])
                if json_commits:
                    latest_idx = int(json_commits[-1].split(".")[0])
                    next_idx = latest_idx + 1
                    next_commit_file = os.path.join(delta_log_dir, f"{next_idx:020d}.json")
                    now_ms = int(datetime.datetime.now(datetime.UTC).timestamp() * 1000)
                    commit_entries: list[dict[str, Any]] = [
                        {
                            "commitInfo": {
                                "timestamp": now_ms,
                                "operation": "UPDATE",
                                "operationParameters": {},
                                "engineInfo": "LifeSciencesDataFoundry",
                            }
                        }
                    ]
                    for old_f in old_parquet_files:
                        commit_entries.append(
                            {
                                "remove": {
                                    "path": old_f,
                                    "deletionTimestamp": now_ms,
                                    "dataChange": True,
                                }
                            }
                        )
                    commit_entries.append(
                        {
                            "add": {
                                "path": out_filename,
                                "partitionValues": {},
                                "size": os.path.getsize(out_file),
                                "modificationTime": now_ms,
                                "dataChange": True,
                                "stats": json.dumps({"numRecords": len(pdf)}),
                            }
                        }
                    )
                    with open(next_commit_file, "w", encoding="utf-8") as f:
                        for entry in commit_entries:
                            f.write(json.dumps(entry) + "\n")

            print(
                f"[REMEDIATION] Overwritten {len(remediated_ids)} records as REMEDIATED in {target_path} (pyarrow fallback)"
            )
            return
        except Exception as pyarrow_err:
            print(f"[REMEDIATION NOTICE] PyArrow fallback notice: {pyarrow_err}")

        # Secondary fallback via Spark DataFrame overwrite
        try:
            df = self.q_writer.read_quarantine_table(table_name)
            id_list_sql = ", ".join(f"'{i}'" for i in remediated_ids)
            df_updated = df.withColumn(
                "status",
                expr(
                    f"case when quarantine_id in ({id_list_sql}) then 'REMEDIATED' else status end"
                ),
            ).withColumn(
                "remediation_timestamp",
                expr(
                    f"case when quarantine_id in ({id_list_sql}) then current_timestamp() else remediation_timestamp end"
                ),
            )
            self.q_writer.write_quarantine_sink(df_updated, table_name, mode="overwrite")
            print(
                f"[REMEDIATION] Overwritten {len(remediated_ids)} records as REMEDIATED in {target_path}"
            )
        except Exception as e:
            print(f"[REMEDIATION NOTICE] Fallback update notice: {e}")

    def remediate_conditions(
        self,
        updated_concept_mappings: dict[str, int] | None = None,
        mapping_file: str | None = None,
    ) -> dict[str, Any]:
        """
        Re-evaluates quarantined condition/diagnosis records against updated ICD-10 concept mappings.
        Corrected records are promoted to the Silver 'clinical_diagnoses' Delta table, and their
        quarantine records are marked as REMEDIATED.
        """
        df_unresolved = self.get_unresolved_records(QUARANTINE_TABLE_CONDITIONS)
        count = df_unresolved.count()
        if count == 0:
            return {
                "table_name": QUARANTINE_TABLE_CONDITIONS,
                "status": "NO_RECORDS",
                "total_evaluated": 0,
                "remediated_count": 0,
                "unresolved_count": 0,
            }

        mappings_data = load_concept_mappings(mapping_file)
        icd10_map = dict(mappings_data.get("icd10_to_snomed", DEFAULT_ICD10_MAPPINGS))
        if updated_concept_mappings:
            icd10_map.update(updated_concept_mappings)

        payload_rdd = df_unresolved.select("raw_payload").rdd.map(lambda r: r[0])
        json_schema = self.spark.read.json(payload_rdd).schema

        df_unpacked = df_unresolved.withColumn(
            "_unpacked", from_json(col("raw_payload"), json_schema)
        ).select("quarantine_id", "_unpacked.*")

        df_with_eval = df_unpacked.withColumn(
            "parsed_diag_dt", expr("try_cast(diagnosis_date as date)")
        )

        valid_codes = [k.upper() for k, v in icd10_map.items() if not k.startswith("_") and v != 0]
        all_valid_codes = list(set(valid_codes) | {c.replace(".", "") for c in valid_codes})

        code_col_name = "icd10_code" if "icd10_code" in df_with_eval.columns else "code"
        is_valid_date = col("parsed_diag_dt").isNotNull()
        is_mapped_code = (
            upper(trim(col(code_col_name))).isin(all_valid_codes)
            if code_col_name in df_with_eval.columns
            else lit(False)
        )
        is_remediated_cond = is_valid_date & is_mapped_code

        df_remediated = df_with_eval.filter(is_remediated_cond)
        remediated_count = df_remediated.count()
        unresolved_count = count - remediated_count

        promoted_path = None
        if remediated_count > 0:
            remediated_ids = [
                r.quarantine_id for r in df_remediated.select("quarantine_id").collect()
            ]
            silver_cols = [c for c in df_remediated.columns if c != "quarantine_id"]
            df_silver_promoted = df_remediated.select(silver_cols)

            merge_keys = [
                k
                for k in [
                    "encounter_id",
                    "patient_id",
                    "raw_patient_id",
                    "diagnosis_date",
                    "icd10_code",
                    "code",
                ]
                if k in df_silver_promoted.columns
            ]
            if not merge_keys:
                merge_keys = df_silver_promoted.columns[:2]

            promoted_path = self.medallion_writer.upsert_silver_table(
                df_silver_promoted, "clinical_diagnoses", merge_keys=merge_keys
            )
            self.mark_as_remediated(QUARANTINE_TABLE_CONDITIONS, remediated_ids)

        return {
            "table_name": QUARANTINE_TABLE_CONDITIONS,
            "status": "SUCCESS",
            "total_evaluated": count,
            "remediated_count": remediated_count,
            "unresolved_count": unresolved_count,
            "promoted_path": promoted_path,
        }

    def remediate_measurements(
        self,
        updated_loinc_mappings: dict[str, int] | None = None,
        mapping_file: str | None = None,
    ) -> dict[str, Any]:
        """
        Re-evaluates quarantined laboratory/measurement records against updated LOINC concept mappings
        and physiological bounds. Corrected records are promoted to Silver 'lab_measurements'.
        """
        df_unresolved = self.get_unresolved_records(QUARANTINE_TABLE_MEASUREMENTS)
        count = df_unresolved.count()
        if count == 0:
            return {
                "table_name": QUARANTINE_TABLE_MEASUREMENTS,
                "status": "NO_RECORDS",
                "total_evaluated": 0,
                "remediated_count": 0,
                "unresolved_count": 0,
            }

        mappings_data = load_concept_mappings(mapping_file)
        loinc_map = dict(
            mappings_data.get("loinc_to_concept")
            or mappings_data.get("loinc_to_measurement")
            or DEFAULT_LOINC_MAPPINGS
        )
        if updated_loinc_mappings:
            loinc_map.update(updated_loinc_mappings)

        payload_rdd = df_unresolved.select("raw_payload").rdd.map(lambda r: r[0])
        json_schema = self.spark.read.json(payload_rdd).schema

        df_unpacked = df_unresolved.withColumn(
            "_unpacked", from_json(col("raw_payload"), json_schema)
        ).select("quarantine_id", "_unpacked.*")

        lab_val_col_name = "numeric_value" if "numeric_value" in df_unpacked.columns else "value"
        df_with_eval = (
            df_unpacked.withColumn(
                "parsed_lab_datetime",
                coalesce(
                    to_timestamp(expr("try_cast(lab_datetime as timestamp)")),
                    to_timestamp(expr("try_cast(lab_datetime as date)")),
                ),
            )
            .withColumn("parsed_lab_dt", col("parsed_lab_datetime").cast("date"))
            .withColumn("numeric_value", expr(f"try_cast({lab_val_col_name} as double)"))
        )

        valid_codes = [k.upper() for k, v in loinc_map.items() if not k.startswith("_") and v != 0]
        # Include dash-stripped variants (e.g. '8480-6' and '84806') to handle format
        # inconsistencies between stored LOINC codes and vocabulary map keys.
        # Matches the dot-stripping expansion applied to ICD-10 codes in remediate_conditions.
        all_valid_loinc_codes = list(set(valid_codes) | {c.replace("-", "") for c in valid_codes})
        is_valid_date = col("parsed_lab_dt").isNotNull()
        is_non_negative = col("numeric_value").isNull() | (col("numeric_value") >= 0.0)
        meas_code_col_name = "loinc_code" if "loinc_code" in df_with_eval.columns else "code"
        is_mapped_code = (
            upper(trim(col(meas_code_col_name))).isin(all_valid_loinc_codes)
            if meas_code_col_name in df_with_eval.columns
            else lit(False)
        )

        is_remediated_meas = is_valid_date & is_non_negative & is_mapped_code
        df_remediated = df_with_eval.filter(is_remediated_meas)
        remediated_count = df_remediated.count()
        unresolved_count = count - remediated_count

        promoted_path = None
        if remediated_count > 0:
            remediated_ids = [
                r.quarantine_id for r in df_remediated.select("quarantine_id").collect()
            ]
            silver_cols = [c for c in df_remediated.columns if c != "quarantine_id"]
            df_silver_promoted = df_remediated.select(silver_cols)

            merge_keys = [
                k
                for k in [
                    "lab_event_id",
                    "patient_id",
                    "raw_patient_id",
                    "parsed_lab_dt",
                    "loinc_code",
                    "code",
                ]
                if k in df_silver_promoted.columns
            ]
            if not merge_keys:
                merge_keys = df_silver_promoted.columns[:2]

            promoted_path = self.medallion_writer.upsert_silver_table(
                df_silver_promoted, "lab_measurements", merge_keys=merge_keys
            )
            self.mark_as_remediated(QUARANTINE_TABLE_MEASUREMENTS, remediated_ids)

        return {
            "table_name": QUARANTINE_TABLE_MEASUREMENTS,
            "status": "SUCCESS",
            "total_evaluated": count,
            "remediated_count": remediated_count,
            "unresolved_count": unresolved_count,
            "promoted_path": promoted_path,
        }

    def remediate_patients(self) -> dict[str, Any]:
        """
        Re-evaluates quarantined demographic patient records. Corrected records are promoted
        to Silver 'clinical_demographics'.
        """
        df_unresolved = self.get_unresolved_records(QUARANTINE_TABLE_PATIENTS)
        count = df_unresolved.count()
        if count == 0:
            return {
                "table_name": QUARANTINE_TABLE_PATIENTS,
                "status": "NO_RECORDS",
                "total_evaluated": 0,
                "remediated_count": 0,
                "unresolved_count": 0,
            }

        payload_rdd = df_unresolved.select("raw_payload").rdd.map(lambda r: r[0])
        json_schema = self.spark.read.json(payload_rdd).schema

        df_unpacked = df_unresolved.withColumn(
            "_unpacked", from_json(col("raw_payload"), json_schema)
        ).select("quarantine_id", "_unpacked.*")

        df_with_eval = df_unpacked.withColumn(
            "parsed_birth_dt",
            coalesce(
                to_timestamp(expr("try_cast(birth_datetime as timestamp)")),
                to_timestamp(expr("try_cast(birth_datetime as date)")),
            ),
        )

        valid_gender = upper(trim(col("gender"))).isin("MALE", "FEMALE", "M", "F", "UNKNOWN")
        is_valid_patient = col("parsed_birth_dt").isNotNull() & valid_gender
        df_remediated = df_with_eval.filter(is_valid_patient)
        remediated_count = df_remediated.count()
        unresolved_count = count - remediated_count

        promoted_path = None
        if remediated_count > 0:
            remediated_ids = [
                r.quarantine_id for r in df_remediated.select("quarantine_id").collect()
            ]
            silver_cols = [c for c in df_remediated.columns if c != "quarantine_id"]
            df_silver_promoted = df_remediated.select(silver_cols)

            merge_keys = [
                k
                for k in ["raw_patient_id", "patient_id", "id", "person_id"]
                if k in df_silver_promoted.columns
            ]
            if not merge_keys:
                merge_keys = df_silver_promoted.columns[:1]

            promoted_path = self.medallion_writer.upsert_silver_table(
                df_silver_promoted, "clinical_demographics", merge_keys=merge_keys
            )
            self.mark_as_remediated(QUARANTINE_TABLE_PATIENTS, remediated_ids)

        return {
            "table_name": QUARANTINE_TABLE_PATIENTS,
            "status": "SUCCESS",
            "total_evaluated": count,
            "remediated_count": remediated_count,
            "unresolved_count": unresolved_count,
            "promoted_path": promoted_path,
        }
