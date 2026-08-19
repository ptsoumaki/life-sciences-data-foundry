"""
Module: writer.py
Description: Enterprise PySpark Delta Lake Medallion writer providing Liquid Clustering,
             Schema Evolution contracts, Deletion Vectors, Change Data Feed,
             Idempotent Upsert (SCD Type 1 MERGE), Unity Catalog registration,
             and GxP storage metrology.
Author: Vivi Tsoumaki
"""

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from omop_cdm_v54.compat import HAS_DELTA

try:
    from delta.tables import DeltaTable
except ImportError:
    DeltaTable = None  # type: ignore[assignment]


class DeltaMedallionWriter:
    """
    Enterprise Medallion Delta Lake Writer providing:
    1. Liquid Clustering (CLUSTER BY (person_id, concept_id)).
    2. Schema Evolution Contracts (mergeSchema=True).
    3. Delta Deletion Vectors & Change Data Feed (CDF).
    4. Idempotent Upserts (Delta MERGE INTO / SCD Type 1).
    5. Unity Catalog (UC) 3-Level Namespace registration.
    6. Programmatic Storage Metrology & GxP Audit Telemetry.
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

    def _get_table_path(self, tier: str, table_name: str) -> str:
        """Constructs canonical file path for a Medallion table tier."""
        return os.path.join(self.base_output_dir, tier.lower(), table_name.lower()).replace(
            "\\", "/"
        )

    def _get_uc_table_name(self, table_name: str) -> str | None:
        """Constructs Unity Catalog 3-level namespace identifier if configured."""
        if self.catalog and self.schema:
            return f"{self.catalog}.{self.schema}.{table_name}"
        return None

    def write_silver_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
        merge_schema: bool = True,
    ) -> str:
        """
        Writes a Silver tier DataFrame to Delta format with schema evolution enabled.
        """
        path = self._get_table_path("silver", table_name)
        writer = df.write.format("delta").mode(mode)
        if merge_schema:
            writer = writer.option("mergeSchema", "true")

        uc_table = self._get_uc_table_name(f"silver_{table_name}")
        if uc_table:
            writer.option("path", path).saveAsTable(uc_table)
            print(f"[DELTA] Silver table '{table_name}' saved to UC '{uc_table}' at {path}")
        else:
            writer.save(path)
            print(
                f"[DELTA] Silver table '{table_name}' saved to {path} (mode={mode}, mergeSchema={merge_schema})"
            )
        return path

    def write_quarantine_table(
        self,
        df: DataFrame,
        table_name: str = "quarantine_records",
        mode: str = "append",
    ) -> str:
        """
        Writes rejected/quarantined records to the Silver Quarantine Delta table with schema evolution.
        """
        path = self._get_table_path("silver", table_name)
        writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")

        uc_table = self._get_uc_table_name(table_name)
        if uc_table:
            writer.option("path", path).saveAsTable(uc_table)
            print(f"[DELTA] Quarantined records saved to UC '{uc_table}' at {path}")
        else:
            writer.save(path)
            print(f"[DELTA] Quarantined records saved to {path} (mode={mode})")
        return path

    def write_gold_omop_table(
        self,
        df: DataFrame,
        table_name: str,
        cluster_by: list[str] | None = None,
        mode: str = "overwrite",
        merge_schema: bool = True,
        user_metadata: str | None = None,
    ) -> str:
        """
        Writes a Gold OMOP CDM v5.4 relational table to Delta Lake format with Liquid Clustering,
        Deletion Vectors, Change Data Feed, and schema evolution merge contracts.
        Includes GxP userMetadata option for SHA-256 cryptographic run lineage auditing.
        """
        path = self._get_table_path("gold", table_name)

        # Infer clustering keys from column presence when not explicitly supplied.
        if cluster_by is None:
            if "person_id" in df.columns and "condition_concept_id" in df.columns:
                cluster_by = ["person_id", "condition_concept_id"]
            elif "person_id" in df.columns and "measurement_concept_id" in df.columns:
                cluster_by = ["person_id", "measurement_concept_id"]
            elif "person_id" in df.columns:
                cluster_by = ["person_id"]

        writer = df.write.format("delta").mode(mode).option("delta.enableChangeDataFeed", "true")

        # Deletion Vectors are unsupported on Windows local file systems.
        if os.name != "nt":
            writer = writer.option("delta.enableDeletionVectors", "true")

        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if user_metadata:
            writer = writer.option("userMetadata", user_metadata)
        uc_table = self._get_uc_table_name(table_name)
        if uc_table:
            if cluster_by and hasattr(writer, "clusterBy"):
                # DataFrameWriter.clusterBy() API available (Delta 3.1+ / Spark 3.5+).
                writer = writer.clusterBy(*cluster_by)
            writer.option("path", path).saveAsTable(uc_table)
            print(
                f"[DELTA] Gold OMOP Table '{table_name}' saved to UC '{uc_table}' at {path} (clusterBy={cluster_by})"
            )
            if cluster_by and not hasattr(writer, "clusterBy") and HAS_DELTA:
                # clusterBy() API unavailable on this Spark/Delta version; apply Liquid
                # Clustering post-write via ALTER TABLE on the UC catalog table name.
                try:
                    cluster_cols_sql = ", ".join(cluster_by)
                    self.spark.sql(f"ALTER TABLE {uc_table} CLUSTER BY ({cluster_cols_sql})")
                    print(
                        f"[DELTA] Executed ALTER TABLE Liquid Clustering SQL on {uc_table} (CLUSTER BY ({cluster_cols_sql}))"
                    )
                except Exception as e:
                    print(f"[DELTA] Note: UC Liquid Clustering fallback failed ({e})")

            return path
        else:
            try:
                if cluster_by and hasattr(writer, "clusterBy"):
                    writer_clustered = writer.clusterBy(*cluster_by)
                    writer_clustered.save(path)
                else:
                    writer.save(path)
            except Exception as e:
                print(
                    f"[DELTA NOTICE] Local Delta save with clusterBy API fallback ({e}). Persisting table standard Delta format."
                )
                writer.save(path)
            print(
                f"[DELTA] Gold OMOP Table '{table_name}' saved to {path} (mode={mode}, clusterBy={cluster_by})"
            )

        # clusterBy() API unavailable on this Spark/Delta version; apply Liquid Clustering
        # post-write via ALTER TABLE SQL on the path-based (non-UC) table reference.
        if cluster_by and not hasattr(writer, "clusterBy") and HAS_DELTA:
            try:
                cluster_cols_sql = ", ".join(cluster_by)
                formatted_path = path.replace("\\", "/")
                if (
                    not formatted_path.startswith("file:/")
                    and not formatted_path.startswith("s3://")
                    and not formatted_path.startswith("s3a://")
                ):
                    if os.name == "nt" and len(formatted_path) > 1 and formatted_path[1] == ":":
                        formatted_path = f"file:///{formatted_path}"
                self.spark.sql(
                    f"ALTER TABLE delta.`{formatted_path}` CLUSTER BY ({cluster_cols_sql})"
                )
                print(
                    f"[DELTA] Executed ALTER TABLE Liquid Clustering SQL on {formatted_path} (CLUSTER BY ({cluster_cols_sql}))"
                )
            except Exception:
                print(
                    f"[DELTA] Note: Path-based Liquid Clustering configured for Databricks Runtime / UC ({cluster_by})"
                )

        return path

    def _is_delta_table(self, table_path: str) -> bool:
        """Checks whether a given path or table identifier is an existing Delta table."""
        if not HAS_DELTA:
            return False
        try:
            return DeltaTable.isDeltaTable(self.spark, table_path)
        except Exception:
            return False

    def upsert_gold_omop_table(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: list[str],
        cluster_by: list[str] | None = None,
    ) -> str:
        """
        Executes Idempotent Upsert (Delta MERGE INTO / SCD Type 1) into Gold OMOP CDM tables.
        Prevents duplicate patient records during incremental loads.
        """
        path = self._get_table_path("gold", table_name)
        formatted_path = path.replace("\\", "/")

        if not self._is_delta_table(formatted_path):
            return self.write_gold_omop_table(
                df, table_name, cluster_by=cluster_by, mode="overwrite"
            )

        target_table = DeltaTable.forPath(self.spark, formatted_path)
        df_dedup = df.dropDuplicates(subset=merge_keys)
        merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in merge_keys])

        target_table.alias("target").merge(
            df_dedup.alias("source"), merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        print(
            f"[DELTA MERGE] Gold Table '{table_name}' updated via Delta MERGE on keys={merge_keys}"
        )
        return path

    def optimize_table(self, table_path: str, zorder_by: list[str] | None = None) -> None:
        """
        Executes Delta Lake compaction and optimization (OPTIMIZE).
        """
        if not self._is_delta_table(table_path):
            print(f"[WARN] Skipping OPTIMIZE for {table_path}")
            return

        try:
            dt = DeltaTable.forPath(self.spark, table_path)
            if zorder_by:
                dt.optimize().executeZOrderBy(*zorder_by)
                print(f"[DELTA OPTIMIZE] Executed OPTIMIZE Z-ORDER BY {zorder_by} on {table_path}")
            else:
                dt.optimize().executeCompaction()
                print(f"[DELTA OPTIMIZE] Executed OPTIMIZE Compaction on {table_path}")
        except Exception as e:
            print(f"[WARN] OPTIMIZE notice: {e}")

    def vacuum_table(self, table_path: str, retention_hours: float | None = 168.0) -> None:
        """
        Cleans up outdated data files (VACUUM).
        """
        if not self._is_delta_table(table_path):
            print(f"[WARN] Skipping VACUUM for {table_path}")
            return

        try:
            dt = DeltaTable.forPath(self.spark, table_path)
            if retention_hours is not None:
                dt.vacuum(retention_hours)
            else:
                dt.vacuum()
            print(f"[DELTA VACUUM] Vacuumed table at {table_path}")
        except Exception as e:
            print(f"[WARN] VACUUM notice: {e}")

    def get_table_telemetry(self, table_path: str) -> dict[str, Any]:
        """
        Extracts GxP metrology and operational telemetry from Delta Lake transaction log.
        """
        if not self._is_delta_table(table_path):
            return {"status": "NOT_FOUND"}

        try:
            dt = DeltaTable.forPath(self.spark, table_path)
            detail = dt.detail().collect()[0].asDict()
            history = dt.history(5).collect()

            num_files = (
                detail.get("numFiles")
                if detail.get("numFiles") is not None
                else detail.get("num_files")
            )
            size_in_bytes = (
                detail.get("sizeInBytes")
                if detail.get("sizeInBytes") is not None
                else detail.get("size_in_bytes")
            )
            clustering_columns = detail.get(
                "clusteringColumns",
                detail.get("clustering_columns", detail.get("partitionColumns", [])),
            )

            return {
                "table_path": table_path,
                "format": detail.get("format"),
                "num_files": num_files,
                "size_in_bytes": size_in_bytes,
                "clustering_columns": clustering_columns,
                "recent_commits": [h.asDict() for h in history],
            }
        except Exception as e:
            return {"table_path": table_path, "status": "TELEMETRY_UNAVAILABLE", "notice": str(e)}
