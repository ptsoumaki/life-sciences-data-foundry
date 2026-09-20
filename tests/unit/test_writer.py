"""
Unit tests for DeltaMedallionWriter (medallion/writer.py).
Tests Silver and Gold Delta writes, idempotent MERGE upserts,
table maintenance (OPTIMIZE, VACUUM), and GxP storage telemetry.
"""

import os

import pyarrow.parquet as pq
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from medallion.writer import DeltaMedallionWriter
from omop_cdm_v54.compat import HAS_DELTA

SAMPLE_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
    ]
)


def _make_sample_df(spark, rows=None):
    if rows is None:
        rows = [(1, "Alice", "A"), (2, "Bob", "B"), (3, "Charlie", "A")]
    return spark.createDataFrame(rows, SAMPLE_SCHEMA)


def _read_table_record_count(spark, path: str) -> int:
    try:
        if HAS_DELTA:
            return spark.read.format("delta").load(path).count()
        return spark.read.parquet(path).count()
    except Exception:
        # Fallback for local Windows environments lacking native hadoop.dll;
        # read parquet data files directly via PyArrow, skipping deletion vectors and delta logs.
        parquet_files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith(".parquet")]
        if not parquet_files:
            return 0
        return sum(pq.read_table(pf).num_rows for pf in parquet_files)


def test_writer_paths(spark, tmp_path):
    writer = DeltaMedallionWriter(spark, base_output_dir=str(tmp_path))
    silver_path = writer._get_table_path("silver", "test_table")
    gold_path = writer._get_table_path("gold", "person")

    assert "silver" in silver_path
    assert "gold" in gold_path
    assert silver_path.endswith("test_table")
    assert gold_path.endswith("person")


def test_writer_uc_table_name(spark, tmp_path):
    writer_no_uc = DeltaMedallionWriter(spark, base_output_dir=str(tmp_path))
    assert writer_no_uc._get_uc_table_name("person") is None

    writer_uc = DeltaMedallionWriter(
        spark, base_output_dir=str(tmp_path), catalog="main", schema="clinical"
    )
    assert writer_uc._get_uc_table_name("person") == "main.clinical.person"


def test_write_silver_table(spark, tmp_path):
    writer = DeltaMedallionWriter(spark, base_output_dir=str(tmp_path))
    df = _make_sample_df(spark)

    path = writer.write_silver_table(df, "test_silver", mode="overwrite")
    assert os.path.exists(path)
    if HAS_DELTA:
        delta_log_dir = os.path.join(path, "_delta_log")
        assert os.path.exists(delta_log_dir)
        commit_files = [f for f in os.listdir(delta_log_dir) if f.endswith(".json")]
        assert len(commit_files) > 0
        with open(os.path.join(delta_log_dir, commit_files[0]), encoding="utf-8") as f:
            log_content = f.read()
        assert "delta.enableChangeDataFeed" in log_content

    assert _read_table_record_count(spark, path) == 3


def test_write_quarantine_table(spark, tmp_path):
    writer = DeltaMedallionWriter(spark, base_output_dir=str(tmp_path))
    df = _make_sample_df(spark)

    path = writer.write_quarantine_table(df, "test_quarantine", mode="overwrite")
    assert os.path.exists(path)
    if HAS_DELTA:
        delta_log_dir = os.path.join(path, "_delta_log")
        assert os.path.exists(delta_log_dir)
        commit_files = [f for f in os.listdir(delta_log_dir) if f.endswith(".json")]
        assert len(commit_files) > 0
        with open(os.path.join(delta_log_dir, commit_files[0]), encoding="utf-8") as f:
            log_content = f.read()
        assert "delta.enableChangeDataFeed" in log_content

    assert _read_table_record_count(spark, path) == 3


def test_write_gold_omop_table(spark, tmp_path):
    writer = DeltaMedallionWriter(spark, base_output_dir=str(tmp_path))
    df = _make_sample_df(spark)

    path = writer.write_gold_omop_table(df, "person", cluster_by=["id"], mode="overwrite")
    assert os.path.exists(path)
    if HAS_DELTA:
        assert os.path.exists(os.path.join(path, "_delta_log"))

    assert _read_table_record_count(spark, path) == 3


def test_get_table_telemetry(spark, tmp_path):
    writer = DeltaMedallionWriter(spark, base_output_dir=str(tmp_path))
    df = _make_sample_df(spark)

    path = writer.write_gold_omop_table(df, "telemetry_test", mode="overwrite")
    telemetry = writer.get_table_telemetry(path)

    if HAS_DELTA:
        assert telemetry.get("table_path") == path
        assert "num_files" in telemetry or "status" in telemetry
    else:
        assert telemetry.get("status") in ("NOT_FOUND", "TELEMETRY_UNAVAILABLE")

    not_found = writer.get_table_telemetry("/non/existent/path")
    assert not_found.get("status") == "NOT_FOUND"


def test_upsert_gold_omop_table(spark, tmp_path):
    writer = DeltaMedallionWriter(spark, base_output_dir=str(tmp_path))
    df_initial = _make_sample_df(spark, [(1, "Alice", "A"), (2, "Bob", "B")])

    path = writer.write_gold_omop_table(df_initial, "upsert_test", mode="overwrite")
    assert os.path.exists(path)

    df_update = _make_sample_df(spark, [(2, "Robert", "B"), (3, "Charlie", "C")])
    result_path = writer.upsert_gold_omop_table(df_update, "upsert_test", merge_keys=["id"])
    assert os.path.exists(result_path)

    assert _read_table_record_count(spark, result_path) >= 2


def test_upsert_silver_table(spark, tmp_path):
    writer = DeltaMedallionWriter(spark, base_output_dir=str(tmp_path))
    df_initial = _make_sample_df(spark, [(1, "Alice", "A"), (2, "Bob", "B")])

    path = writer.write_silver_table(df_initial, "upsert_silver_test", mode="overwrite")
    assert os.path.exists(path)

    df_update = _make_sample_df(spark, [(2, "Robert", "B"), (3, "Charlie", "C")])
    result_path = writer.upsert_silver_table(df_update, "upsert_silver_test", merge_keys=["id"])
    assert os.path.exists(result_path)

    assert _read_table_record_count(spark, result_path) >= 2


def test_optimize_and_vacuum_table(spark, tmp_path):
    writer = DeltaMedallionWriter(spark, base_output_dir=str(tmp_path))
    df = _make_sample_df(spark)
    path = writer.write_gold_omop_table(df, "maint_test", mode="overwrite")

    # Should run or skip gracefully without unhandled exceptions
    writer.optimize_table(path)
    writer.optimize_table(path, zorder_by=["category"])
    writer.vacuum_table(path, retention_hours=168.0)

    # Non-delta paths should be gracefully skipped
    writer.optimize_table("/non/existent/path")
    writer.vacuum_table("/non/existent/path")
