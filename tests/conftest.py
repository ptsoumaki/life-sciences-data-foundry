"""
Pytest configuration and shared session-scoped PySpark fixtures for the OMOP CDM pipeline test suite.
"""

import os
import sys
import pytest
from pyspark.sql import SparkSession
from omop_cdm_v54.compat import HAS_DELTA, configure_spark_with_delta_pip
from omop_cdm_v54.pipeline import configure_windows_hadoop_environment


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession with Delta Lake extensions for unit and integration tests.

    Configured for minimal resource usage (2 local threads, Spark UI disabled).
    Delegates Windows Hadoop environment setup to configure_windows_hadoop_environment()
    from pipeline.py to keep that logic in a single canonical location.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    configure_windows_hadoop_environment()

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("PySpark-OMOP-Unit-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

    if HAS_DELTA:
        builder = builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        session = builder.getOrCreate()

    session.sparkContext.setLogLevel("WARN")

    yield session

    session.stop()
