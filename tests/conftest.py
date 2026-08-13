"""
Pytest configuration and shared session-scoped PySpark fixtures.
"""

import os
import sys
import pytest
from pyspark.sql import SparkSession

# Ensure repository root and analytical-layer directories are in sys.path
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
analytical_dir = os.path.join(base_dir, "analytical-layer")
for p in [base_dir, analytical_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)


@pytest.fixture(scope="session")
def spark():
    """
    Provides a lightweight, session-scoped PySpark SparkSession for fast local unit testing.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Configure dummy Hadoop environment on Windows to avoid winutils warnings
    if os.name == 'nt':
        hadoop_dir = os.path.join(base_dir, "hadoop")
        bin_dir = os.path.join(hadoop_dir, "bin")
        os.makedirs(bin_dir, exist_ok=True)
        winutils_path = os.path.join(bin_dir, "winutils.exe")
        if not os.path.exists(winutils_path) or os.path.getsize(winutils_path) < 100:
            csc = r"C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe"
            cs_path = os.path.join(bin_dir, "dummy.cs")
            if os.path.exists(csc):
                try:
                    import subprocess
                    with open(cs_path, "w") as f:
                        f.write("class Program { static int Main(string[] args) { return 0; } }\n")
                    subprocess.run([csc, "/nologo", f"/out:{winutils_path}", cs_path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except Exception:
                    pass
        os.environ["HADOOP_HOME"] = hadoop_dir
        os.environ["hadoop.home.dir"] = hadoop_dir

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("PySpark-OMOP-Unit-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")

    yield session

    session.stop()
