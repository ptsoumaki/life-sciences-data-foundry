"""
Module: compat.py
Description: Compatibility shim providing conditional Delta Lake imports and the HAS_DELTA
             capability flag. All modules that require optional Delta Lake functionality import
             from this module to ensure graceful degradation to Parquet on environments where
             delta-spark is not installed (e.g., local development without Hadoop native libraries).
"""

from typing import Any

try:
    from delta import configure_spark_with_delta_pip as _configure_spark_with_delta_pip
    from delta.tables import DeltaTable as _DeltaTable

    configure_spark_with_delta_pip: Any = _configure_spark_with_delta_pip
    DeltaTable: Any = _DeltaTable
    HAS_DELTA = True
except ImportError:
    configure_spark_with_delta_pip = None
    DeltaTable = None
    HAS_DELTA = False
