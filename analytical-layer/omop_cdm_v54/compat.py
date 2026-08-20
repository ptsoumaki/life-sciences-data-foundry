"""Compatibility and shared constants module."""

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
