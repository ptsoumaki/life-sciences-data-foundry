"""Compatibility and shared constants module."""

from collections.abc import Callable
from typing import Any

configure_spark_with_delta_pip: Callable[..., Any] | None

try:
    from delta import configure_spark_with_delta_pip as _configure_spark_with_delta_pip

    configure_spark_with_delta_pip = _configure_spark_with_delta_pip
    HAS_DELTA = True
except ImportError:
    configure_spark_with_delta_pip = None
    HAS_DELTA = False
