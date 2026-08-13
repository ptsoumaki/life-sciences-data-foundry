"""
Compatibility and shared constants module.
"""

try:
    from delta import configure_spark_with_delta_pip
    HAS_DELTA = True
except ImportError:
    HAS_DELTA = False
    configure_spark_with_delta_pip = None
