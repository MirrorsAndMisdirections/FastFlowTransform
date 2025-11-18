# fastflowtransform/table_formats/__init__.py
from __future__ import annotations

from typing import Any

from fastflowtransform.typing import SparkSession

from .base import SparkFormatHandler
from .spark_default import DefaultSparkFormatHandler
from .spark_delta import DeltaFormatHandler
from .spark_hudi import HudiFormatHandler
from .spark_iceberg import IcebergFormatHandler

# Mapping: normalized format name -> handler class
_SPARK_FORMAT_REGISTRY: dict[str, type[SparkFormatHandler]] = {
    "delta": DeltaFormatHandler,
    "iceberg": IcebergFormatHandler,
    "hudi": HudiFormatHandler,
}


def register_spark_format(
    name: str,
    handler_cls: type[SparkFormatHandler],
) -> None:
    """
    Register or override a Spark format handler.

    This can be used by extensions/plug-ins to add new formats without
    touching core code.
    """
    _SPARK_FORMAT_REGISTRY[name.lower()] = handler_cls


def get_spark_format_handler(
    table_format: str | None,
    spark: SparkSession,
    *,
    table_options: dict[str, Any] | None = None,
) -> SparkFormatHandler:
    """
    Factory for SparkFormatHandler based on a logical format name.

    - If a specific handler is registered (delta, iceberg, ...), use it.
    - Otherwise fall back to DefaultSparkFormatHandler with `table_format`.
    """
    fmt = (table_format or "").lower()
    handler_cls = _SPARK_FORMAT_REGISTRY.get(fmt)

    if handler_cls is not None:
        # Handlers like DeltaFormatHandler/IcebergFormatHandler don't need table_format
        return handler_cls(spark, table_options=table_options or {})

    # Fallback: generic Spark format handler (parquet/orc/etc.)
    return DefaultSparkFormatHandler(
        spark,
        table_format=fmt or None,
        table_options=table_options or {},
    )
