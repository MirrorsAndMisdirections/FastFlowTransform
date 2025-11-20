# fastflowtransform/executors/_spark_imports.py
from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING

__all__ = ["get_spark_functions", "get_spark_window"]


def _spark_missing_error(exc: Exception) -> RuntimeError:
    return RuntimeError(
        "pyspark is required for Spark/Databricks executors. "
        "Install the extra: fastflowtransform[spark]."
    )


if TYPE_CHECKING:  # pragma: no cover - typing only
    # We import these only for static typing.
    from pyspark.sql import (
        Window,
    )

    # `Window` itself is a class with static constructors (partitionBy, orderBy, ...),
    # so using it directly as the return type is fine.
    def get_spark_window() -> type[Window]:  # Window is a class
        ...

    # `functions` is a module; for typing purposes we just expose it as ModuleType.
    def get_spark_functions() -> ModuleType: ...

else:
    # Runtime implementations - no need to annotate; type-checkers use the stubs above.
    def get_spark_window():
        """
        Lazy import for pyspark.sql.Window.

        Raises:
            RuntimeError: if pyspark is not installed or import fails.
        """
        try:
            from pyspark.sql import Window  # noqa PLC0415
        except Exception as exc:  # pragma: no cover
            raise _spark_missing_error(exc) from exc
        return Window

    def get_spark_functions():
        """
        Lazy import for pyspark.sql.functions as F.

        Raises:
            RuntimeError: if pyspark is not installed or import fails.
        """
        try:
            from pyspark.sql import functions as F  # noqa PLC0415
        except Exception as exc:  # pragma: no cover
            raise _spark_missing_error(exc) from exc
        return F
