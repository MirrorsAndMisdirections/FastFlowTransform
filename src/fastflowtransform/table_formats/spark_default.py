# fastflowtransform/table_formats/spark_default.py
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastflowtransform.table_formats.base import SparkFormatHandler
from fastflowtransform.typing import SDF, SparkSession


class DefaultSparkFormatHandler(SparkFormatHandler):
    """
    Default Spark format handler for non-Delta managed tables
    (e.g. Parquet, ORC, generic catalog tables).

    Responsibilities:
      - save_df_as_table() using DataFrameWriter.saveAsTable.
      - incremental_insert() uses the base implementation (INSERT INTO ...).
      - incremental_merge() is intentionally NOT implemented and is expected
        to be handled by the executor via a generic fallback.
    """

    def __init__(
        self,
        spark: SparkSession,
        *,
        table_format: str | None = None,
        table_options: dict[str, Any] | None = None,
        sql_runner: Callable[[str], Any] | None = None,
    ) -> None:
        super().__init__(
            spark,
            table_format=table_format,
            table_options=table_options,
            sql_runner=sql_runner,
        )

    def save_df_as_table(self, table_name: str, df: SDF) -> None:
        """
        Save DataFrame as a managed table using Spark's built-in formats.

        - Overwrites the table content.
        - Uses self.table_format (if provided) as the writer format.
        - Applies self.table_options as writer options.
        """
        writer = df.write.mode("overwrite")

        if self.table_format:
            writer = writer.format(self.table_format)

        if self.table_options:
            writer = writer.options(**self.table_options)

        writer.saveAsTable(table_name)
