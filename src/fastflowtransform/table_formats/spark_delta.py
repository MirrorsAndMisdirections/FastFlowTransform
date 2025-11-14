# fastflowtransform/table_formats/spark_delta.py
from __future__ import annotations

from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame as SDF, SparkSession

from fastflowtransform.table_formats.base import SparkFormatHandler


class DeltaFormatHandler(SparkFormatHandler):
    """
    Delta Lake format handler using delta-spark's DeltaTable API.

    Responsibilities:
      - save_df_as_table() with format("delta").
      - incremental_insert(): default SparkFormatHandler implementation
        (INSERT INTO).
      - incremental_merge(): uses DeltaTable.merge()
        with whenMatchedUpdateAll / whenNotMatchedInsertAll.
    """

    def __init__(
        self,
        spark: SparkSession,
        *,
        table_options: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(spark, table_format="delta", table_options=table_options or {})

    # ---------- Core helpers ----------
    def _delta_table_for(self, table_name: str) -> DeltaTable:
        """
        Resolve a DeltaTable from a table name.

        This assumes a managed/catalog Delta table; unmanaged/path-based
        tables are handled via the storage layer and *not* by this handler.
        """
        try:
            return DeltaTable.forName(self.spark, table_name)
        except Exception as exc:  # pragma: no cover - error path
            raise RuntimeError(
                f"Delta table '{table_name}' does not exist "
                f"or is not registered as a Delta table: {exc}"
            ) from exc

    # ---------- Required API ----------
    def save_df_as_table(self, table_name: str, df: SDF) -> None:
        """
        Save DataFrame as a managed Delta table.

        Overwrites the table content:
          - writer.format("delta")
          - writer.mode("overwrite")
          - options from self.table_options
        """
        writer = df.write.format("delta").mode("overwrite")

        if self.table_options:
            writer = writer.options(**self.table_options)

        writer.saveAsTable(table_name)

    # ---------- Incremental API ----------
    # incremental_insert: base implementation is fine:
    #   INSERT INTO table SELECT ...
    # but we keep the signature here for clarity/override if needed.
    def incremental_insert(self, table_name: str, select_body_sql: str) -> None:
        super().incremental_insert(table_name, select_body_sql)

    def incremental_merge(
        self,
        table_name: str,
        select_body_sql: str,
        unique_key: list[str],
    ) -> None:
        """
        Delta MERGE implementation using DeltaTable.merge API.

        Semantics:
          - If unique_key is empty -> falls back to insert-only semantics.
          - Otherwise:
              MERGE INTO table AS t
              USING (<select_body_sql>) AS s
              ON  AND-joined equality on unique_key
              WHEN MATCHED THEN UPDATE SET *
              WHEN NOT MATCHED THEN INSERT *
        """
        body = select_body_sql.strip().rstrip(";\n\t ")
        if not unique_key:
            # No keys -> treat this as pure append.
            self.incremental_insert(table_name, body)
            return

        # Materialize the source DataFrame for the merge
        source_df = self.spark.sql(body)

        # Build the join predicate: t.k = s.k AND ...
        condition = " AND ".join([f"t.`{k}` = s.`{k}`" for k in unique_key])

        delta_tbl = self._delta_table_for(table_name)

        (
            delta_tbl.alias("t")
            .merge(source_df.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
