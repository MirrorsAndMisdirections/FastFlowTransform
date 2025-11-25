# fastflowtransform/table_formats/spark_delta.py
from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from fastflowtransform.table_formats.base import SparkFormatHandler
from fastflowtransform.typing import SDF, SparkSession

if TYPE_CHECKING:  # pragma: no cover - typing only
    from delta.tables import DeltaTable
else:  # pragma: no cover - runtime import
    try:
        from delta.tables import DeltaTable  # type: ignore
    except Exception:

        class DeltaTable:  # type: ignore[misc]
            """Fallback stub when delta-spark is unavailable."""

            def __init__(self, *args: Any, **kwargs: Any) -> None:
                raise ImportError(
                    "delta-spark is required for DeltaFormatHandler. "
                    "Install fastflowtransform[spark] or delta-spark."
                )


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
        sql_runner: Callable[[str], Any] | None = None,
    ) -> None:
        super().__init__(
            spark,
            table_format="delta",
            table_options=table_options or {},
            sql_runner=sql_runner,
        )

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

    @staticmethod
    def _quote_identifier(name: str) -> str:
        parts = [p for p in name.split(".") if p]
        if not parts:
            esc = name.replace("`", "``")
            return f"`{esc}`"
        return ".".join(f"`{part.replace('`', '``')}`" for part in parts)

    @staticmethod
    def _sql_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _restore_table_metadata(
        self,
        table_ident: str,
        *,
        table_comment: str | None,
        column_comments: dict[str, str],
        table_properties: dict[str, Any],
    ) -> None:
        if table_comment:
            with suppress(Exception):
                self.run_sql(
                    f"COMMENT ON TABLE {table_ident} IS {self._sql_literal(table_comment)}"
                )

        if table_properties:
            assignments = []
            for key, value in table_properties.items():
                if value is None:
                    continue
                key_str = str(key)
                if key_str.lower() in {"transient_lastddltime"}:
                    continue
                assignments.append(f"{self._sql_literal(key_str)}={self._sql_literal(str(value))}")
            if assignments:
                props_sql = ", ".join(assignments)
                with suppress(Exception):
                    self.run_sql(f"ALTER TABLE {table_ident} SET TBLPROPERTIES ({props_sql})")

        for col, comment in column_comments.items():
            if not comment:
                continue
            col_ident = f"{table_ident}.{self._quote_identifier(col)}"
            with suppress(Exception):
                self.run_sql(f"COMMENT ON COLUMN {col_ident} IS {self._sql_literal(comment)}")

    # ---------- Required API ----------
    def save_df_as_table(self, table_name: str, df: SDF) -> None:
        """
        Save DataFrame as a managed Delta table.

        For existing tables we bypass Hive's ALTER TABLE path by overwriting the
        physical Delta location directly (with schema overwrite) and refreshing
        the table metadata. New tables go through saveAsTable so they are
        registered in the metastore.
        """

        def _writer() -> Any:
            w = df.write.format("delta").mode("overwrite")
            if self.table_options:
                w = w.options(**self.table_options)
            return w

        exists = False
        try:
            exists = self.spark.catalog.tableExists(table_name)
        except Exception:
            exists = False

        if not exists:
            _writer().saveAsTable(table_name)
            return

        table_comment: str | None = None
        table_properties: dict[str, Any] = {}
        column_comments: dict[str, str] = {}
        table_ident = self._quote_identifier(table_name)

        try:
            info = self.spark.catalog.getTable(table_name)
            table_comment = getattr(info, "description", None)
            props = getattr(info, "properties", None)
            if isinstance(props, dict):
                table_properties = dict(props)
        except Exception:
            pass

        try:
            cols = self.spark.catalog.listColumns(table_name)
            column_comments = {}
            for col in cols:
                comment = getattr(col, "comment", None)
                if comment:
                    column_comments[col.name] = comment
        except Exception:
            column_comments = {}

        location: str | None = None
        try:
            detail = self._delta_table_for(table_name).detail().collect()
            if detail:
                location = detail[0].get("location") or detail[0].get("path")
        except Exception:
            location = None

        if not location:
            try:
                info = self.spark.catalog.getTable(table_name)
                location = getattr(info, "location", None)
            except Exception:
                location = None

        if not location:
            # Fallback: drop and recreate if we can't resolve the location.
            with suppress(Exception):
                self.run_sql(f"DROP TABLE IF EXISTS {table_ident}")
            _writer().saveAsTable(table_name)
            self._restore_table_metadata(
                table_ident,
                table_comment=table_comment,
                column_comments=column_comments,
                table_properties=table_properties,
            )
            return

        _writer().option("overwriteSchema", "true").save(location)
        with suppress(Exception):
            self.spark.catalog.refreshTable(table_name)
        self._restore_table_metadata(
            table_ident,
            table_comment=table_comment,
            column_comments=column_comments,
            table_properties=table_properties,
        )

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
        source_df = self.run_sql(body)

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
