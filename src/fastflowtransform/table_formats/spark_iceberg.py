from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from typing import Any

from fastflowtransform.table_formats.base import SparkFormatHandler
from fastflowtransform.typing import SDF, SparkSession


class IcebergFormatHandler(SparkFormatHandler):
    """
    Iceberg format handler using Spark's Iceberg integration.

    Responsibilities:
      - save_df_as_table() with format("iceberg").
      - incremental_insert(): default SparkFormatHandler implementation
        (INSERT INTO).
      - incremental_merge(): uses Spark SQL MERGE INTO ... USING (...) syntax,
        which Iceberg supports when the catalog is configured for Iceberg.
    """

    def __init__(
        self,
        spark: SparkSession,
        *,
        table_options: dict[str, Any] | None = None,
        sql_runner: Callable[[str], Any] | None = None,
    ) -> None:
        options = dict(table_options or {})
        catalog = options.pop("catalog_name", None) or options.pop("__catalog_name__", None)
        self.catalog_name = str(catalog) if catalog else "iceberg"
        super().__init__(
            spark,
            table_format="iceberg",
            table_options=options,
            sql_runner=sql_runner,
        )

    # ---------- Core helpers ----------
    def _qualify_table_name(self, table_name: str, database: str | None = None) -> str:
        """
        Normalize arbitrary input like "seed_events" or "db.seed_events"
        to the fully-qualified Iceberg identifier "iceberg.db.seed_events".
        """
        raw = (table_name or "").strip()
        if not raw:
            raise ValueError("Empty table name for IcebergFormatHandler")

        parts = [p for p in raw.split(".") if p]
        cat = self.catalog_name

        if len(parts) == 1:
            # table → iceberg.<current_db>.table
            db = database or self.spark.catalog.currentDatabase()
            return ".".join([cat, db, parts[0]])
        if len(parts) == 2:
            # db.table → iceberg.db.table
            return ".".join([cat, *parts])
        # len >= 3: assume already catalog.db.table
        return ".".join(parts)

    # ---------- Identifier overrides ----------
    def qualify_identifier(self, table_name: str, *, database: str | None = None) -> str:
        return self._qualify_table_name(table_name, database=database)

    def allows_unmanaged_paths(self) -> bool:
        return False

    def relation_exists(self, table_name: str, *, database: str | None = None) -> bool:
        ident = self.qualify_identifier(table_name, database=database)
        try:
            return bool(self.spark.catalog.tableExists(ident))
        except Exception:
            return False

    @staticmethod
    def _quote_part(value: str) -> str:
        return f"`{value.replace('`', '``')}`"

    def _sql_identifier(self, table_name: str, *, database: str | None = None) -> str:
        qualified = self._qualify_table_name(table_name, database=database)
        parts = [p for p in qualified.split(".") if p]
        return ".".join(self._quote_part(part) for part in parts)

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
                props = ", ".join(assignments)
                with suppress(Exception):
                    self.run_sql(f"ALTER TABLE {table_ident} SET TBLPROPERTIES ({props})")

        for name, comment in column_comments.items():
            if not comment:
                continue
            col_ident = f"{table_ident}.{self._quote_part(name)}"
            with suppress(Exception):
                self.run_sql(f"COMMENT ON COLUMN {col_ident} IS {self._sql_literal(comment)}")

    # ---------- Required API ----------
    def save_df_as_table(self, table_name: str, df: SDF) -> None:
        """
        Save DataFrame as an Iceberg table in the configured catalog.

        Uses DataFrameWriterV2:

            df.writeTo("iceberg.db.table").using("iceberg").createOrReplace()
        """
        full_name = self._qualify_table_name(table_name)
        writer = df.writeTo(full_name).using("iceberg")
        for k, v in self.table_options.items():
            writer = writer.tableProperty(str(k), str(v))

        existed = False
        table_comment: str | None = None
        table_properties: dict[str, Any] = {}
        column_comments: dict[str, str] = {}
        table_ident = self._sql_identifier(table_name)

        try:
            existed = bool(self.spark.catalog.tableExists(full_name))
        except Exception:
            existed = False

        if existed:
            try:
                info = self.spark.catalog.getTable(full_name)
                table_comment = getattr(info, "description", None)
                props = getattr(info, "properties", None)
                if isinstance(props, dict):
                    table_properties = dict(props)
            except Exception:
                pass

            try:
                cols = self.spark.catalog.listColumns(full_name)
                for col in cols:
                    comment = getattr(col, "comment", None)
                    if comment:
                        column_comments[col.name] = comment
            except Exception:
                column_comments = {}

        # Upsert semantics for seeds / full-refresh
        writer.createOrReplace()

        if existed:
            self._restore_table_metadata(
                table_ident,
                table_comment=table_comment,
                column_comments=column_comments,
                table_properties=table_properties,
            )

    # ---------- Incremental API ----------
    def incremental_insert(self, table_name: str, select_body_sql: str) -> None:
        body = select_body_sql.strip().rstrip(";\n\t ")
        if not body.lower().startswith("select"):
            raise ValueError(f"incremental_insert expects SELECT body, got: {body[:40]!r}")

        full_name = self._sql_identifier(table_name)
        self.run_sql(f"INSERT INTO {full_name} {body}")

    def incremental_merge(
        self,
        table_name: str,
        select_body_sql: str,
        unique_key: list[str],
    ) -> None:
        """
        Iceberg MERGE implementation.

            MERGE INTO iceberg.db.table AS t
            USING (<select_body_sql>) AS s
            ON  AND-joined equality on unique_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        body = select_body_sql.strip().rstrip(";\n\t ")
        if not unique_key:
            self.incremental_insert(table_name, body)
            return

        full_name = self._sql_identifier(table_name)
        pred = " AND ".join([f"t.`{k}` = s.`{k}`" for k in unique_key])

        self.run_sql(
            f"""
            MERGE INTO {full_name} AS t
            USING ({body}) AS s
            ON {pred}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
