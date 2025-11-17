from __future__ import annotations

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
    ) -> None:
        super().__init__(spark, table_format="iceberg", table_options=table_options or {})
        self.catalog_name = "iceberg"

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
            self.spark.table(ident)
            return True
        except Exception:
            return False

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

        # Upsert semantics for seeds / full-refresh
        writer.createOrReplace()

    # ---------- Incremental API ----------
    def incremental_insert(self, table_name: str, select_body_sql: str) -> None:
        body = select_body_sql.strip().rstrip(";\n\t ")
        if not body.lower().startswith("select"):
            raise ValueError(f"incremental_insert expects SELECT body, got: {body[:40]!r}")

        full_name = self._qualify_table_name(table_name)
        self.spark.sql(f"INSERT INTO {full_name} {body}")

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

        full_name = self._qualify_table_name(table_name)
        pred = " AND ".join([f"t.`{k}` = s.`{k}`" for k in unique_key])

        self.spark.sql(
            f"""
            MERGE INTO {full_name} AS t
            USING ({body}) AS s
            ON {pred}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
