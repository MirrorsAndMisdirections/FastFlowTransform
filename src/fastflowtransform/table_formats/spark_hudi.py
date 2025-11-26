from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastflowtransform.table_formats.base import SparkFormatHandler
from fastflowtransform.typing import SDF, SparkSession


class HudiFormatHandler(SparkFormatHandler):
    """
    Hudi format handler using Spark's Hudi integration.

    Responsibilities:
      - save_df_as_table() via df.write.format("hudi").saveAsTable(...)
      - incremental_insert(): INSERT INTO ... SELECT ...
      - incremental_merge(): MERGE INTO ... USING (...) WHEN MATCHED/NOT MATCHED ...
        (Hudi's Spark MERGE support must be enabled in the cluster).
    """

    def __init__(
        self,
        spark: SparkSession,
        *,
        default_database: str | None = None,
        table_options: dict[str, Any] | None = None,
        sql_runner: Callable[[str], Any] | None = None,
    ) -> None:
        # table_format="hudi" so the base class knows what we're dealing with
        super().__init__(
            spark,
            table_format="hudi",
            table_options=table_options or {},
            sql_runner=sql_runner,
        )
        self.default_database = default_database or spark.catalog.currentDatabase()

    # ---------- Core helpers ----------
    def _qualify_table_name(self, table_name: str, database: str | None = None) -> str:
        """
        Normalize input like "seed_events" or "db.seed_events" to "db.seed_events".

        For Hudi we normally rely on the current Spark catalog / Hive metastore,
        so there is no extra "catalog." prefix like in Iceberg.
        """
        raw = (table_name or "").strip()
        if not raw:
            raise ValueError("Empty table name for HudiFormatHandler")

        parts = [p for p in raw.split(".") if p]
        if len(parts) == 1:
            db = database or self.default_database
            return ".".join([db, parts[0]])
        # already db.table or catalog.db.table - just pass through
        if len(parts) == 2:
            return ".".join(parts)
        return ".".join(parts)

    # ---------- Identifier overrides ----------
    def qualify_identifier(self, table_name: str, *, database: str | None = None) -> str:
        # For Spark SQL we just use db.table, no extra quoting here - the caller
        # can quote if needed.
        return self._qualify_table_name(table_name, database=database)

    def allows_unmanaged_paths(self) -> bool:
        # Hudi can work as a path-based table as well, so we allow that.
        # (Your higher-level executor can still decide whether to use paths.)
        return True

    def relation_exists(self, table_name: str, *, database: str | None = None) -> bool:
        ident = self.qualify_identifier(table_name, database=database)
        try:
            return self.spark.catalog.tableExists(ident)
        except Exception:
            return False

    # ---------- Required API ----------
    def save_df_as_table(self, table_name: str, df: SDF) -> None:
        """
        Save DataFrame as a Hudi table registered in the current catalog.

        Typical Hudi options you might pass via table_options include:
          - hoodie.datasource.write.recordkey.field
          - hoodie.datasource.write.precombine.field
          - hoodie.table.name (optional when using saveAsTable)
        """
        full_name = self._qualify_table_name(table_name)

        writer = df.write.format("hudi")
        for k, v in self.table_options.items():
            writer = writer.option(str(k), str(v))

        # Full refresh semantics: overwrite the Hudi table
        writer.mode("overwrite").saveAsTable(full_name)

    # ---------- Incremental API ----------
    def incremental_insert(self, table_name: str, select_body_sql: str) -> None:
        """
        Append-only incremental load.

        Uses Spark SQL INSERT INTO; the Hudi connector will handle this as an
        insert/upsert depending on table configuration.
        """
        body = select_body_sql.strip().rstrip(";\n\t ")
        if not body.lower().startswith("select"):
            raise ValueError(f"incremental_insert expects SELECT body, got: {body[:40]!r}")

        full_name = self._qualify_table_name(table_name)
        self.run_sql(f"INSERT INTO {full_name} {body}")

    def incremental_merge(
        self,
        table_name: str,
        select_body_sql: str,
        unique_key: list[str],
    ) -> None:
        """
        Hudi MERGE implementation.

            MERGE INTO db.table AS t
            USING (<select_body_sql>) AS s
            ON  AND-joined equality on unique_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *

        This requires Hudi's MERGE support to be enabled on your Spark cluster.
        """
        body = select_body_sql.strip().rstrip(";\n\t ")
        if not unique_key:
            # No key - fall back to simple insert
            self.incremental_insert(table_name, body)
            return

        full_name = self._qualify_table_name(table_name)
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
