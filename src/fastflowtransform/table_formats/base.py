# fastflowtransform/table_formats/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from fastflowtransform.typing import SDF, SparkSession


class SparkFormatHandler(ABC):
    """
    Abstract base for Spark table format handlers (Delta, Parquet, Iceberg, ...).

    Responsibilities:
      - Saving a DataFrame as a managed table.
      - Incremental INSERT semantics.
      - Optional incremental MERGE semantics (can raise NotImplementedError).

    This is intentionally minimal so that engines (DatabricksSparkExecutor)
    can:
      - Delegate managed table handling to the handler.
      - Still implement engine-level fallbacks for merge semantics.
    """

    def __init__(
        self,
        spark: SparkSession,
        *,
        table_format: str | None = None,
        table_options: dict[str, Any] | None = None,
        sql_runner: Callable[[str], Any] | None = None,
    ) -> None:
        self.spark = spark
        self.table_format: str | None = (table_format or "").lower() or None
        # Normalize options to strings for Spark
        self.table_options: dict[str, str] = {
            str(k): str(v) for k, v in (table_options or {}).items()
        }

        # central hook for executing SQL (can be engine-guarded)
        self._sql_runner: Callable[[str], Any] = sql_runner or spark.sql

    # ---- SQL helper ----
    def run_sql(self, sql: str) -> Any:
        """Execute SQL via the injected runner (guardable in the executor)."""
        return self._sql_runner(sql)

    # ---- Identifier helpers ----
    def qualify_identifier(self, table_name: str, *, database: str | None = None) -> str:
        """Return the physical table identifier for Spark APIs (unquoted)."""
        return (table_name or "").strip()

    def format_identifier_for_sql(self, table_name: str, *, database: str | None = None) -> str:
        """Return a SQL-safe identifier (per-part quoted) for the table."""
        ident = self.qualify_identifier(table_name, database=database)
        parts = [p for p in ident.split(".") if p]
        if not parts:
            return self._quote_part(ident)
        return ".".join(self._quote_part(part) for part in parts)

    def format_test_table(
        self, table_name: str | None, *, database: str | None = None
    ) -> str | None:
        if table_name is None:
            return None
        return self.format_identifier_for_sql(table_name, database=database)

    def allows_unmanaged_paths(self) -> bool:
        """Whether storage.path overrides should be honored for this format."""
        return True

    def relation_exists(self, table_name: str, *, database: str | None = None) -> bool:
        ident = self.qualify_identifier(table_name, database=database)
        try:
            return bool(self.spark.catalog.tableExists(ident))
        except Exception:
            return False

    @staticmethod
    def _quote_part(value: str) -> str:
        inner = (value or "").replace("`", "``")
        return f"`{inner}`"

    # ---- Required API ----
    @abstractmethod
    def save_df_as_table(self, table_name: str, df: SDF) -> None:
        """
        Save the given DataFrame as a (managed) table.

        The input name is the *fully-qualified* identifier Spark should use,
        e.g. "db.table" or just "table".
        """
        raise NotImplementedError

    # ---- Optional / defaulted API ----
    def incremental_insert(self, table_name: str, select_body_sql: str) -> None:
        """
        Default incremental INSERT implementation, format-agnostic.

        `select_body_sql` must be a *SELECT-able* body (no trailing semicolon),
        e.g. "SELECT ... FROM ...".
        """
        body = select_body_sql.strip().rstrip(";\n\t ")
        if not body.lower().startswith("select"):
            # This is a guard; DatabricksSparkExecutor uses _selectable_body already.
            raise ValueError(f"incremental_insert expects SELECT body, got: {body[:40]!r}")
        self.run_sql(f"INSERT INTO {table_name} {body}")

    def incremental_merge(
        self,
        table_name: str,
        select_body_sql: str,
        unique_key: list[str],
    ) -> None:
        """
        Optional: incremental MERGE semantics (UPSERT-like).
        Subclasses may override this. Default: not supported.

        Engines using this handler MUST be prepared to handle NotImplementedError
        and fall back to a more generic strategy.
        """
        raise NotImplementedError(
            f"incremental_merge is not implemented for format '{self.table_format or 'default'}'"
        )
