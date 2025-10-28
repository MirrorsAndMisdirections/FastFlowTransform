# fastflowtransform/executors/duckdb_exec.py
from __future__ import annotations

from collections.abc import Iterable
from contextlib import suppress
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from duckdb import CatalogException

from fastflowtransform.core import Node, relation_for
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.meta import ensure_meta_table, upsert_meta


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


class DuckExecutor(BaseExecutor[pd.DataFrame]):
    ENGINE_NAME = "duckdb"

    def __init__(self, db_path: str = ":memory:"):
        if db_path and db_path != ":memory:" and "://" not in db_path:
            with suppress(Exception):
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.con = duckdb.connect(db_path)

    def clone(self) -> DuckExecutor:
        """
        Generates a new Executor instance with own connection for Thread-Worker.
        """
        return DuckExecutor(self.db_path)

    # ---- Frame hooks ----
    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> pd.DataFrame:
        try:
            return self.con.table(relation).df()
        except CatalogException as e:
            existing = [
                r[0]
                for r in self.con.execute(
                    "select table_name from information_schema.tables "
                    "where table_schema in ('main','temp')"
                ).fetchall()
            ]
            raise RuntimeError(
                f"Dependency table not found: '{relation}'\n"
                f"Deps: {list(deps)}\nExisting tables: {existing}\n"
                "Hinweis: gleiche Datei-DB/Connection für Seeding & Run verwenden."
            ) from e

    def _materialize_relation(self, relation: str, df: pd.DataFrame, node: Node) -> None:
        tmp = "_ff_py_out"
        try:
            self.con.register(tmp, df)
            self.con.execute(f'create or replace table "{relation}" as select * from "{tmp}"')
        finally:
            try:
                self.con.unregister(tmp)
            except Exception:
                self.con.execute(f'drop view if exists "{tmp}"')

    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        self.con.execute(f'create or replace view "{view_name}" as select * from "{backing_table}"')

    def _frame_name(self) -> str:
        return "pandas"

    # ---- SQL hooks ----
    def _format_relation_for_ref(self, name: str) -> str:
        return _q(relation_for(name))

    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        location = cfg.get("location")
        if location:
            raise NotImplementedError("DuckDB executor does not support path-based sources yet.")

        identifier = cfg.get("identifier")
        if not identifier:
            raise KeyError(f"Source {source_name}.{table_name} missing identifier")

        parts = [
            p
            for p in (
                cfg.get("catalog") or cfg.get("database"),
                cfg.get("schema"),
                identifier,
            )
            if p
        ]
        if not parts:
            parts = [identifier]

        return ".".join(_q(str(part)) for part in parts)

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        self.con.execute(f"create or replace view {target_sql} as {select_body}")

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        self.con.execute(f"create or replace table {target_sql} as {select_body}")

    # ---- Meta hook ----
    def on_node_built(self, node: Node, relation: str, fingerprint: str) -> None:
        """
        After successful materialization, ensure the meta table exists and upsert the row.
        """
        # Best-effort: do not let meta errors break the run
        try:
            ensure_meta_table(self)
            upsert_meta(self, node.name, relation, fingerprint, "duckdb")
        except Exception:
            pass

    # ── Incremental API ────────────────────────────────────────────────────
    def exists_relation(self, relation: str) -> bool:
        sql = """
          select 1
          from information_schema.tables
          where table_schema in ('main','temp')
            and lower(table_name) = lower(?)
          limit 1
        """
        res = self.con.execute(sql, [relation]).fetchone()
        return bool(res)

    def create_table_as(self, relation: str, select_sql: str) -> None:
        # Use only the SELECT body and strip trailing semicolons for safety.
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        self.con.execute(f"create table {relation} as {body}")

    def incremental_insert(self, relation: str, select_sql: str) -> None:
        # Ensure the inner SELECT is clean (no trailing semicolon; SELECT body only).
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        self.con.execute(f"insert into {relation} {body}")

    def incremental_merge(self, relation: str, select_sql: str, unique_key: list[str]) -> None:
        """
        Fallback strategy:
          - Staging-CTE: data from SELECT
          - Delete-Merge: delete collisions in target
          - Insert all staging rows
        """
        keys_pred = " AND ".join([f"t.{k}=s.{k}" for k in unique_key])
        # Clean inner SELECT for CTE: remove trailing semicolon and keep only SELECT body
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        sql = f"""
        with src as ({body})
        delete from {relation} t using src s where {keys_pred};
        insert into {relation} select * from src;
        """
        self.con.execute(sql)

    def alter_table_sync_schema(
        self, relation: str, select_sql: str, *, mode: str = "append_new_columns"
    ) -> None:
        """
        Best-effort: add new columns with inferred type.
        """
        # Probe: empty projection from the SELECT (cleaned to avoid parser issues).
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        probe = self.con.execute(f"select * from ({body}) as q limit 0")
        cols = [c[0] for c in probe.description or []]
        # vorhandene Spalten
        existing = {
            r[0]
            for r in self.con.execute(
                "select column_name from information_schema.columns "
                + "where lower(table_name)=lower(?)",
                [relation],
            ).fetchall()
        }
        add = [c for c in cols if c not in existing]
        for c in add:
            # Typ heuristisch: typeof aus einer CAST-Probe; fallback VARCHAR
            try:
                # Versuche Typ aus Expression abzuleiten (best effort)
                self.con.execute(f"alter table {relation} add column {c} varchar")
            except Exception:
                self.con.execute(f"alter table {relation} add column {c} varchar")
