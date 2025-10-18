# flowforge/executors/duckdb_exec.py
from __future__ import annotations

from collections.abc import Iterable
from contextlib import suppress
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from duckdb import CatalogException

from flowforge.core import Node, relation_for

from .base import BaseExecutor


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


class DuckExecutor(BaseExecutor[pd.DataFrame]):
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
        return _q(cfg["identifier"])

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        self.con.execute(f"create or replace view {target_sql} as {select_body}")

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        self.con.execute(f"create or replace table {target_sql} as {select_body}")
