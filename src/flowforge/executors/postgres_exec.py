# flowforge/executors/postgres_exec.py
from collections.abc import Iterable
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError

from flowforge.core import Node, relation_for
from flowforge.errors import ProfileConfigError
from flowforge.executors._shims import SAConnShim
from flowforge.executors.base import BaseExecutor
from flowforge.meta import ensure_meta_table, upsert_meta


class PostgresExecutor(BaseExecutor[pd.DataFrame]):
    def __init__(self, dsn: str, schema: str | None = None):
        """
        dsn e.g.: postgresql+psycopg://user:pass@localhost:5432/dbname
        schema: default schema for reads/writes (if sources.yml does not define one)
        """
        if not dsn:
            raise ProfileConfigError(
                "Postgres DSN not set. Hint: profiles.yml → postgres.dsn or env FF_PG_DSN."
            )
        self.engine: Engine = create_engine(dsn, future=True)
        self.schema = schema

        # ⇣ flowforge.testing expects executor.con.execute("SQL")
        self.con = SAConnShim(self.engine, schema=self.schema)

    # --- Helpers ---------------------------------------------------------
    def _q_ident(self, ident: str) -> str:
        # Simple, safe quoting for identifiers
        return '"' + ident.replace('"', '""') + '"'

    def _qualified(self, relname: str, schema: str | None = None) -> str:
        sch = schema or self.schema
        if sch:
            return f"{self._q_ident(sch)}.{self._q_ident(relname)}"
        return self._q_ident(relname)

    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> pd.DataFrame:
        qualified = self._qualified(relation)
        try:
            with self.engine.begin() as conn:
                if self.schema:
                    conn.execute(text(f'SET LOCAL search_path = "{self.schema}"'))
                return pd.read_sql_query(text(f"select * from {qualified}"), conn)
        except ProgrammingError:
            raise

    def _materialize_relation(self, relation: str, df: pd.DataFrame, node: Node) -> None:
        try:
            df.to_sql(
                relation,
                self.engine,
                if_exists="replace",
                index=False,
                schema=self.schema,
                method="multi",
            )
        except SQLAlchemyError:
            # ... bestehende Fehleraufbereitung
            raise

    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        q_view = self._qualified(view_name)
        q_back = self._qualified(backing_table)
        with self.engine.begin() as c:
            if self.schema:
                c.execute(text(f'SET LOCAL search_path = "{self.schema}"'))
            c.execute(text(f"DROP VIEW IF EXISTS {q_view} CASCADE"))
            c.execute(text(f"CREATE OR REPLACE VIEW {q_view} AS SELECT * FROM {q_back}"))

    def _frame_name(self) -> str:
        return "pandas"

    # ---- SQL hooks ----
    def _format_relation_for_ref(self, name: str) -> str:
        return self._qualified(relation_for(name))

    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        ident = cfg["identifier"]
        src_schema = cfg.get("schema")
        return self._qualified(ident, schema=src_schema)

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        with self.engine.begin() as conn:
            if self.schema:
                conn.execute(text(f"SET LOCAL search_path = {self._q_ident(self.schema)}"))
            conn.execute(text(f"DROP VIEW IF EXISTS {target_sql} CASCADE"))
            conn.execute(text(f"CREATE OR REPLACE VIEW {target_sql} AS {select_body}"))

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        with self.engine.begin() as conn:
            if self.schema:
                conn.execute(text(f"SET LOCAL search_path = {self._q_ident(self.schema)}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {target_sql} CASCADE"))
            conn.execute(text(f"CREATE TABLE {target_sql} AS {select_body}"))

    def on_node_built(self, node: Node, relation: str, fingerprint: str) -> None:
        """
        Write/update _ff_meta in the current schema after a successful build.
        """
        try:
            ensure_meta_table(self)
            upsert_meta(self, node.name, relation, fingerprint, "postgres")
        except Exception:
            pass
