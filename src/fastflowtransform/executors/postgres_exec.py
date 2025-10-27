# fastflowtransform/executors/postgres_exec.py
from collections.abc import Iterable
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError

from fastflowtransform.core import Node, relation_for
from fastflowtransform.errors import ProfileConfigError
from fastflowtransform.executors._shims import SAConnShim
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.meta import ensure_meta_table, upsert_meta


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

        # ⇣ fastflowtransform.testing expects executor.con.execute("SQL")
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

    # ── Incremental API ────────────────────────────────────────────────────
    def exists_relation(self, relation: str) -> bool:
        sql = text("""
          select 1 from information_schema.tables
          where table_schema = current_schema()
            and lower(table_name) = lower(:t)
          limit 1
        """)
        with self.engine.begin() as con:
            return bool(con.execute(sql, {"t": relation}).fetchone())

    def create_table_as(self, relation: str, select_sql: str) -> None:
        # Use only the SELECT body and strip any trailing semicolons
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        qrel = self._qualified(relation)
        with self.engine.begin() as con:
            con.execute(text(f"create table {qrel} as {body}"))

    def incremental_insert(self, relation: str, select_sql: str) -> None:
        # Clean inner SELECT for safety
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        qrel = self._qualified(relation)
        with self.engine.begin() as con:
            con.execute(text(f"insert into {qrel} {body}"))

    def incremental_merge(self, relation: str, select_sql: str, unique_key: list[str]) -> None:
        """
        Portable fallback (without columns list): staging + delete + insert.
        """
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        qrel = self._qualified(relation)
        pred = " AND ".join([f"t.{k}=s.{k}" for k in unique_key])
        with self.engine.begin() as con:
            # Use a temp table to avoid re-running the SELECT twice
            con.execute(text(f"create temporary table ff_stg as {body}"))
            try:
                con.execute(text(f"delete from {qrel} t using ff_stg s where {pred}"))
                con.execute(text(f"insert into {qrel} select * from ff_stg"))
            finally:
                con.execute(text("drop table if exists ff_stg"))

    def alter_table_sync_schema(
        self, relation: str, select_sql: str, *, mode: str = "append_new_columns"
    ) -> None:
        """
        Additive: add new columns as text/varchar.
        """
        with self.engine.begin() as con:
            # Probe schema from a cleaned SELECT to avoid parser issues
            body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
            cols = [r[0] for r in con.execute(text(f"select * from ({body}) q limit 0"))]
            existing = {
                r[0]
                for r in con.execute(
                    text(
                        "select column_name from information_schema.columns "
                        "where table_schema = current_schema() and lower(table_name)=lower(:t)"
                    ),
                    {"t": relation},
                ).fetchall()
            }
            add = [c for c in cols if c not in existing]
            for c in add:
                qrel = self._qualified(relation)
                con.execute(text(f'alter table {qrel} add column "{c}" text'))
