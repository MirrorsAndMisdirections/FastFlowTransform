# fastflowtransform/executors/postgres_exec.py
from collections.abc import Iterable
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError

from fastflowtransform.core import Node, relation_for
from fastflowtransform.errors import ModelExecutionError, ProfileConfigError
from fastflowtransform.executors._shims import SAConnShim
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.meta import ensure_meta_table, upsert_meta


class PostgresExecutor(BaseExecutor[pd.DataFrame]):
    ENGINE_NAME = "postgres"

    def __init__(self, dsn: str, schema: str | None = None):
        """
        Initialize Postgres executor.

        dsn     e.g.: postgresql+psycopg://user:pass@localhost:5432/dbname
        schema  default schema for reads/writes (also used for search_path)
        """
        if not dsn:
            raise ProfileConfigError(
                "Postgres DSN not set. Hint: profiles.yml → postgres.dsn or env FF_PG_DSN."
            )
        self.engine: Engine = create_engine(dsn, future=True)
        self.schema = schema

        if self.schema:
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self._q_ident(self.schema)}"))
            except SQLAlchemyError as exc:
                raise ProfileConfigError(
                    f"Failed to ensure schema '{self.schema}' exists: {exc}"
                ) from exc

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

    def _set_search_path(self, conn: Connection | SAConnShim) -> None:
        if self.schema:
            conn.execute(text(f"SET LOCAL search_path = {self._q_ident(self.schema)}"))

    def _extract_select_like(self, sql_or_body: str) -> str:
        """
        Normalize a SELECT/CTE body:
        - Accept full statements and strip everything before the first WITH/SELECT.
        - Strip trailing semicolons/whitespace.
        """
        s = (sql_or_body or "").lstrip()
        lower = s.lower()
        pos_with = lower.find("with")
        pos_select = lower.find("select")
        if pos_with == -1 and pos_select == -1:
            return s.rstrip(";\n\t ")
        start = min([p for p in (pos_with, pos_select) if p != -1])
        return s[start:].rstrip(";\n\t ")

    # ---------- IO ----------
    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> pd.DataFrame:
        qualified = self._qualified(relation)
        try:
            with self.engine.begin() as conn:
                if self.schema:
                    conn.execute(text(f'SET LOCAL search_path = "{self.schema}"'))
                return pd.read_sql_query(text(f"select * from {qualified}"), conn)
        except ProgrammingError as e:
            raise e

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
        except SQLAlchemyError as e:
            raise ModelExecutionError(
                node_name=node.name, relation=self._qualified(relation), message=str(e)
            ) from e

    # ---------- Python view helper ----------
    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        q_view = self._qualified(view_name)
        q_back = self._qualified(backing_table)
        try:
            with self.engine.begin() as c:
                self._set_search_path(c)
                c.execute(text(f"DROP VIEW IF EXISTS {q_view} CASCADE"))
                c.execute(text(f"CREATE OR REPLACE VIEW {q_view} AS SELECT * FROM {q_back}"))
        except Exception as e:
            raise ModelExecutionError(node.name, q_view, str(e)) from e

    def _frame_name(self) -> str:
        return "pandas"

    # ---- SQL hooks ----
    def _format_relation_for_ref(self, name: str) -> str:
        return self._qualified(relation_for(name))

    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        if cfg.get("location"):
            raise NotImplementedError("Postgres executor does not support path-based sources.")

        ident = cfg.get("identifier")
        if not ident:
            raise KeyError(f"Source {source_name}.{table_name} missing identifier")

        relation = self._qualified(ident, schema=cfg.get("schema"))
        database = cfg.get("database") or cfg.get("catalog")
        if database:
            return f"{self._q_ident(database)}.{relation}"
        return relation

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        try:
            with self.engine.begin() as conn:
                self._set_search_path(conn)
                conn.execute(text(f"DROP VIEW IF EXISTS {target_sql} CASCADE"))
                conn.execute(text(f"CREATE OR REPLACE VIEW {target_sql} AS {select_body}"))
        except Exception as e:
            preview = f"-- target={target_sql}\n{select_body}"
            raise ModelExecutionError(node.name, target_sql, str(e), sql_snippet=preview) from e

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        """
        Postgres does NOT support 'CREATE OR REPLACE TABLE'.
        Use DROP TABLE IF EXISTS + CREATE TABLE AS, and accept CTE bodies.
        """
        try:
            with self.engine.begin() as conn:
                self._set_search_path(conn)
                conn.execute(text(f"DROP TABLE IF EXISTS {target_sql} CASCADE"))
                conn.execute(text(f"CREATE TABLE {target_sql} AS {select_body}"))
        except Exception as e:
            preview = f"-- target={target_sql}\n{select_body}"
            raise ModelExecutionError(node.name, target_sql, str(e), sql_snippet=preview) from e

    # ---------- meta ----------
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
        """
        Return True if a table OR view exists for 'relation' in current schema.
        """
        sql = text(
            """
            select 1
            from information_schema.tables
            where table_schema = current_schema()
              and lower(table_name) = lower(:t)
            union all
            select 1
            from information_schema.views
            where table_schema = current_schema()
              and lower(table_name) = lower(:t)
            limit 1
            """
        )
        with self.engine.begin() as con:
            return bool(con.execute(sql, {"t": relation}).fetchone())

    def create_table_as(self, relation: str, select_sql: str) -> None:
        body = self._extract_select_like(select_sql)
        qrel = self._qualified(relation)
        with self.engine.begin() as con:
            self._set_search_path(con)
            con.execute(text(f"create table {qrel} as {body}"))

    def incremental_insert(self, relation: str, select_sql: str) -> None:
        body = self._extract_select_like(select_sql)
        qrel = self._qualified(relation)
        with self.engine.begin() as con:
            self._set_search_path(con)
            con.execute(text(f"insert into {qrel} {body}"))

    def incremental_merge(self, relation: str, select_sql: str, unique_key: list[str]) -> None:
        """
        Portable fallback: staging + delete + insert.
        """
        body = self._extract_select_like(select_sql)
        qrel = self._qualified(relation)
        pred = " AND ".join([f"t.{k}=s.{k}" for k in unique_key])
        with self.engine.begin() as con:
            self._set_search_path(con)
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
        Add new columns present in SELECT but missing on target (as text).
        """
        body = self._extract_select_like(select_sql)
        qrel = self._qualified(relation)
        with self.engine.begin() as con:
            self._set_search_path(con)
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
                con.execute(text(f'alter table {qrel} add column "{c}" text'))
