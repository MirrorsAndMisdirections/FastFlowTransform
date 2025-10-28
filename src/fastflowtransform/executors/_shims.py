from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from typing import Any

from google.cloud.bigquery import Client
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.sql.elements import ClauseElement


class BigQueryConnShim:
    """
    Lightweight shim so fastflowtransform.testing can call executor.con.execute(...)
    against BigQuery clients.
    """

    marker = "BQ_SHIM"

    def __init__(
        self,
        client: Client,
        location: str | None = None,
        project: str | None = None,
        dataset: str | None = None,
    ):
        self.client = client
        self.location = location
        self.project = project
        self.dataset = dataset

    def execute(self, sql_or_stmts: Any) -> Any:
        if isinstance(sql_or_stmts, str):
            return self.client.query(sql_or_stmts, location=self.location)
        if isinstance(sql_or_stmts, Sequence) and not isinstance(
            sql_or_stmts, (bytes, bytearray, str)
        ):
            job = None
            for stmt in sql_or_stmts:
                job = self.client.query(str(stmt), location=self.location)
                job.result()
            return job
        raise TypeError(f"Unsupported sql argument type for BigQuery shim: {type(sql_or_stmts)}")


_RE_PG_COR_TABLE = re.compile(
    r"""^\s*create\s+or\s+replace\s+table\s+
        (?P<ident>(?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+))?)   # optional schema + ident
        \s+as\s+(?P<body>.*)$
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)


def _rewrite_pg_create_or_replace_table(sql: str) -> str:
    """
    Rewrite 'CREATE OR REPLACE TABLE t AS <body>' into
    'DROP TABLE IF EXISTS t CASCADE; CREATE TABLE t AS <body>' for Postgres.
    Leave all other SQL untouched.
    """
    m = _RE_PG_COR_TABLE.match(sql or "")
    if not m:
        return sql
    ident = m.group("ident").strip()
    body = m.group("body").strip().rstrip(";\n\t ")
    return f"DROP TABLE IF EXISTS {ident} CASCADE; CREATE TABLE {ident} AS {body}"


class SAConnShim:
    """
    Compatibility layer so fastflowtransform.testing can call executor.con.execute(...)
    against SQLAlchemy engines (Postgres, etc.). Adds PG-safe DDL rewrites.
    """

    marker = "PG_SHIM"

    def __init__(self, engine: Engine, schema: str | None = None):
        self._engine = engine
        self._schema = schema

    def _exec_one(self, conn: Any, stmt: Any, params: dict | None = None) -> Any:
        # tuple (sql, params)
        statement_len = 2
        if (
            isinstance(stmt, tuple)
            and len(stmt) == statement_len
            and isinstance(stmt[0], str)
            and isinstance(stmt[1], dict)
        ):
            return self._exec_one(conn, stmt[0], stmt[1])

        # sqlalchemy expression
        if isinstance(stmt, ClauseElement):
            return conn.execute(stmt)

        # plain string (apply rewrite, then possibly split into multiple statements)
        if isinstance(stmt, str):
            rewritten = _rewrite_pg_create_or_replace_table(stmt)
            parts = [p.strip() for p in rewritten.split(";") if p.strip()]
            res = None
            for i, part in enumerate(parts):
                res = conn.execute(text(part), params if (i == len(parts) - 1) else None)
            return res

        # iterable of statements -> sequential execution
        if isinstance(stmt, Iterable) and not isinstance(stmt, (bytes, bytearray, str)):
            res = None
            for s in stmt:
                res = self._exec_one(conn, s)
            return res

        # fallback
        return self._exec_one(conn, str(stmt))

    def execute(self, sql: Any) -> Any:
        with self._engine.begin() as conn:
            if self._schema:
                conn.execute(text(f'SET LOCAL search_path = "{self._schema}"'))
            return self._exec_one(conn, sql)
