from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from google.cloud.bigquery import Client
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.sql.elements import ClauseElement


class BigQueryConnShim:
    """
    Lightweight shim so flowforge.testing can call executor.con.execute(...)
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


class SAConnShim:
    """
    Compatibility layer so flowforge.testing can call executor.con.execute(...)
    against SQLAlchemy engines (Postgres, etc.).
    """

    marker = "PG_SHIM"

    def __init__(self, engine: Engine, schema: str | None = None):
        self._engine = engine
        self._schema = schema

    def _exec_one(self, conn: Any, stmt: Any) -> Any:
        statement_tuple_len = 2
        if (
            isinstance(stmt, tuple)
            and len(stmt) == statement_tuple_len
            and isinstance(stmt[0], str)
            and isinstance(stmt[1], dict)
        ):
            return conn.execute(text(stmt[0]), stmt[1])
        if isinstance(stmt, ClauseElement):
            return conn.execute(stmt)
        if isinstance(stmt, str):
            return conn.execute(text(stmt))
        if isinstance(stmt, Iterable) and not isinstance(stmt, (bytes, bytearray, str)):
            res = None
            for s in stmt:
                res = self._exec_one(conn, s)
            return res
        raise TypeError(f"Unsupported statement type in shim: {type(stmt)} → {stmt!r}")

    def execute(self, sql: Any) -> Any:
        with self._engine.begin() as conn:
            sql_tuple_len = 2
            if self._schema:
                conn.execute(text(f'SET LOCAL search_path = "{self._schema}"'))
            if isinstance(sql, (str, ClauseElement)) or (
                isinstance(sql, tuple)
                and len(sql) == sql_tuple_len
                and isinstance(sql[0], str)
                and isinstance(sql[1], dict)
            ):
                return self._exec_one(conn, sql)
            if isinstance(sql, Iterable) and not isinstance(sql, (bytes, bytearray, str)):
                res = None
                for item in sql:
                    res = self._exec_one(conn, item)
                return res
            raise TypeError(f"Unsupported sql argument type: {type(sql)} → {sql!r}")
