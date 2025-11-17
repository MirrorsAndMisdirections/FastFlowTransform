# tests/unit/executors/test_shims_unit.py
from __future__ import annotations

from collections.abc import Sequence
from types import SimpleNamespace
from typing import Any, cast

import pytest
from google.cloud.bigquery import Client
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

from fastflowtransform.executors._shims import (
    BigQueryConnShim,
    SAConnShim,
    _rewrite_pg_create_or_replace_table,
)

# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------


class _FakeConn:
    """Collects executed statements for assertions."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, dict[str, Any] | None]] = []

    # SQLAlchemy-style execute
    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> Any:
        # store string form for easier asserts
        sql_str = stmt.text if hasattr(stmt, "text") else str(stmt)
        self.executed.append((sql_str, params))
        # return something fetchable
        return SimpleNamespace(fetchone=lambda: None, fetchall=lambda: [])

    # context manager API
    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    # so SAConnShim can call conn.begin() in the fallback (we never do here)
    def begin(self) -> _FakeConn:
        return self


class _FakeEngine:
    """Engine that always returns the same connection."""

    def __init__(self) -> None:
        self.conn = _FakeConn()

    def begin(self) -> _FakeConn:
        return self.conn


# ---------------------------------------------------------------------------
# _rewrite_pg_create_or_replace_table
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_rewrite_pg_create_or_replace_table_simple():
    sql = "CREATE OR REPLACE TABLE public.t AS SELECT 1"
    out = _rewrite_pg_create_or_replace_table(sql)
    assert "DROP TABLE IF EXISTS public.t CASCADE;" in out
    assert "CREATE TABLE public.t AS SELECT 1" in out


@pytest.mark.unit
def test_rewrite_pg_create_or_replace_table_with_schema_and_quotes():
    sql = '  create or replace table "raw"."users" as select * from src  '
    out = _rewrite_pg_create_or_replace_table(sql)
    # two statements
    assert 'DROP TABLE IF EXISTS "raw"."users" CASCADE;' in out
    assert 'CREATE TABLE "raw"."users" AS select * from src' in out


@pytest.mark.unit
def test_rewrite_pg_create_or_replace_table_untouched_for_other_sql():
    sql = "SELECT 1"
    out = _rewrite_pg_create_or_replace_table(sql)
    assert out == sql


# ---------------------------------------------------------------------------
# SAConnShim
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_sa_shim_executes_plain_sql_without_schema():
    eng = _FakeEngine()
    shim = SAConnShim(cast(Engine, eng), schema=None)

    shim.execute("SELECT 1")

    executed = eng.conn.executed
    assert len(executed) == 1
    sql, params = executed[0]
    assert sql.strip().upper().startswith("SELECT 1")
    assert params is None


@pytest.mark.unit
def test_sa_shim_sets_search_path_when_schema_given():
    eng = _FakeEngine()
    shim = SAConnShim(cast(Engine, eng), schema="public")

    shim.execute("SELECT 42")

    executed = eng.conn.executed
    # 1) SET LOCAL ...  2) SELECT 42
    assert len(executed) == 2
    assert 'SET LOCAL search_path = "public"' in executed[0][0]
    assert "SELECT 42" in executed[1][0]


@pytest.mark.unit
def test_sa_shim_rewrites_cor_table_into_two_statements():
    eng = _FakeEngine()
    shim = SAConnShim(cast(Engine, eng), schema=None)

    shim.execute("CREATE OR REPLACE TABLE my_tbl AS SELECT 1")

    executed = eng.conn.executed
    # should have been split into DROP + CREATE
    assert len(executed) == 2
    assert "DROP TABLE IF EXISTS my_tbl CASCADE" in executed[0][0]
    assert "CREATE TABLE my_tbl AS SELECT 1" in executed[1][0]


@pytest.mark.unit
def test_sa_shim_executes_iterable_sequentially():
    eng = _FakeEngine()
    shim = SAConnShim(cast(Engine, eng), schema=None)

    shim.execute(["SELECT 1", "SELECT 2"])

    executed = [sql for (sql, _) in eng.conn.executed]
    assert executed == ["SELECT 1", "SELECT 2"]


@pytest.mark.unit
def test_sa_shim_executes_tuple_with_params_on_last_statement():
    eng = _FakeEngine()
    shim = SAConnShim(cast(Engine, eng), schema=None)

    shim.execute(("SELECT :x", {"x": 10}))

    executed = eng.conn.executed
    assert len(executed) == 1
    sql, params = executed[0]
    assert "SELECT :x" in sql
    assert params == {"x": 10}


@pytest.mark.unit
def test_sa_shim_executes_sqlalchemy_clauseelement():
    eng = _FakeEngine()
    shim = SAConnShim(cast(Engine, eng), schema=None)

    stmt = sa_text("SELECT 1")
    shim.execute(stmt)

    executed = eng.conn.executed
    assert len(executed) == 1
    assert "SELECT 1" in executed[0][0]


# ---------------------------------------------------------------------------
# BigQueryConnShim
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_bq_shim_executes_single_sql():
    calls: dict[str, Any] = {}

    class FakeJob:
        def __init__(self) -> None:
            self.result_called = False

        def result(self):
            self.result_called = True
            # Simulate a RowIterator; list is fine for the wrapper.
            return ["ROW-1"]

    class FakeClient:
        def query(self, sql: str, location: str | None = None):
            calls["sql"] = sql
            calls["location"] = location
            return FakeJob()

    fake = FakeClient()
    shim = BigQueryConnShim(cast(Client, fake), location="EU")
    res = shim.execute("SELECT 1")

    # Shim now returns a cursor-like wrapper
    assert isinstance(res, BigQueryConnShim._ResultWrapper)
    assert calls["sql"] == "SELECT 1"
    # We don't pass location into client.query, so it should be None.
    assert calls["location"] is None
    # And fetchone() should give the first row.
    assert res.fetchone() == "ROW-1"
    assert res.fetchone() is None


@pytest.mark.unit
def test_bq_shim_executes_sequence_and_returns_last_job():
    seen: list[str] = []

    class FakeJob:
        def result(self) -> None:
            return None

    class FakeClient:
        def query(self, sql: str, location: str | None = None):
            seen.append(sql)
            return FakeJob()

    fake = FakeClient()
    shim = BigQueryConnShim(cast(Client, fake), location="EU")
    res = shim.execute(["SELECT 1", "SELECT 2", "SELECT 3"])

    # should have executed all
    assert seen == ["SELECT 1", "SELECT 2", "SELECT 3"]
    # and returned a cursor-like wrapper over the last result
    assert isinstance(res, BigQueryConnShim._ResultWrapper)


def test_bq_shim_raises_on_unsupported_type():
    fake_client = SimpleNamespace(query=lambda *a, **k: None)

    shim = BigQueryConnShim(client=cast(Client, fake_client))

    with pytest.raises(TypeError):
        shim.execute(123)


# ---------------------------------------------------------------------------
# Mixed / defensive
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_sa_shim_iterable_of_mixed_types():
    """Ensure iterable with strings and ClauseElements is executed in order."""
    eng = _FakeEngine()
    shim = SAConnShim(cast(Engine, eng))

    stmts: Sequence[Any] = ["SELECT 1", sa_text("SELECT 2"), "SELECT 3"]
    shim.execute(stmts)

    executed = [sql for (sql, _) in eng.conn.executed]
    # sql text may contain trailing semicolons/spaces from TextClause
    assert "SELECT 1" in executed[0]
    assert "SELECT 2" in executed[1]
    assert "SELECT 3" in executed[2]
