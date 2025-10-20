# tests/integration/test_meta_postgres.py
from __future__ import annotations

import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from flowforge.meta import ensure_meta_table, get_meta, relation_exists, upsert_meta

pytestmark = pytest.mark.postgres  # mark to opt-in in CI


def test_postgres_meta_roundtrip(pg_env):
    engine = create_engine(pg_env["FF_PG_DSN"])
    ex = SimpleNamespace(engine=engine, schema=os.getenv("FF_PG_SCHEMA", "public"))

    ensure_meta_table(ex)
    # Use a unique node name per test run to avoid leftover rows from previous runs
    node = f"users.ff::{uuid4().hex}"
    assert get_meta(ex, node) is None

    upsert_meta(ex, node, "users", "abc", "postgres")
    row = get_meta(ex, node)
    assert row is not None
    fp, rel, _, eng = row
    assert (fp, rel, eng) == ("abc", "users", "postgres")

    # relation exists
    with engine.begin() as conn:
        conn.execute(text('create table if not exists "users" (id int)'))
    assert relation_exists(ex, "users") is True
