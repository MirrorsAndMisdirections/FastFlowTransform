# tests/integration/meta/test_meta_duckdb_integration.py
from __future__ import annotations

from pathlib import Path

import pytest

from fastflowtransform.executors.duckdb import DuckExecutor
from fastflowtransform.meta import ensure_meta_table, get_meta, relation_exists, upsert_meta


@pytest.mark.integration
@pytest.mark.duckdb
def test_duckdb_meta_roundtrip(tmp_path: Path):
    ex = DuckExecutor(db_path=str(tmp_path / "t.duckdb"))
    ensure_meta_table(ex)

    # Initially empty
    assert get_meta(ex, "users.ff") is None

    # Upsert and read back
    upsert_meta(ex, "users.ff", "users", "abc123", "duckdb")
    row = get_meta(ex, "users.ff")
    assert row is not None
    fp, rel, built_at, eng = row
    assert fp == "abc123"
    assert rel == "users"
    assert eng == "duckdb"
    assert built_at is not None

    # Update
    upsert_meta(ex, "users.ff", "users", "def456", "duckdb")
    row2 = get_meta(ex, "users.ff")
    assert row2 is not None
    fp2, rel2, _, eng2 = row2
    assert fp2 == "def456"
    assert rel2 == "users"
    assert eng2 == "duckdb"

    # Relation existence helper
    # Create a physical table and verify existence
    ex.con.execute('create table "users" (id int)')
    assert relation_exists(ex, "users") is True
    assert relation_exists(ex, "does_not_exist") is False
