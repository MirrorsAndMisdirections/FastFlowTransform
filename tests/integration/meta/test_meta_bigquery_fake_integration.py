# tests/integration/test_meta_bigquery_fake_integration.py
from __future__ import annotations

import re
from types import SimpleNamespace

import pytest

from fastflowtransform.meta import ensure_meta_table, get_meta, relation_exists, upsert_meta


class FakeBQClient:
    """Very small in-memory fake for BigQuery client.query()."""

    def __init__(self):
        self.tables = {}  # qual -> dict[node_name -> dict]
        self.relations = set()

    class _Res:
        def __init__(self, rows):
            self._rows = rows

        def result(self):
            return self._rows

    def query(self, sql: str):
        sql_low = sql.lower()
        sql_stripped_low = sql_low.lstrip()
        # Naive parser for our limited SQL shapes
        if "create table if not exists" in sql_low and "_ff_meta" in sql_low:
            # no-op; table is "created" on demand
            return FakeBQClient._Res([])
        if sql_stripped_low.startswith("merge ") and "_ff_meta" in sql_low:
            # Extract '...literal...' AS <field> with a small regex
            def _grab(field: str) -> str:
                m = re.search(rf"'([^']*)'\s+as\s+{field}\b", sql, flags=re.IGNORECASE)
                return m.group(1) if m else ""

            node = _grab("node_name")
            rel = _grab("relation")
            fp = _grab("fp")
            eng = _grab("engine")
            qual = "meta"  # single bucket
            self.tables.setdefault(qual, {})
            self.tables[qual][node] = {"fp": fp, "relation": rel, "engine": eng, "built_at": "now"}
            return FakeBQClient._Res([])
        if "information_schema.tables" in sql_low:
            # existence check: table_name = 'rel'
            rel = sql.split("table_name = '", 1)[1].split("'", 1)[0]
            return FakeBQClient._Res([(1,)] if rel in self.relations else [])
        if "select fp, relation, built_at, engine from" in sql_low:
            node = sql.split("where node_name = '", 1)[1].split("'", 1)[0]
            qual = "meta"
            row = self.tables.get(qual, {}).get(node)
            return FakeBQClient._Res(
                [] if not row else [(row["fp"], row["relation"], row["built_at"], row["engine"])]
            )
        return FakeBQClient._Res([])


@pytest.mark.integration
@pytest.mark.bigquery
def test_bigquery_meta_with_fake():
    client = FakeBQClient()
    ex = SimpleNamespace(client=client, dataset="dset", project="proj")

    ensure_meta_table(ex)
    upsert_meta(ex, "users.ff", "users", "x1", "bigquery")
    row = get_meta(ex, "users.ff")
    assert row is not None and row[0] == "x1" and row[1] == "users"

    # existence
    client.relations.add("users")
    assert relation_exists(ex, "users") is True
    assert relation_exists(ex, "unknown") is False
