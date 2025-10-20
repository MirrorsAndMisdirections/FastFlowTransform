# tests/unit/test_cache_skip_logic.py
from __future__ import annotations

from flowforge.cache import FingerprintCache, can_skip_node


class _DummyExec:
    def __init__(self, present: set[str]):
        self._present = present

    # DuckDB-like `con.execute(...).fetchall()` shape
    class _Con:
        def __init__(self, present: set[str]):
            self.present = present

        def execute(self, sql, params=None):
            class _R:
                def __init__(self, present, table):
                    self.present = present
                    self.table = table

                def fetchall(self):
                    return [(1,)] if self.table in self.present else []

            # Return local class instance directly (do not reference as attribute of _Con)
            return _R(self.present, (params or [None])[0])

    @property
    def con(self):
        return _DummyExec._Con(self._present)


def test_can_skip_node_requires_artifact_when_non_ephemeral(tmp_path):
    cache = FingerprintCache(tmp_path, profile="dev", engine="duckdb")
    cache.entries = {"users.ff": "xxx"}

    # artifact missing → cannot skip
    ex = _DummyExec(present=set())
    assert not can_skip_node(
        node_name="users.ff",
        new_fp="xxx",
        cache=cache,
        executor=ex,
        materialized="table",
    )

    # artifact present → skip ok
    ex2 = _DummyExec(present={"users"})
    assert can_skip_node(
        node_name="users.ff",
        new_fp="xxx",
        cache=cache,
        executor=ex2,
        materialized="table",
    )


def test_ephemeral_skip_without_artifact(tmp_path):
    cache = FingerprintCache(tmp_path, profile="dev", engine="duckdb")
    cache.entries = {"ephem.ff": "yyy"}
    ex = _DummyExec(present=set())
    assert can_skip_node(
        node_name="ephem.ff",
        new_fp="yyy",
        cache=cache,
        executor=ex,
        materialized="ephemeral",
    )
