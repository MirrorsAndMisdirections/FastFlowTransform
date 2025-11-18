from __future__ import annotations

from pathlib import Path

import pytest

from fastflowtransform.core import Node
from fastflowtransform.executors.duckdb import DuckExecutor


@pytest.mark.integration
@pytest.mark.duckdb
def test_duckdb_on_node_built_no_crash(tmp_path: Path):
    # Smoke-test: calling the hook must not raise errors (best-effort semantics)
    ex = DuckExecutor(db_path=":memory:")
    node = Node(name="x.ff", kind="sql", path=tmp_path / "x.ff.sql", deps=[], meta={})
    ex.on_node_built(node, relation="x", fingerprint="abc123")  # should not raise
