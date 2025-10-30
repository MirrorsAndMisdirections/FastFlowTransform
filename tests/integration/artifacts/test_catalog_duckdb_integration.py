import json
from pathlib import Path

import pytest

from fastflowtransform.artifacts import write_catalog
from fastflowtransform.core import REGISTRY
from fastflowtransform.executors.duckdb_exec import DuckExecutor


@pytest.mark.integration
@pytest.mark.duckdb
def test_catalog_duckdb(tmp_path: Path):
    (tmp_path / "models").mkdir(parents=True)
    (tmp_path / "models" / "t.ff.sql").write_text(
        "create or replace table t as select 1::int as id, 'x'::varchar as email", encoding="utf-8"
    )
    (tmp_path / "sources.yml").write_text("{}", encoding="utf-8")
    REGISTRY.load_project(tmp_path)
    env = REGISTRY.get_env()
    ex = DuckExecutor(":memory:")
    ex.run_sql(REGISTRY.get_node("t.ff"), env)

    p = write_catalog(tmp_path, ex)
    data = json.loads(p.read_text(encoding="utf-8"))
    assert "relations" in data
    cols = {c["name"] for c in data["relations"]["t"]["columns"]}
    assert {"id", "email"}.issubset(cols)
