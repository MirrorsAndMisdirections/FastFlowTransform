# tests/integration/executors/duckdb/test_materializations_integration.py
from pathlib import Path

import pytest

from fastflowtransform.core import REGISTRY
from fastflowtransform.executors.duckdb_exec import DuckExecutor


@pytest.mark.duckdb
@pytest.mark.integration
def test_materializations_and_ephemeral_inlining_duckdb(tmp_path: Path):
    # Arrange: minimal project with 3 models (table/view/ephemeral) + consumer
    models = tmp_path / "models"
    models.mkdir(parents=True, exist_ok=True)
    # empty sources.yml so source(...) is available if needed
    (tmp_path / "sources.yml").write_text("{}", encoding="utf-8")

    # Base model (default: table)
    (models / "base.ff.sql").write_text(
        "select 1 as id, 'x@gmail.com' as email;",
        encoding="utf-8",
    )

    # View model
    (models / "v_users.ff.sql").write_text(
        "{{ config(materialized='view') }}\nselect id, email from {{ ref('base.ff') }};",
        encoding="utf-8",
    )

    # Ephemeral model (will not be materialized)
    (models / "e_only.ff.sql").write_text(
        "{{ config(materialized='ephemeral') }}\nselect id from {{ ref('base.ff') }};",
        encoding="utf-8",
    )

    # Consumer model that inlines the ephemeral
    (models / "mart.ff.sql").write_text(
        "select u.id from {{ ref('v_users.ff') }} u join {{ ref('e_only.ff') }} e using(id);",
        encoding="utf-8",
    )

    # Load project and execute (topological order is trivial here)
    REGISTRY.load_project(tmp_path)
    env = REGISTRY.env
    assert env is not None
    ex = DuckExecutor(db_path=":memory:")

    for name in ["base.ff", "v_users.ff", "mart.ff"]:
        node = REGISTRY.nodes[name]
        ex.run_sql(node, env)

    # Assert: view exists as a VIEW
    is_view_rows = ex.con.execute(
        "select count(*) from information_schema.tables "
        "where table_type='VIEW' and lower(table_name)='v_users'"
    ).fetchone()
    assert is_view_rows is not None, "v_users should be a VIEW"
    is_view = is_view_rows[0]
    assert is_view == 1, "v_users should be a VIEW"

    # Assert: ephemeral not materialized (no table or view named e_only)
    e_count_rows = ex.con.execute(
        "select count(*) from information_schema.tables where lower(table_name)='e_only'"
    ).fetchone()
    assert e_count_rows is not None, "e_only should not be materialized (ephemeral)"
    e_count = e_count_rows[0]
    assert e_count == 0, "e_only should not be materialized (ephemeral)"

    # Assert: mart table exists and contains the expected row(s)
    mart_rows = ex.con.execute("select count(*) from mart").fetchone()
    assert mart_rows is not None, "mart should be materialized as a TABLE"
    mart_rows = mart_rows[0]
    assert mart_rows == 1, "mart should be materialized as a TABLE with one row"
