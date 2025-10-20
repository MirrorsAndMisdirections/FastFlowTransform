# tests/test_python_model_materialized_view.py
from pathlib import Path

import pytest

from fastflowtransform.core import REGISTRY
from fastflowtransform.executors.duckdb_exec import DuckExecutor


@pytest.mark.duckdb
def test_python_model_materialized_as_view(tmp_path: Path, monkeypatch):
    # Arrange: minimal project
    m = tmp_path / "models"
    m.mkdir()
    (tmp_path / "sources.yml").write_text("{}", encoding="utf-8")

    # Seed base table via SQL model
    (m / "base.ff.sql").write_text("select 1 as id, 'x@gmail.com' as email;", encoding="utf-8")

    # Python model (returns a pandas DataFrame), configured as view
    (m / "py_users.ff.py").write_text(
        """
from fastflowtransform.decorators import model
import pandas as pd

@model(name="py_users.ff", deps=["base.ff"])
def build(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["is_gmail"] = out["email"].str.endswith("@gmail.com")
    return out
""",
        encoding="utf-8",
    )

    # Consumer SQL model to use the view
    (m / "mart.ff.sql").write_text(
        "{{ config(materialized='table') }}\nselect id, is_gmail from {{ ref('py_users.ff') }};",
        encoding="utf-8",
    )

    # Also place a config block on the Python view in a tiny shim SQL file:
    # We attach materialized='view' via a separate top directive model that won't be built;
    # alternatively, set REGISTRY.nodes['py_users.ff'].meta manually after load.
    # For simplicity here, we modify meta after load.
    REGISTRY.load_project(tmp_path)
    # mark the python node as view
    REGISTRY.nodes["py_users.ff"].meta["materialized"] = "view"

    env = REGISTRY.env
    assert env is not None
    ex = DuckExecutor(db_path=":memory:")

    # Build
    ex.run_sql(REGISTRY.nodes["base.ff"], env)
    ex.run_python(REGISTRY.nodes["py_users.ff"])
    ex.run_sql(REGISTRY.nodes["mart.ff"], env)

    # Assert: py_users is a VIEW
    is_view_rows = ex.con.execute(
        "select count(*) from information_schema.tables where table_type='VIEW' "
        "and lower(table_name)='py_users'"
    ).fetchone()
    assert is_view_rows is not None
    is_view = is_view_rows[0]
    assert is_view == 1

    # Assert: mart exists and reads from the view
    rows = ex.con.execute("select id, is_gmail from mart").fetchall()
    assert rows == [(1, True)]
