# tests/integration/core/test_python_model_dependencies_integration.py
import pandas as pd
import pytest

from fastflowtransform.core import REGISTRY, Node
from fastflowtransform.executors.duckdb_exec import DuckExecutor


@pytest.mark.integration
@pytest.mark.duckdb
def test_python_model_dep_loading_single_and_multi(tmp_path):
    ex = DuckExecutor()
    con = ex.con

    # seed zwei Tabellen
    con.execute("create table users as select 1::int as id, 'a@example.com'::varchar as email")
    con.execute("create table orders as select 1::int as user_id, 10.0::double as order_value")

    # registriere zwei Python-Modelle in REGISTRY (vereinfachtes Setup)
    def one(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(flag=True)

    def multi(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
        return dfs["orders"].merge(dfs["users"], left_on="user_id", right_on="id")

    REGISTRY.py_funcs["u1"] = one
    REGISTRY.py_funcs["m1"] = multi
    REGISTRY.nodes["u1"] = Node(name="u1", kind="python", path=tmp_path / "u1", deps=["users.ff"])
    REGISTRY.nodes["m1"] = Node(
        name="m1", kind="python", path=tmp_path / "m1", deps=["orders.ff", "users.ff"]
    )

    # 1 dep
    ex.run_python(REGISTRY.nodes["u1"])
    u1_row = con.execute("select count(*) from u1").fetchone()
    assert u1_row
    assert u1_row[0] == 1

    # >1 deps
    ex.run_python(REGISTRY.nodes["m1"])
    m1_row = con.execute("select count(*) from m1").fetchone()
    assert m1_row
    assert m1_row[0] == 1
