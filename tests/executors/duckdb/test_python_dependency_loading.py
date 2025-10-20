import pandas as pd
import pytest

from fastflowtransform.core import REGISTRY, Node
from fastflowtransform.executors.duckdb_exec import DuckExecutor


@pytest.mark.duckdb
def test_duckdb_executor_dep_loading_unit(tmp_path):
    ex = DuckExecutor()
    con = ex.con
    con.execute("create table users as select 1::int as id, 'a@example.com'::varchar as email")
    con.execute(
        "create table orders as "
        "select 101::int as order_id, 1::int as user_id, 10.0::double as amount"
    )

    # registriere ein Multi-Dep Python-Modell on-the-fly
    def multi(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
        return dfs["orders"].merge(dfs["users"], left_on="user_id", right_on="id")

    REGISTRY.py_funcs["m1"] = multi
    REGISTRY.nodes["m1"] = Node(
        name="m1", kind="python", path=tmp_path / "m1", deps=["orders.ff", "users.ff"]
    )

    ex.run_python(REGISTRY.nodes["m1"])
    m1_row = con.execute("select count(*) from m1").fetchone()
    assert m1_row
    assert m1_row[0] == 1
