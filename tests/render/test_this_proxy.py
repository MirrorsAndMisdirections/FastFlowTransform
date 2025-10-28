# tests/render/test_this_proxy.py
from pathlib import Path

from jinja2 import Environment

from fastflowtransform.core import Node
from fastflowtransform.executors.duckdb_exec import DuckExecutor


def test_this_string_and_name(tmp_path: Path):
    p = tmp_path / "m.ff.sql"
    p.write_text("select '{{ this }}' as a, '{{ this.name }}' as b", encoding="utf-8")
    node = Node(name="m.ff", kind="sql", path=p)
    env = Environment()
    ex = DuckExecutor()
    sql = ex.render_sql(node, env).strip().lower()
    assert sql == "select 'm' as a, 'm' as b"
