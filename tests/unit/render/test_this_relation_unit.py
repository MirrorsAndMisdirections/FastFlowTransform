# tests/unit/render/test_this_relation_unit.py
import pytest
from jinja2 import Environment, FileSystemLoader, select_autoescape

from fastflowtransform.core import Node
from fastflowtransform.executors.duckdb_exec import DuckExecutor


def _env_for_tests() -> Environment:
    return Environment(
        loader=FileSystemLoader(["."]),
        autoescape=select_autoescape([]),
        trim_blocks=True,
        lstrip_blocks=True,
    )


@pytest.mark.unit
def test_this_renders_physical_relation(tmp_path):
    # Arrange: Minimal SQL-Model, das nur `{{ this }}` rendert
    sql_path = tmp_path / "m.ff.sql"
    sql_path.write_text("select '{{ this }}' as rel\n", encoding="utf-8")

    node = Node(name="m.ff", kind="sql", path=sql_path)

    env = _env_for_tests()
    ex = DuckExecutor()  # nur für render_sql(), DB wird nicht genutzt

    # Act
    rendered = ex.render_sql(node, env).strip()

    # Assert
    # Erwartet: physischer Name ist 'm' (relation_for("m.ff"))
    assert rendered.lower() == "select 'm' as rel"
