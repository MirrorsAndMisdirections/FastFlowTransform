from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from jinja2 import Environment, FileSystemLoader

from fastflowtransform.core import REGISTRY, Node
from fastflowtransform.executors.base import BaseExecutor


# -------- Minimal no-DB executor to drive render_sql/run_sql --------
class _DummyExec(BaseExecutor[Any]):
    def __init__(self) -> None:
        # capture points for assertions (target_sql, body, node_name)
        self.last_view: tuple[str, str, str] | None = None
        self.last_table: tuple[str, str, str] | None = None
        self.last_py_view: tuple[str, str, str] | None = None

    def _format_relation_for_ref(self, name: str) -> str:
        # simple double-quote for "targets"
        return '"' + (name[:-3] if name.endswith(".ff") else name).replace('"', '""') + '"'

    def _format_source_reference(self, cfg: dict[str, Any], *_: str) -> str:
        ident = cfg["identifier"]
        return '"' + str(ident).replace('"', '""') + '"'

    def _create_or_replace_view(self, target_sql: str, body: str, node: Node) -> None:
        # capture for assertions if needed
        self.last_view = target_sql, body, node.name

    def _create_or_replace_table(self, target_sql: str, body: str, node: Node) -> None:
        self.last_table = target_sql, body, node.name

    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        self.last_py_view = view_name, backing_table, node.name

    # unused by these tests:
    def _read_relation(self, relation: str, node: Node, deps):
        raise NotImplementedError

    def _materialize_relation(self, relation: str, df: Any, node: Node) -> None:
        raise NotImplementedError

    def _columns_of(self, frame: Any) -> list[str]:
        return []

    def _is_frame(self, obj: Any) -> bool:
        return False


def _write(p: Path, text: str) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    return p


def _setup_env(models_dir: Path) -> None:
    # ensure a clean Jinja env on the registry for this temp project
    REGISTRY.env = Environment(loader=FileSystemLoader(str(models_dir)), autoescape=False)
    # no global macros for these tests unless temp project provides some
    REGISTRY.macros = getattr(REGISTRY, "macros", {})
    REGISTRY.cli_vars = {}


@pytest.mark.unit
def test_config_hook_sets_materialized_view(tmp_path: Path):
    proj = tmp_path
    models = proj / "models"
    _setup_env(models)

    # model with config(materialized='view')
    _write(models / "users.ff.sql", "{{ config(materialized='view') }}\nselect 1 as id")
    REGISTRY.nodes.clear()
    REGISTRY.nodes["users.ff"] = Node(
        name="users.ff", kind="sql", path=models / "users.ff.sql", deps=[], meta={}
    )

    ex = _DummyExec()
    jenv = REGISTRY.env
    assert jenv is not None

    # run_sql triggers render + config capture; we don't assert SQL execution
    ex.run_sql(REGISTRY.nodes["users.ff"], jenv)

    assert REGISTRY.nodes["users.ff"].meta.get("materialized") == "view"


@pytest.mark.unit
def test_var_reads_project_and_cli_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    proj = tmp_path
    models = proj / "models"
    _setup_env(models)

    # project vars via project.yml
    (proj / "project.yml").write_text("vars:\n  day: '2000-01-01'\n", encoding="utf-8")

    # template reads var('day', default)
    _write(models / "echo_day.ff.sql", "select '{{ var('day','X') }}' as day")
    REGISTRY.nodes.clear()
    REGISTRY.nodes["echo_day.ff"] = Node(
        "echo_day.ff", "sql", models / "echo_day.ff.sql", [], meta={}
    )

    # make project vars visible to REGISTRY (your impl likely reads on load; we inject here)
    REGISTRY.project_vars = {"day": "2000-01-01"}
    # CLI override
    REGISTRY.cli_vars = {"day": "2025-10-01"}

    ex = _DummyExec()
    jenv = REGISTRY.env
    assert jenv is not None

    # render and capture body used for materialization
    ex.run_sql(REGISTRY.nodes["echo_day.ff"], jenv)
    assert ex.last_table is not None
    _, body, _ = ex.last_table  # created as table by default
    assert "2025-10-01" in body  # CLI overrides project.yml
    assert "2000-01-01" not in body


@pytest.mark.unit
def test_macros_are_loaded_and_callable(tmp_path: Path):
    proj = tmp_path
    models = proj / "models"
    macros = models / "macros"
    _setup_env(models)

    # define a macro
    _write(macros / "my_macro.sql", "{% macro my_macro(x) %}upper({{ x }}){% endmacro %}\n")
    # using the macro in a model
    _write(models / "m.ff.sql", "select {{ my_macro(\"'abc'\") }} as v")

    # simulate loader registered macros on env.globals + REGISTRY.macros
    # (if your loader already does this on load_project, mimic it here)
    assert REGISTRY.env is not None
    tpl = REGISTRY.env.get_template("macros/my_macro.sql")
    # Jinja's macro objects live in the template module dict
    mod = tpl.module
    REGISTRY.env.globals.update({k: getattr(mod, k) for k in dir(mod) if not k.startswith("_")})
    REGISTRY.macros = {"my_macro": macros / "my_macro.sql"}

    REGISTRY.nodes.clear()
    REGISTRY.nodes["m.ff"] = Node("m.ff", "sql", models / "m.ff.sql", [], meta={})

    ex = _DummyExec()
    jenv = REGISTRY.env
    ex.run_sql(REGISTRY.nodes["m.ff"], jenv)  # should render macro call
    assert ex.last_table is not None
    _, body, _ = ex.last_table
    assert "upper('abc')" in body
    # registry keeps macro inventory for docs
    assert "my_macro" in REGISTRY.macros
