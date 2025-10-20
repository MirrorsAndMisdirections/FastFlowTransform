# tests/unit/test_cli_select.py
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment
from typer.testing import CliRunner

from flowforge.cli import _build_predicates, _parse_select, _selector, app
from flowforge.core import REGISTRY, Node


# -------------------------------
# Helpers-only: predicates & parse
# -------------------------------
def _mk_node(tmp_path: Path, name: str, kind: str = "sql", mat: str = "table", tags=None) -> Node:
    p = tmp_path / (name + (".sql" if kind == "sql" else ".py"))
    p.write_text("-- stub\n", encoding="utf-8")
    n = Node(name=name, kind=kind, path=p, deps=[], meta={"materialized": mat})
    if tags is not None:
        n.meta["tags"] = tags
    return n


def test_select_predicates_and_parse(tmp_path: Path):
    a = _mk_node(tmp_path, "users", mat="view", tags=["mart", "dim"])
    b = _mk_node(tmp_path, "orders.ff", mat="table", tags=["fct"])
    c = _mk_node(tmp_path, "ephemeral_tmp", mat="ephemeral", tags=[])

    REGISTRY.nodes = {x.name: x for x in (a, b, c)}

    # name glob matches logical or physical name (orders.ff -> 'orders')
    tokens = _parse_select(["orders"])
    pred = _selector(_build_predicates(tokens))
    assert pred(b) is True
    assert pred(a) is False

    # tag:mart
    tokens = _parse_select(["tag:mart"])
    pred = _selector(_build_predicates(tokens))
    assert pred(a) is True and pred(b) is False and pred(c) is False

    # type:view
    tokens = _parse_select(["type:view"])
    pred = _selector(_build_predicates(tokens))
    assert pred(a) is True and pred(b) is False and pred(c) is False

    # kind:python (none in this set)
    tokens = _parse_select(["kind:python"])
    pred = _selector(_build_predicates(tokens))
    assert pred(a) is False and pred(b) is False and pred(c) is False

    # AND-combo: tag:mart AND type:view
    tokens = _parse_select(["tag:mart", "type:view"])
    pred = _selector(_build_predicates(tokens))
    assert pred(a) is True and pred(b) is False and pred(c) is False


# ---------------------------------
# CLI-level: run / dag with Typer
# ---------------------------------
def test_cli_run_and_dag_apply_select_filters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Arrange a tiny in-memory registry
    a = _mk_node(tmp_path, "users", mat="view", tags=["mart"])
    b = _mk_node(tmp_path, "orders.ff", mat="table", tags=["fct"])
    c = _mk_node(tmp_path, "stg_events", mat="ephemeral", tags=["stg"])
    REGISTRY.nodes = {x.name: x for x in (a, b, c)}

    # Patch project loader to avoid filesystem discovery
    def fake_load_project_and_env(project_arg: str):
        # ensure a minimal Jinja env exists
        env = Environment()
        REGISTRY.env = env
        return tmp_path, env

    monkeypatch.setattr("flowforge.cli.bootstrap._load_project_and_env", fake_load_project_and_env)

    # Patch profile resolution to avoid reading profiles.yml / env
    def fake_resolve_profile(env_name, engine, proj):
        # return dummy EnvSettings + duckdb-like profile struct
        env = SimpleNamespace()
        prof = SimpleNamespace(engine="duckdb", duckdb=SimpleNamespace(path=":memory:"))
        return env, prof

    monkeypatch.setattr("flowforge.cli.bootstrap._resolve_profile", fake_resolve_profile)

    # Patch executor factory to avoid any real DB work
    calls = {"run_sql": [], "run_py": []}

    class DummyEx:
        pass

    def fake_make_executor(prof, jenv):
        ex = DummyEx()

        def run_sql(node):
            calls["run_sql"].append(node.name)

        def run_py(node):
            calls["run_py"].append(node.name)

        return ex, run_sql, run_py

    monkeypatch.setattr("flowforge.cli.bootstrap._make_executor", fake_make_executor)

    # Patch topo_sort to preserve deterministic order users -> orders.ff -> stg_events
    def fake_topo(nodes):
        return ["users", "orders.ff", "stg_events"]

    monkeypatch.setattr("flowforge.cli.test_cmd.topo_sort", fake_topo, raising=False)

    runner = CliRunner()

    # ---- RUN: filter only views with tag:mart
    result = runner.invoke(
        app, ["run", str(tmp_path), "--select", "tag:mart", "--select", "type:view"]
    )
    assert result.exit_code == 0, result.output
    # Only 'users' should have been executed, and it's SQL kind in our setup
    assert calls["run_sql"] == ["users"]
    assert calls["run_py"] == []

    calls["run_sql"].clear()
    calls["run_py"].clear()

    # ---- RUN: filter by name glob 'orders' (matches physical relation of 'orders.ff')
    result = runner.invoke(app, ["run", str(tmp_path), "--select", "orders"])
    assert result.exit_code == 0, result.output
    assert calls["run_sql"] == ["orders.ff"]

    # ---- DAG: ensure filtered nodes passed into render_site
    captured = {}

    def fake_render_site(out_dir, nodes_dict, executor=None):
        captured["nodes"] = set(nodes_dict.keys())

    monkeypatch.setattr("flowforge.cli.dag_cmd.render_site", fake_render_site)

    # select kind:sql excludes none here (all sql), but test with 'ephemeral'
    # type to keep just stg_events
    result = runner.invoke(app, ["dag", str(tmp_path), "--html", "--select", "type:ephemeral"])
    assert result.exit_code == 0, result.output
    # Only the ephemeral node should be included in docs rendering
    assert captured["nodes"] == {"stg_events"}
