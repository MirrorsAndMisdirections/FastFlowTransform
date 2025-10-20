from types import SimpleNamespace

from jinja2 import Environment
from typer.testing import CliRunner

from fastflowtransform.cli import app
from fastflowtransform.core import REGISTRY, Node


def _mk_node(tmp, name, kind="sql", deps=None, mat="table", tags=None):
    p = tmp / f"{name}.dummy"
    p.write_text("--", encoding="utf-8")
    n = Node(
        name=name,
        kind=kind,
        path=p,
        deps=list(deps or []),
        meta={"materialized": mat, "tags": tags or []},
    )
    return n


def test_run_select_includes_upstream_and_excludes_others(tmp_path, monkeypatch):
    # Graph:
    # users.ff  -> users_enriched
    # orders.ff -> mart_orders_enriched (depends on users_enriched, orders.ff)
    a = _mk_node(tmp_path, "users.ff")
    b = _mk_node(tmp_path, "users_enriched", deps=["users.ff"])
    c = _mk_node(tmp_path, "orders.ff")
    d = _mk_node(tmp_path, "mart_orders_enriched", deps=["users_enriched", "orders.ff"])

    REGISTRY.nodes = {x.name: x for x in (a, b, c, d)}

    # Minimal env/prof
    def fake_load_project_and_env(project_arg: str):
        env = Environment()
        REGISTRY.env = env
        return tmp_path, env

    monkeypatch.setattr(
        "fastflowtransform.cli.bootstrap._load_project_and_env", fake_load_project_and_env
    )

    def fake_resolve_profile(env_name, engine, proj):
        env = SimpleNamespace()
        prof = SimpleNamespace(engine="duckdb", duckdb=SimpleNamespace(path=":memory:"))
        return env, prof

    monkeypatch.setattr("fastflowtransform.cli.bootstrap._resolve_profile", fake_resolve_profile)

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

    monkeypatch.setattr("fastflowtransform.cli.bootstrap._make_executor", fake_make_executor)

    # Use real dag.levels for safety (but graph is simple)
    runner = CliRunner()
    res = runner.invoke(app, ["run", str(tmp_path), "--select", "users_enriched"])
    assert res.exit_code == 0, res.output
    # Expect: users.ff (dep) AND users_enriched; not orders.* or marts
    assert set(calls["run_sql"]) == {"users.ff", "users_enriched"}
    assert calls["run_py"] == []


def test_run_exclude_removes_targets_and_downstream(tmp_path, monkeypatch):
    # Graph: stg -> mart
    stg = _mk_node(tmp_path, "stg_events")
    mart = _mk_node(tmp_path, "mart_sessions", deps=["stg_events"])
    REGISTRY.nodes = {x.name: x for x in (stg, mart)}

    def fake_load_project_and_env(project_arg: str):
        env = Environment()
        REGISTRY.env = env
        return tmp_path, env

    monkeypatch.setattr(
        "fastflowtransform.cli.bootstrap._load_project_and_env", fake_load_project_and_env
    )

    def fake_resolve_profile(env_name, engine, proj):
        return (
            SimpleNamespace(),
            SimpleNamespace(engine="duckdb", duckdb=SimpleNamespace(path=":memory:")),
        )

    monkeypatch.setattr("fastflowtransform.cli.bootstrap._resolve_profile", fake_resolve_profile)

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

    monkeypatch.setattr("fastflowtransform.cli.bootstrap._make_executor", fake_make_executor)

    runner = CliRunner()
    # No select → seeds = all; exclude mart_* removes mart and its downstream (none),
    # stg_events remains and should be built.
    res = runner.invoke(app, ["run", str(tmp_path), "--exclude", "mart_*"])
    assert res.exit_code == 0, res.output
    assert set(calls["run_sql"]) == {"stg_events"}


def test_run_select_conflicts_with_exclude_drops_invalid_targets(tmp_path, monkeypatch):
    # Graph: A -> B; select B, exclude A ⇒ nothing to run
    a = _mk_node(tmp_path, "users_enriched", deps=["users.ff"])
    b = _mk_node(tmp_path, "users.ff")
    REGISTRY.nodes = {x.name: x for x in (a, b)}

    def fake_load_project_and_env(project_arg: str):
        env = Environment()
        REGISTRY.env = env
        return tmp_path, env

    monkeypatch.setattr(
        "fastflowtransform.cli.bootstrap._load_project_and_env", fake_load_project_and_env
    )

    def fake_resolve_profile(env_name, engine, proj):
        return (
            SimpleNamespace(),
            SimpleNamespace(engine="duckdb", duckdb=SimpleNamespace(path=":memory:")),
        )

    monkeypatch.setattr("fastflowtransform.cli.bootstrap._resolve_profile", fake_resolve_profile)

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

    monkeypatch.setattr("fastflowtransform.cli.bootstrap._make_executor", fake_make_executor)

    runner = CliRunner()
    res = runner.invoke(
        app, ["run", str(tmp_path), "--select", "users_enriched", "--exclude", "users.ff"]
    )
    # Selection becomes empty (target invalid), CLI exits 0 with message
    assert res.exit_code == 0, res.output
    assert calls["run_sql"] == []
