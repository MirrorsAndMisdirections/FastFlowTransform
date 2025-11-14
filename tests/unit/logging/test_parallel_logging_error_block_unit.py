from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest
from jinja2 import Environment
from typer.testing import CliRunner

from fastflowtransform.cli import app
from fastflowtransform.core import REGISTRY, Node
from fastflowtransform.run_executor import schedule as real_schedule

cli_bootstrap = importlib.import_module("fastflowtransform.cli.bootstrap")
cli_run = importlib.import_module("fastflowtransform.cli.run")


@pytest.mark.unit
def test_error_block_prints_after_logs_without_interleaving(monkeypatch, tmp_path):
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    # Minimal schedule stub: one level, one failing node.
    def fake_schedule(levels, **kw):
        # Run the real schedule but inject a failing run_node
        def run_node(name: str):
            raise KeyError("boom")

        kw = {**kw, "run_node": run_node}
        return real_schedule(levels, **kw)

    monkeypatch.setattr(cli_run, "schedule", fake_schedule)

    # Stub DAG + registry: just one node name in one level
    monkeypatch.setattr(cli_run, "dag_levels", lambda _nodes: [["failing"]])

    # Stub project/profile/executor to avoid I/O
    def fake_load_project_and_env(project_arg: str):
        REGISTRY.nodes = {"failing": Node("failing", "sql", path=None, deps=[], meta={})}  # type: ignore[arg-type]
        REGISTRY.env = Environment()
        return project_arg, REGISTRY.env

    def fake_resolve_profile(env_name, engine, proj):
        return (
            SimpleNamespace(),
            SimpleNamespace(engine="duckdb", duckdb=SimpleNamespace(path=":memory:")),
        )

    def fake_make_executor(_p, _e):
        class E:
            pass

        return E(), lambda n: (_ for _ in ()).throw(KeyError("boom")), lambda n: None

    monkeypatch.setattr(cli_bootstrap, "_load_project_and_env", fake_load_project_and_env)
    monkeypatch.setattr(cli_bootstrap, "_resolve_profile", fake_resolve_profile)
    monkeypatch.setattr(cli_bootstrap, "_make_executor", fake_make_executor)

    runner = CliRunner()
    res = runner.invoke(app, ["run", str(tmp_path), "--cache", "off"])
    # Exit with error
    assert res.exit_code != 0
    # Logs first (including ✖ line), then the error block (starts with '┌')
    # Ensure the error block wasn't interleaved with preceding lines
    out = res.output
    assert "✖ L01 [DUCK] failing" in out
    assert "┌" in out and out.index("┌") > out.index("✖ L01 [DUCK] failing")
