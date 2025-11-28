from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment
from typer.testing import CliRunner

from fastflowtransform.cli import app
from fastflowtransform.core import REGISTRY, Node


def _stub_minimal_context(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Stub project/profile/executor so the command can run without I/O."""
    # Make sure the minimal project skeleton passes CLI path validation.
    (tmp_path / "models").mkdir(parents=True, exist_ok=True)

    # minimal profiles.yml with dev + dev_utest
    profiles_yaml = """
dev:
  engine: duckdb
  duckdb:
    path: "dummy_path"

dev_utest:
  engine: duckdb
  duckdb:
    path: ":memory:"
""".strip()

    (tmp_path / "profiles.yml").write_text(profiles_yaml)

    def fake_load_project_and_env(project_arg: str):
        # Minimal registry with one model file path (not used by utest runner)
        REGISTRY.nodes = {
            "dummy": Node(
                "dummy",
                "sql",
                path=tmp_path / "models" / "dummy.ff.sql",
                deps=[],
                meta={},
            )
        }
        REGISTRY.env = Environment()
        # Important: project_dir must be tmp_path so the CLI finds profiles.yml there
        return tmp_path, REGISTRY.env

    def fake_resolve_profile(env_name, engine, proj):
        # We ignore env_name/engine here - tests don't care about real profile contents.
        return (
            SimpleNamespace(),  # raw profile (unused)
            SimpleNamespace(engine="duckdb", duckdb=SimpleNamespace(path=":memory:")),
        )

    class DummyExec: ...

    def fake_make_executor(_prof, _env):
        return DummyExec(), lambda n: None, lambda n: None

    monkeypatch.setattr(
        "fastflowtransform.cli.bootstrap._load_project_and_env", fake_load_project_and_env
    )
    monkeypatch.setattr("fastflowtransform.cli.bootstrap._resolve_profile", fake_resolve_profile)
    monkeypatch.setattr("fastflowtransform.cli.bootstrap._make_executor", fake_make_executor)


@pytest.mark.integration
def test_utest_cache_default_off(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    _stub_minimal_context(monkeypatch, tmp_path)

    captured = {}

    def fake_discover(project, path=None, only_model=None):
        return [{"model": "x"}]

    def fake_run(specs, executor, jenv, only_case=None, **kw):
        captured.update(kw)
        return 0

    monkeypatch.setattr("fastflowtransform.cli.utest_cmd.discover_unit_specs", fake_discover)
    monkeypatch.setattr("fastflowtransform.cli.utest_cmd.run_unit_specs", fake_run)

    res = CliRunner().invoke(app, ["utest", str(tmp_path)])
    assert res.exit_code == 0, res.output
    # default must be 'off'
    assert captured.get("cache_mode") == "off"
    assert captured.get("reuse_meta") is False


@pytest.mark.integration
def test_utest_cache_rw_and_reuse(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    _stub_minimal_context(monkeypatch, tmp_path)

    captured = {}

    def fake_discover(project, path=None, only_model=None):
        return [{"model": "x"}]

    def fake_run(specs, executor, jenv, only_case=None, **kw):
        captured.update(kw)
        return 0

    monkeypatch.setattr("fastflowtransform.cli.utest_cmd.discover_unit_specs", fake_discover)
    monkeypatch.setattr("fastflowtransform.cli.utest_cmd.run_unit_specs", fake_run)

    res = CliRunner().invoke(app, ["utest", str(tmp_path), "--cache", "rw", "--reuse-meta"])
    assert res.exit_code == 0, res.output
    assert captured.get("cache_mode") == "rw"
    assert captured.get("reuse_meta") is True


@pytest.mark.integration
def test_utest_cache_ro(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    _stub_minimal_context(monkeypatch, tmp_path)

    captured = {}

    def fake_discover(project, path=None, only_model=None):
        return [{"model": "x"}]

    def fake_run(specs, executor, jenv, only_case=None, **kw):
        captured.update(kw)
        return 0

    monkeypatch.setattr("fastflowtransform.cli.utest_cmd.discover_unit_specs", fake_discover)
    monkeypatch.setattr("fastflowtransform.cli.utest_cmd.run_unit_specs", fake_run)

    res = CliRunner().invoke(app, ["utest", str(tmp_path), "--cache", "ro"])
    assert res.exit_code == 0, res.output
    assert captured.get("cache_mode") == "ro"
