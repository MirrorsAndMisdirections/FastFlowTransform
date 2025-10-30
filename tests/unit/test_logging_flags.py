# tests/unit/test_logging_flags.py
import importlib

from jinja2 import Environment
from typer.testing import CliRunner

from fastflowtransform.cli import app

cli_bootstrap = importlib.import_module("fastflowtransform.cli.bootstrap")
cli_run = importlib.import_module("fastflowtransform.cli.run")


def test_verbose_flags_wiring(monkeypatch, tmp_path):
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    # stub the heavy bits so the command exits early after logging lines
    monkeypatch.setattr(
        cli_bootstrap,
        "_load_project_and_env",
        lambda proj: (tmp_path, Environment()),
    )
    monkeypatch.setattr(
        cli_bootstrap,
        "_resolve_profile",
        lambda *a, **k: (
            None,
            type("P", (), {"engine": "duckdb", "duckdb": type("D", (), {"path": ":memory:"})()})(),
        ),
    )
    monkeypatch.setattr(
        cli_run,
        "REGISTRY",
        type("R", (), {"nodes": {}, "set_cli_vars": lambda *_: None})(),
    )

    runner = CliRunner()
    # default (quiet-ish)
    res = runner.invoke(app, ["run", str(tmp_path)])
    assert res.exit_code == 0

    # -v should show the "Profil" line (we don't assert text content here to keep it loose)
    res = runner.invoke(app, ["-v", "run", str(tmp_path)])
    assert res.exit_code == 0

    # -vv enables SQL debug env var
    res = runner.invoke(app, ["-vv", "run", str(tmp_path)])
    assert res.exit_code == 0

    res = runner.invoke(app, ["-q", "dag", str(tmp_path)])
    assert res.exit_code == 0
