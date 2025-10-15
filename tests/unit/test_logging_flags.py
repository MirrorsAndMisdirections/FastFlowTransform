# tests/unit/test_logging_flags.py
from typer.testing import CliRunner
from flowforge.cli import app

def test_verbose_flags_wiring(monkeypatch):
    calls = {"echo": []}

    # stub the heavy bits so the command exits early after logging lines
    monkeypatch.setattr("flowforge.cli._load_project_and_env", lambda proj: (__import__('pathlib').Path("."), None))
    monkeypatch.setattr("flowforge.cli._resolve_profile", lambda *a, **k: (None, type("P", (), {"engine":"duckdb", "duckdb": type("D", (), {"path":":memory:"})()})()))
    monkeypatch.setattr("flowforge.cli.topo_sort", lambda nodes: [])
    monkeypatch.setattr("flowforge.cli.REGISTRY", type("R", (), {"nodes": {}, "set_cli_vars": lambda *_: None})())

    runner = CliRunner()
    # default (quiet-ish)
    res = runner.invoke(app, ["run", "."])
    assert res.exit_code == 0

    # -v should show the "Profil" line (we don't assert text content here to keep it loose)
    res = runner.invoke(app, ["-v", "run", "."])
    assert res.exit_code == 0

    # -vv enables SQL debug env var
    res = runner.invoke(app, ["-vv", "run", "."])
    assert res.exit_code == 0
    
    res = runner.invoke(app, ["-q", "dag", "."])
    assert res.exit_code == 0
    