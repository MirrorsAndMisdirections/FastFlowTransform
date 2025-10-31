# tests/unit/cli/test_seed_cmd_unit.py
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import typer

import fastflowtransform.cli.seed_cmd as seed_mod


@pytest.mark.unit
def test_seed_happy_path(monkeypatch, tmp_path: Path):
    """
    seed(...) should:
    - prepare context
    - call ctx.make_executor()
    - call seed_project(...) with project, executor, and default_schema (None for non-postgres)
    - echo a humanized message
    """
    called = {
        "prepare_ctx": None,
        "make_exec": 0,
        "seed_project": None,
        "echo": None,
    }

    # fake executor
    fake_executor = object()

    # fake context returned by _prepare_context
    fake_ctx = SimpleNamespace(
        project=tmp_path,
        profile=SimpleNamespace(engine="duckdb"),  # not postgres → default_schema=None
        make_executor=lambda: (
            called.__setitem__("make_exec", called["make_exec"] + 1) or (fake_executor, None, None)
        ),
    )

    # patch _prepare_context
    monkeypatch.setattr(
        seed_mod,
        "_prepare_context",
        lambda project, env_name, engine, vars: called.__setitem__(
            "prepare_ctx", (project, env_name, engine, vars)
        )
        or fake_ctx,
        raising=True,
    )

    # patch seed_project
    def fake_seed_project(project, executor, default_schema):
        called["seed_project"] = (project, executor, default_schema)
        return 3  # pretend we seeded 3 tables

    monkeypatch.setattr(seed_mod, "seed_project", fake_seed_project, raising=True)

    # patch echo
    monkeypatch.setattr(
        seed_mod,
        "echo",
        lambda msg: called.__setitem__("echo", msg),
        raising=True,
    )

    # ACT
    seed_mod.seed(project=".", env_name="dev", engine=None, vars=None)

    # ASSERT
    # 1) context got correct params
    assert called["prepare_ctx"] == (".", "dev", None, None)
    # 2) executor was created
    assert called["make_exec"] == 1
    # 3) seed_project was called with project + executor + None (because duckdb)
    proj_arg, exec_arg, schema_arg = called["seed_project"]
    assert proj_arg == tmp_path
    assert exec_arg is fake_executor
    assert schema_arg is None
    # 4) message was printed, humanized
    assert "3 table(s)" in called["echo"]


@pytest.mark.unit
def test_seed_uses_postgres_schema_if_profile_is_postgres(monkeypatch, tmp_path: Path):
    """
    If profile.engine == 'postgres', seed(...) should pass profile.postgres.db_schema
    as default_schema into seed_project(...).
    """
    called = {}

    fake_executor = object()

    fake_ctx = SimpleNamespace(
        project=tmp_path,
        profile=SimpleNamespace(
            engine="postgres",
            postgres=SimpleNamespace(db_schema="public"),
        ),
        make_executor=lambda: (fake_executor, None, None),
    )

    monkeypatch.setattr(seed_mod, "_prepare_context", lambda *a, **k: fake_ctx, raising=True)

    def fake_seed_project(project, executor, default_schema):
        called["args"] = (project, executor, default_schema)
        return 1

    monkeypatch.setattr(seed_mod, "seed_project", fake_seed_project, raising=True)
    monkeypatch.setattr(seed_mod, "echo", lambda *_: None, raising=True)

    # ACT
    seed_mod.seed(project=".", env_name="dev")

    # ASSERT
    proj_arg, exec_arg, schema_arg = called["args"]
    assert proj_arg == tmp_path
    assert exec_arg is fake_executor
    assert schema_arg == "public"  # <-- this is the important part


@pytest.mark.unit
def test_seed_passes_through_cli_vars(monkeypatch, tmp_path: Path):
    """
    We just want to be sure that whatever --vars / vars=... arrives
    is forwarded to _prepare_context.
    """
    captured = {}

    fake_ctx = SimpleNamespace(
        project=tmp_path,
        profile=SimpleNamespace(engine="duckdb"),
        make_executor=lambda: (object(), None, None),
    )

    def fake_prepare(project, env_name, engine, vars):
        captured["prepare"] = (project, env_name, engine, vars)
        return fake_ctx

    monkeypatch.setattr(seed_mod, "_prepare_context", fake_prepare, raising=True)
    monkeypatch.setattr(seed_mod, "seed_project", lambda *_: 0, raising=True)
    monkeypatch.setattr(seed_mod, "echo", lambda *_: None, raising=True)

    # ACT
    seed_mod.seed(project=".", env_name="dev", vars=["day=2025-10-01", "limit=5"])

    # ASSERT
    assert captured["prepare"] == (".", "dev", None, ["day=2025-10-01", "limit=5"])


@pytest.mark.unit
def test_register_adds_command():
    app = typer.Typer()
    seed_mod.register(app)

    names: set[str] = set()
    for cmd in app.registered_commands:
        if cmd.name:
            names.add(cmd.name)
        elif cmd.callback is not None:
            names.add(cmd.callback.__name__)

    assert "seed" in names
