from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment
from typer.testing import CliRunner

from flowforge.cli import app
from flowforge.core import REGISTRY
from flowforge.executors.duckdb_exec import DuckExecutor
from flowforge.meta import get_meta


def _write_sql_model(project: Path, name: str, sql: str) -> None:
    (project / "models").mkdir(parents=True, exist_ok=True)
    # Logical node name for SQL models is "<stem>.ff"
    # File should be "name.ff.sql"
    (project / "models" / f"{name}.ff.sql").write_text(sql, encoding="utf-8")


def _profile_stub(tmp_path: Path):
    # Use a real DuckDB file so we can inspect it after CLI run
    db_path = str(tmp_path / "test.duckdb")

    def fake_load_project_and_env(project_arg: str):
        # ⚠️ IMPORTANT: actually load the project so REGISTRY.nodes is populated
        proj = Path(project_arg)
        REGISTRY.load_project(proj)
        # Reuse the env that load_project created (fallback to a fresh one)
        env = REGISTRY.env or Environment()
        return proj, env

    def fake_resolve_profile(env_name, engine, proj):
        env = SimpleNamespace()
        prof = SimpleNamespace(engine="duckdb", duckdb=SimpleNamespace(path=db_path))
        return env, prof

    return fake_load_project_and_env, fake_resolve_profile, db_path


@pytest.mark.duckdb
def test_executor_meta_hook_duckdb_build_then_skip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Arrange project with a single SQL model
    proj = tmp_path / "proj"
    proj.mkdir(parents=True, exist_ok=True)
    _write_sql_model(
        proj,
        "users",
        """{{ config(materialized='table') }}
           select 1 as id
        """,
    )

    # Stub project loading and profile resolution to use our file-based DB
    fake_lpae, fake_resolve, db_path = _profile_stub(tmp_path)
    monkeypatch.setattr("flowforge.cli._load_project_and_env", fake_lpae)
    monkeypatch.setattr("flowforge.cli._resolve_profile", fake_resolve)

    # First run: build (cache default rw), should create meta row
    runner = CliRunner()
    res1 = runner.invoke(app, ["run", str(proj), "--cache", "rw"])
    assert res1.exit_code == 0, res1.output

    # Inspect meta using a separate executor on the same DB file
    ex = DuckExecutor(db_path=db_path)
    row1 = get_meta(ex, "users.ff")
    assert row1 is not None, "meta row should exist after build"
    fp1, rel1, built1, eng1 = row1
    assert rel1 == "users"
    assert eng1 == "duckdb"

    # Sleep a tick to make a built_at change visible if it were updated
    time.sleep(0.05)

    # Second run: cache hit -> node skipped -> meta must remain unchanged
    res2 = runner.invoke(app, ["run", str(proj), "--cache", "rw"])
    assert res2.exit_code == 0, res2.output

    row2 = get_meta(ex, "users.ff")
    assert row2 is not None
    fp2, rel2, built2, eng2 = row2
    assert fp2 == fp1
    assert rel2 == rel1
    assert eng2 == eng1
    # AC: skipped node does not update meta → built_at unchanged
    assert built2 == built1, "built_at must not change on skip"
