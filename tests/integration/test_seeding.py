from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

from fastflowtransform.core import REGISTRY
from fastflowtransform.executors.duckdb_exec import DuckExecutor
from fastflowtransform.executors.postgres_exec import PostgresExecutor
from fastflowtransform.seeding import seed_project


def _make_env(project: Path) -> Environment:
    return Environment(loader=FileSystemLoader(str(project / "models")))


@pytest.mark.duckdb
def test_duckdb_unit_seeding(tmp_path, duckdb_project):
    ex = DuckExecutor(db_path=":memory:")
    # Load registry + Jinja so run() works later
    REGISTRY.load_project(duckdb_project)

    # Load seeds directly (without the CLI)
    n = seed_project(duckdb_project, ex)
    assert n >= 1

    # Afterwards you can execute nodes via ex.run_sql/ex.run_python
    # or continue using the CLI in other tests.


@pytest.mark.postgres
def test_pg_unit_seeding(pg_project, pg_env):
    ex = PostgresExecutor(dsn=pg_env["FF_PG_DSN"], schema=pg_env.get("FF_PG_SCHEMA"))
    n = seed_project(pg_project, ex, default_schema=pg_env.get("FF_PG_SCHEMA"))
    assert n >= 1
