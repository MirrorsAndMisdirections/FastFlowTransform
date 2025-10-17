import os

import pytest

from tests.common.utils import ROOT, run


# ---- DuckDB ----
@pytest.fixture(scope="session")
def duckdb_project():
    return ROOT / "examples" / "simple_duckdb"


@pytest.fixture(scope="session")
def duckdb_db_path(duckdb_project):
    p = duckdb_project / ".local" / "demo.duckdb"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


@pytest.fixture(scope="session")
def duckdb_env(duckdb_db_path):
    return {"FF_ENGINE": "duckdb", "FF_DUCKDB_PATH": str(duckdb_db_path)}


@pytest.fixture(scope="module")
def duckdb_seeded(duckdb_project, duckdb_env):
    run(["flowforge", "seed", str(duckdb_project), "--env", "dev"], duckdb_env)
    yield


# ---- Postgres ----
@pytest.fixture(scope="session")
def pg_project():
    return ROOT / "examples" / "postgres"


@pytest.fixture(scope="session")
def pg_env():
    # Passe DSN/Schema bei Bedarf an dein Profil an
    dsn = os.environ.get("FF_PG_DSN", "postgresql+psycopg://postgres:postgres@localhost:5432/ffdb")
    schema = os.environ.get("FF_PG_SCHEMA", "public")  # falls Profile das verwenden
    return {"FF_ENGINE": "postgres", "FF_PG_DSN": dsn, "FF_PG_SCHEMA": schema}


@pytest.fixture(scope="module")
def pg_seeded(pg_project, pg_env):
    # optional: Datenbank leeren/neu erstellen - je nach CI-Setup
    run(["flowforge", "seed", str(pg_project), "--env", "stg"], pg_env)
    yield
