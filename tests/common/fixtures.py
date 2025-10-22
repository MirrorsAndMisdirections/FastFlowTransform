import os
from contextlib import suppress
from pathlib import Path

import psycopg
import pytest
from jinja2 import Environment, FileSystemLoader, select_autoescape
from psycopg import sql

from fastflowtransform.core import REGISTRY
from tests.common.utils import ROOT, run


# ---- Jinja Env ----
@pytest.fixture(scope="session")
def jinja_env():
    """
    Minimal Jinja2 rnvironment for rendering-tests.
    """
    env = Environment(
        loader=FileSystemLoader(["."]),
        autoescape=select_autoescape([]),
        trim_blocks=True,
        lstrip_blocks=True,
    )

    env.globals.setdefault("is_incremental", lambda: False)
    env.globals.setdefault("this", None)
    env.globals.setdefault(
        "var",
        lambda k, default=None: (
            (getattr(REGISTRY, "vars", {}) or {}).get(k, default) if REGISTRY else default
        ),
    )
    return env


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
    db_path = duckdb_env.get("FF_DUCKDB_PATH")
    if db_path:
        db_file = Path(db_path)
        if db_file.exists():
            db_file.unlink()
            # ensure parent dir exists for fresh DB creation
            db_file.parent.mkdir(parents=True, exist_ok=True)
    run(["fft", "seed", str(duckdb_project), "--env", "dev"], duckdb_env)
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
    dsn = pg_env.get("FF_PG_DSN")
    schema = pg_env.get("FF_PG_SCHEMA") or "public"
    if dsn and schema and ("psycopg://" in dsn or "+psycopg" in dsn):
        with suppress(Exception), psycopg.connect(dsn) as conn:
            conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            conn.commit()
    run(["fft", "seed", str(pg_project), "--env", "stg"], pg_env)
    yield
