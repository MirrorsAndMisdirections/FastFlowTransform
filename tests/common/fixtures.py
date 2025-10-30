# tests/common/fixtures.py
import os
from contextlib import suppress
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import psycopg
import pytest
from jinja2 import Environment, FileSystemLoader, select_autoescape
from psycopg import sql

from fastflowtransform import utest
from fastflowtransform.core import REGISTRY
from fastflowtransform.executors.databricks_spark_exec import DatabricksSparkExecutor
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


# ---- Spark ----
@pytest.fixture
def exec_minimal(monkeypatch):
    with patch("fastflowtransform.executors.databricks_spark_exec.SparkSession") as SP:
        fake_spark = MagicMock()
        SP.builder.master.return_value.appName.return_value.getOrCreate.return_value = fake_spark
        ex = DatabricksSparkExecutor()
    # accept mocks as frames in unit tests
    monkeypatch.setattr(ex, "_is_frame", lambda obj: True)
    return ex


@pytest.fixture
def exec_factory():
    """
    Build a DatabricksSparkExecutor with arbitrary __init__ kwargs,
    but always with mocked SparkSession (no real JVM).
    Returns (executor, fake_builder, fake_spark).
    """

    def _make(**kwargs):
        with patch("fastflowtransform.executors.databricks_spark_exec.SparkSession") as SP:
            fake_builder = SP.builder.master.return_value.appName.return_value
            # make .config(...) chainable
            fake_builder.config.return_value = fake_builder
            fake_builder.enableHiveSupport.return_value = fake_builder
            fake_spark = MagicMock()
            fake_builder.getOrCreate.return_value = fake_spark

            ex = DatabricksSparkExecutor(**kwargs)
        return ex, fake_builder, fake_spark

    return _make


@pytest.fixture(scope="session")
def spark_tmpdir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("spark_wh")


@pytest.fixture(scope="session")
def spark_exec(spark_tmpdir: Path) -> DatabricksSparkExecutor:
    return DatabricksSparkExecutor(
        master="local[*]",
        app_name="fft-it",
        warehouse_dir=str(spark_tmpdir),
        database="default",
    )


# ---- utest ----
@pytest.fixture
def fake_registry(tmp_path, monkeypatch):
    # wir brauchen ein REGISTRY mit projekt-dir und 1 node
    node = SimpleNamespace(name="model_a", kind="sql", deps=["src1"])
    reg = SimpleNamespace(
        nodes={"model_a": node},
        sources={},
        get_project_dir=lambda: tmp_path,
    )
    monkeypatch.setattr(utest, "REGISTRY", reg)
    # relation_for -> immer schema.model
    monkeypatch.setattr(utest, "relation_for", lambda name: f"public.{name}")
    return reg


@pytest.fixture
def duckdb_executor():
    """
    Fake-Executor, der dem DuckDB-Pfad ähnelt:
    - hat .con
    - con.register(...)
    - con.execute(...)
    - con.table(...).df()
    """
    con = MagicMock()
    # für _read_result (duckdb)
    table_df = pd.DataFrame([{"id": 1}])
    con.table.return_value.df.return_value = table_df

    class DuckEx:
        def __init__(self, con):
            self.con = con

        # für _execute_node(sql)
        def run_sql(self, node, jenv):
            # schreibt nix, simuliert nur Erfolg
            return None

        def run_python(self, node):
            return None

    return DuckEx(con)


@pytest.fixture
def postgres_executor():
    """
    Fake-Executor für den Postgres-Zweig in _read_result.
    """
    engine = MagicMock()

    class PgEx:
        def __init__(self, engine):
            self.engine = engine
            self.schema = "public"

    return PgEx(engine)
