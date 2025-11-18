# tests/common/fixtures.py
from __future__ import annotations

import os
from contextlib import suppress
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import sqlalchemy as sa
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy import text

if TYPE_CHECKING:  # pragma: no cover - typing only
    import psycopg
    from psycopg import sql

    from fastflowtransform.executors.databricks_spark import (
        DatabricksSparkExecutor as DatabricksSparkExecutorType,
    )
else:
    try:
        import psycopg  # type: ignore
        from psycopg import sql  # type: ignore
    except ModuleNotFoundError:
        psycopg = None  # type: ignore
        sql = None  # type: ignore

    DatabricksSparkExecutorType = Any

try:  # Optional: Spark deps may not be installed in core runs
    from fastflowtransform.executors.databricks_spark import DatabricksSparkExecutor
except ModuleNotFoundError:  # pragma: no cover - import guard
    DatabricksSparkExecutor = None  # type: ignore

from fastflowtransform import utest
from fastflowtransform.core import REGISTRY
from tests.common.utils import ROOT, run

try:  # Optional: Spark deps may not be installed in core runs
    from fastflowtransform.executors.databricks_spark import DatabricksSparkExecutor
except ModuleNotFoundError:  # pragma: no cover - import guard
    DatabricksSparkExecutor = None  # type: ignore


# ---- Load Env Variables ----
@pytest.fixture(scope="session", autouse=True)
def load_test_env():
    candidates = [
        ROOT / "tests" / ".env",
        ROOT / "tests" / ".env.dev_databricks",
        ROOT / "tests" / ".env.dev_duckdb",
        ROOT / "tests" / ".env.dev_postgres",
    ]

    for env_file in candidates:
        if env_file.is_file():
            load_dotenv(env_file, override=False)


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


@pytest.fixture(scope="function")
def duckdb_seeded(duckdb_project, duckdb_env):
    db_path = duckdb_env.get("FF_DUCKDB_PATH")
    db_file = Path(db_path) if db_path else None
    if db_file:
        if db_file.exists():
            db_file.unlink()
        db_file.parent.mkdir(parents=True, exist_ok=True)
    run(["fft", "seed", str(duckdb_project), "--env", "dev"], duckdb_env)
    try:
        yield
    finally:
        if db_file:
            with suppress(Exception):
                db_file.unlink()


# ---- Postgres ----
@pytest.fixture(scope="session")
def pg_project():
    return ROOT / "examples" / "postgres"


@pytest.fixture(scope="session")
def pg_env():
    dsn = os.environ.get("FF_PG_DSN", "postgresql+psycopg://postgres:postgres@localhost:5432/ffdb")
    schema = os.environ.get("FF_PG_SCHEMA", "public")
    return {"FF_ENGINE": "postgres", "FF_PG_DSN": dsn, "FF_PG_SCHEMA": schema}


@pytest.fixture(scope="module")
def pg_seeded(pg_project, pg_env):
    dsn = pg_env.get("FF_PG_DSN")
    schema = pg_env.get("FF_PG_SCHEMA") or "public"
    if psycopg is None or sql is None:
        pytest.skip("psycopg not installed; install fastflowtransform[postgres] to run PG fixtures")
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
    if DatabricksSparkExecutor is None:
        pytest.skip(
            "pyspark/delta not installed; install fastflowtransform[spark] to run Spark tests"
        )
    with patch("fastflowtransform.executors.databricks_spark.SparkSession") as SP:
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

    def _make(**kwargs) -> tuple[DatabricksSparkExecutorType, Any, MagicMock]:
        if DatabricksSparkExecutor is None:
            pytest.skip(
                "pyspark/delta not installed; install fastflowtransform[spark] to run Spark tests"
            )
        with patch("fastflowtransform.executors.databricks_spark.SparkSession") as SP:
            fake_builder = SP.builder.master.return_value.appName.return_value
            # make .config(...) chainable
            fake_builder.config.return_value = fake_builder
            fake_builder.enableHiveSupport.return_value = fake_builder
            fake_conf = MagicMock()
            fake_sc_conf = MagicMock()
            fake_sc = MagicMock(getConf=MagicMock(return_value=fake_sc_conf))
            fake_spark = MagicMock(conf=fake_conf, sparkContext=fake_sc)
            fake_builder.getOrCreate.return_value = fake_spark

            ex = DatabricksSparkExecutor(**kwargs)
        return ex, fake_builder, fake_spark

    return _make


@pytest.fixture(scope="session")
def spark_tmpdir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("spark_wh")


@pytest.fixture(scope="session")
def spark_exec(spark_tmpdir: Path) -> DatabricksSparkExecutorType:
    if DatabricksSparkExecutor is None:
        pytest.skip(
            "pyspark/delta not installed; install fastflowtransform[spark] to run Spark tests"
        )
    return DatabricksSparkExecutor(
        master="local[*]",
        app_name="fft-it",
        warehouse_dir=str(spark_tmpdir),
        database="default",
    )


@pytest.fixture(scope="session")
def spark_exec_delta(spark_tmpdir):
    if DatabricksSparkExecutor is None:
        pytest.skip(
            "pyspark/delta not installed; install fastflowtransform[spark] to run Spark tests"
        )

    extra_conf = {
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "1",
    }

    return DatabricksSparkExecutor(
        master="local[*]",
        app_name="fft-it-delta",
        warehouse_dir=str(spark_tmpdir),
        database=os.getenv("FF_DBR_DATABASE", "default"),
        extra_conf=extra_conf,
        table_format="delta",  # executor will configure Delta & verify it
    )


# ---- utest ----
@pytest.fixture
def fake_registry(tmp_path, monkeypatch):
    node = SimpleNamespace(name="model_a", kind="sql", deps=["src1"])
    reg = SimpleNamespace(
        nodes={"model_a": node},
        sources={},
        get_project_dir=lambda: tmp_path,
    )
    monkeypatch.setattr(utest, "REGISTRY", reg)
    monkeypatch.setattr(utest, "relation_for", lambda name: f"public.{name}")
    return reg


@pytest.fixture
def duckdbutor():
    """
    Fake-Executor:
    - has .con
    - con.register(...)
    - con.execute(...)
    - con.table(...).df()
    """
    con = MagicMock()
    table_df = pd.DataFrame([{"id": 1}])
    con.table.return_value.df.return_value = table_df

    class DuckEx:
        def __init__(self, con):
            self.con = con

        def run_sql(self, node, jenv):
            return None

        def run_python(self, node):
            return None

    return DuckEx(con)


@pytest.fixture
def postgresutor():
    """
    Fake-Executor for Postgres in _read_result.
    """
    engine = MagicMock()

    class PgEx:
        def __init__(self, engine):
            self.engine = engine
            self.schema = "public"

    return PgEx(engine)


# ---- Examples ----
@pytest.fixture(scope="session")
def duckdb_engine_env(tmp_path_factory):
    """Basic env for DuckDB examples."""
    db_dir = tmp_path_factory.mktemp("duckdb")
    db_path = db_dir / "examples.duckdb"
    return {
        "FF_ENGINE": "duckdb",
        "FF_DUCKDB_PATH": str(db_path),
    }


@pytest.fixture(scope="session")
def postgres_engine_env():
    """Basic env for Postgres. Skipped if DSN is missing or DB not reachable."""
    dsn = os.environ.get(
        "FF_PG_DSN",
        "postgresql+psycopg://postgres:postgres@localhost:5432/ffdb",
    )
    schema = os.environ.get("FF_PG_SCHEMA", "public")

    # Optional: Connectivity-Check
    try:
        engine = sa.create_engine(dsn)
        with engine.connect() as conn:
            conn.execute(text("select 1"))
    except Exception as exc:
        pytest.skip(f"Postgres not reachable at DSN={dsn!r}: {exc}")

    return {
        "FF_ENGINE": "postgres",
        "FF_PG_DSN": dsn,
        "FF_PG_SCHEMA": schema,
    }


@pytest.fixture(scope="session")
def spark_engine_env(tmp_path_factory):
    """Basic env for Databricks-Spark-Executor. Skipped if JAVA_HOME is missing."""
    if not os.environ.get("JAVA_HOME"):
        pytest.skip("JAVA_HOME not set for Spark tests")

    warehouse = tmp_path_factory.mktemp("spark_warehouse")

    return {
        "FF_ENGINE": "databricks_spark",
        "FF_SPARK_MASTER": "local[*]",
        "FF_DBR_ENABLE_HIVE": "1",
        "FF_SPARK_WAREHOUSE_DIR": str(warehouse),
    }


@pytest.fixture(scope="session")
def bigquery_engine_env():
    """
    Basic env for BigQuery examples. Skips if required env vars are missing.
    """
    project = os.environ.get("FF_BQ_PROJECT")
    dataset = os.environ.get("FF_BQ_DATASET")
    location = os.environ.get("FF_BQ_LOCATION")

    if not (project and dataset and location):
        pytest.skip("FF_BQ_PROJECT/FF_BQ_DATASET/FF_BQ_LOCATION not set for BigQuery tests")

    env = {
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": os.environ.get("FF_ENGINE_VARIANT", "bigframes"),
        "FF_BQ_PROJECT": project,
        "FF_BQ_DATASET": dataset,
        "FF_BQ_LOCATION": location,
    }

    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds:
        env["GOOGLE_APPLICATION_CREDENTIALS"] = creds

    return env
