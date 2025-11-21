# tests/common/fixtures.py
from __future__ import annotations

import os
import types
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import sqlalchemy as sa
from dotenv import load_dotenv
from jinja2 import DictLoader, Environment, FileSystemLoader, select_autoescape
from sqlalchemy import text

import fastflowtransform.executors.bigquery.base as bq_base
import fastflowtransform.executors.bigquery.pandas as bq_pandas
import fastflowtransform.typing as fft_typing
from fastflowtransform.executors.bigquery.pandas import BigQueryExecutor
from tests.common.mock.bigquery import install_fake_bigquery
from tests.common.mock.snowflake_snowpark import FakeSnowflakeSession

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
from tests.common.utils import ROOT

try:  # Optional: Spark deps may not be installed in core runs
    from fastflowtransform.executors.databricks_spark import DatabricksSparkExecutor
except ModuleNotFoundError:  # pragma: no cover - import guard
    DatabricksSparkExecutor = None  # type: ignore


try:  # Optional: Spark deps may not be installed in core runs
    from fastflowtransform.executors.databricks_spark import DatabricksSparkExecutor
except ModuleNotFoundError:  # pragma: no cover - import guard
    DatabricksSparkExecutor = None  # type: ignore

# --- Snowflake ----------------------------------------------------
try:
    from fastflowtransform.executors.snowflake_snowpark import (
        SnowflakeSnowparkExecutor,
        _SFCursorShim,
    )
except ModuleNotFoundError:  # pragma: no cover
    SnowflakeSnowparkExecutor = None  # type: ignore[assignment]
    _SFCursorShim = None  # type: ignore[assignment]


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


# ---- Postgres ----
@pytest.fixture(scope="session")
def pg_project():
    return ROOT / "examples" / "postgres"


@pytest.fixture(scope="session")
def pg_env():
    dsn = os.environ.get("FF_PG_DSN", "postgresql+psycopg://postgres:postgres@localhost:5432/ffdb")
    schema = os.environ.get("FF_PG_SCHEMA", "public")
    return {"FF_ENGINE": "postgres", "FF_PG_DSN": dsn, "FF_PG_SCHEMA": schema}


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


# ---- Snowflake Snowpark ----
@pytest.fixture(scope="session")
def snowflake_engine_env():
    """
    Basic env for Snowflake Snowpark examples / integration tests.

    Skips if required env vars are missing, so the test suite works without
    a Snowflake account configured.
    """
    account = os.environ.get("FF_SF_ACCOUNT")
    user = os.environ.get("FF_SF_USER")
    password = os.environ.get("FF_SF_PASSWORD")
    warehouse = os.environ.get("FF_SF_WAREHOUSE")
    database = os.environ.get("FF_SF_DATABASE")
    schema = os.environ.get("FF_SF_SCHEMA")
    role = os.environ.get("FF_SF_ROLE")
    allow_create_schema = os.environ.get("FF_SF_ALLOW_CREATE_SCHEMA", "1")

    # If any core bits are missing, skip Snowflake tests entirely
    required = [account, user, password, warehouse, database, schema]
    if not all(required):
        pytest.skip(
            "Snowflake env not configured for tests "
            "(need FF_SF_ACCOUNT, FF_SF_USER, FF_SF_PASSWORD, "
            "FF_SF_WAREHOUSE, FF_SF_DATABASE, FF_SF_SCHEMA)."
        )

    env = {
        "FF_ENGINE": "snowflake_snowpark",
        "FF_SF_ACCOUNT": account,
        "FF_SF_USER": user,
        "FF_SF_PASSWORD": password,
        "FF_SF_WAREHOUSE": warehouse,
        "FF_SF_DATABASE": database,
        "FF_SF_SCHEMA": schema,
        "FF_SF_ALLOW_CREATE_SCHEMA": allow_create_schema,
    }
    if role:
        env["FF_SF_ROLE"] = role
    return env


@pytest.fixture(scope="session")
def snowflake_cfg(snowflake_engine_env):
    """
    Canonical config dict for SnowflakeSnowparkExecutor, derived from env.
    """
    return {
        "account": snowflake_engine_env["FF_SF_ACCOUNT"],
        "user": snowflake_engine_env["FF_SF_USER"],
        "password": snowflake_engine_env["FF_SF_PASSWORD"],
        "warehouse": snowflake_engine_env["FF_SF_WAREHOUSE"],
        "database": snowflake_engine_env["FF_SF_DATABASE"],
        "schema": snowflake_engine_env["FF_SF_SCHEMA"],
        "role": snowflake_engine_env.get("FF_SF_ROLE"),
        "allow_create_schema": bool(
            int(snowflake_engine_env.get("FF_SF_ALLOW_CREATE_SCHEMA", "1"))
        ),
    }


@pytest.fixture
def snowflake_executor_fake() -> Any:
    """
    Fake SnowflakeSnowparkExecutor using an in-memory FakeSnowflakeSession.
    This does NOT talk to a real Snowflake account and does not need env vars.
    It is intended for SQL-shape tests, similar to the BigQuery fake.
    """
    if SnowflakeSnowparkExecutor is None:
        pytest.skip("SnowflakeSnowparkExecutor not importable")

    # Build instance without running its __init__ (which would try to connect).
    ex: Any = SnowflakeSnowparkExecutor.__new__(SnowflakeSnowparkExecutor)

    # Minimal attributes that the executor expects.
    ex.database = "FF_TEST_DB"
    ex.schema = "FF_TEST_SCHEMA"
    ex.allow_create_schema = False

    # Fake Snowflake session and cursor shim.
    session = FakeSnowflakeSession()
    ex.session = session
    if _SFCursorShim is not None:
        ex.con = _SFCursorShim(session)  # type: ignore[arg-type]
    else:
        # Cheap fallback if for some reason the shim isn't available
        ex.con = types.SimpleNamespace(execute=lambda sql, params=None: None)

    return ex


@pytest.fixture(scope="session")
def jinja_env_bigquery() -> Environment:
    """
    Very small Jinja environment for BigQuery unit tests.
    (You can also reuse your global jinja_env from tests/common/fixtures.py
     and just import that instead.)
    """
    env = Environment(
        loader=DictLoader({}),
        autoescape=select_autoescape([]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    # same globals your main jinja_env normally has
    env.globals.setdefault("is_incremental", lambda: False)
    env.globals.setdefault("this", None)
    return env


@pytest.fixture
def bq_executor_fake(monkeypatch) -> BigQueryExecutor:
    """
    BigQueryExecutor wired against the FakeClient from tests/common/mock/bigquery.py.
    No real BigQuery project / dataset / credentials required.
    """
    # Make sure all FFT modules that cache a `bigquery` symbol
    # see the fake module.
    fake_bq = install_fake_bigquery(
        monkeypatch,
        target_modules=[fft_typing, bq_base, bq_pandas],
    )

    # Instantiate FakeClient via the fake module so the types line up
    client = fake_bq.Client(project="ff_test_project", location="EU")  # type: ignore[attr-defined]

    # Optionally pretend the dataset exists (for _ensure_dataset tests)
    client.add_dataset("ff_test_project.ff_snapshots")  # type: ignore[attr-defined]

    ex = BigQueryExecutor(
        project="ff_test_project",
        dataset="ff_snapshots",
        location="EU",
        client=client,
        allow_create_dataset=True,
    )
    return ex
