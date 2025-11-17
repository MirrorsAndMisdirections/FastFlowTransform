# tests/common/mock/profiles.py
from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from fastflowtransform.settings import (
    Profile,
)


def fake_bigquery_profile(
    *,
    project: str = "p1",
    dataset: str = "ds1",
    location: str | None = "EU",
    use_bigframes: bool = False,
    allow_create_dataset: bool = False,
) -> Profile:
    """
    Return a shape-compatible fake of a BigQuery profile.

    Only attributes that fastflowtransform.cli.bootstrap._validate_profile_params()
    and _make_executor() actually read are provided.
    """
    ns = SimpleNamespace(
        # top-level field that bootstrap branches on
        engine="bigquery",
        # nested bigquery section
        bigquery=SimpleNamespace(
            project=project,
            dataset=dataset,
            location=location,
            use_bigframes=use_bigframes,
            allow_create_dataset=allow_create_dataset,
        ),
    )
    # tell the type checker: "this is good enough to be treated as Profile"
    return cast(Profile, ns)


def fake_duckdb_profile(
    *,
    path: str = ":memory:",
    schema: str | None = None,
    catalog: str | None = None,
) -> Profile:
    """
    Fake DuckDB profile - just enough for _validate_profile_params and _make_executor.
    """
    ns = SimpleNamespace(
        engine="duckdb",
        duckdb=SimpleNamespace(
            path=path,
            db_schema=schema,
            catalog=catalog,
        ),
    )
    return cast(Profile, ns)


def fake_postgres_profile(
    *,
    dsn: str = "postgres://user:pass@localhost:5432/db",
    schema: str = "public",
) -> Profile:
    """
    Fake Postgres profile - fields match what bootstrap checks:
      - postgres.dsn
      - postgres.db_schema
    """
    ns = SimpleNamespace(
        engine="postgres",
        postgres=SimpleNamespace(
            dsn=dsn,
            db_schema=schema,
        ),
    )
    return cast(Profile, ns)


def fake_databricks_spark_profile(
    *,
    master: str = "local[*]",
    app_name: str = "ff-test",
    extra_conf: dict[str, str] | None = None,
    warehouse_dir: str | None = None,
    use_hive_metastore: bool = False,
    catalog: str | None = None,
    database: str | None = None,
    table_format: str | None = None,
    table_options: dict[str, str] | None = None,
) -> Profile:
    """
    Fake Databricks/Spark profile - mirrors the attribute names the real code expects.
    """
    ns = SimpleNamespace(
        engine="databricks_spark",
        databricks_spark=SimpleNamespace(
            master=master,
            app_name=app_name,
            extra_conf=extra_conf or {},
            warehouse_dir=warehouse_dir,
            use_hive_metastore=use_hive_metastore,
            catalog=catalog,
            database=database,
            table_format=table_format,
            table_options=table_options or {},
        ),
    )
    return cast(Profile, ns)


def fake_snowflake_snowpark_profile(
    *,
    account: str = "acc",
    user: str = "user",
    password: str = "pass",
    warehouse: str = "wh",
    database: str = "db",
    db_schema: str = "PUBLIC",
    role: str | None = None,
) -> Profile:
    """
    Fake Snowflake Snowpark profile - includes all required fields the bootstrap validates.
    """
    ns = SimpleNamespace(
        engine="snowflake_snowpark",
        snowflake_snowpark=SimpleNamespace(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            db_schema=db_schema,
            role=role,
        ),
    )
    return cast(Profile, ns)


def make_fake_profile(engine: str = "bigquery") -> Profile:
    """
    Convenience factory for tests that don't care about the exact engine.
    """
    if engine == "duckdb":
        return fake_duckdb_profile()
    if engine == "postgres":
        return fake_postgres_profile()
    if engine == "databricks_spark":
        return fake_databricks_spark_profile()
    if engine == "snowflake_snowpark":
        return fake_snowflake_snowpark_profile()
    # default → bigquery
    return fake_bigquery_profile()
