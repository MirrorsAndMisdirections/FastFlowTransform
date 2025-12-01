# tests/integration/examples/config.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from tests.common.utils import ROOT


@dataclass
class ExampleConfig:
    name: str
    path: Path
    make_target: str
    env_by_engine: dict[str, str]
    spark_table_formats: list[str] | None = None
    bigquery_env_by_backend: dict[str, str] | None = None


EXAMPLES: list[ExampleConfig] = [
    ExampleConfig(
        name="api_demo",
        path=ROOT / "examples" / "api_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks",
            "bigquery": "dev_bigquery_pandas",
            "snowflake_snowpark": "dev_snowflake",
        },
        bigquery_env_by_backend={
            "pandas": "dev_bigquery_pandas",
            "bigframes": "dev_bigquery_bigframes",
        },
    ),
    ExampleConfig(
        name="basic_demo",
        path=ROOT / "examples" / "basic_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks",
            "bigquery": "dev_bigquery_pandas",
            "snowflake_snowpark": "dev_snowflake",
        },
        bigquery_env_by_backend={
            "pandas": "dev_bigquery_pandas",
            "bigframes": "dev_bigquery_bigframes",
        },
    ),
    ExampleConfig(
        name="cache_demo",
        path=ROOT / "examples" / "cache_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks",
            "bigquery": "dev_bigquery_pandas",
            "snowflake_snowpark": "dev_snowflake",
        },
        bigquery_env_by_backend={
            "pandas": "dev_bigquery_pandas",
            "bigframes": "dev_bigquery_bigframes",
            "bigquery": "dev_bigquery_pandas",
        },
    ),
    ExampleConfig(
        name="ci_demo",
        path=ROOT / "examples" / "ci_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks",
            "bigquery": "dev_bigquery_pandas",
            "snowflake_snowpark": "dev_snowflake",
        },
        bigquery_env_by_backend={
            "pandas": "dev_bigquery_pandas",
            "bigframes": "dev_bigquery_bigframes",
        },
    ),
    ExampleConfig(
        name="dq_demo",
        path=ROOT / "examples" / "dq_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks",
            "bigquery": "dev_bigquery_pandas",
            "snowflake_snowpark": "dev_snowflake",
        },
        bigquery_env_by_backend={
            "pandas": "dev_bigquery_pandas",
            "bigframes": "dev_bigquery_bigframes",
        },
    ),
    ExampleConfig(
        name="hooks_demo",
        path=ROOT / "examples" / "hooks_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks",
            "bigquery": "dev_bigquery_pandas",
            "snowflake_snowpark": "dev_snowflake",
        },
        bigquery_env_by_backend={
            "pandas": "dev_bigquery_pandas",
            "bigframes": "dev_bigquery_bigframes",
        },
    ),
    ExampleConfig(
        name="incremental_demo",
        path=ROOT / "examples" / "incremental_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks_parquet",
            "bigquery": "dev_bigquery_pandas",
            "snowflake_snowpark": "dev_snowflake",
        },
        spark_table_formats=["parquet", "delta", "iceberg"],
        bigquery_env_by_backend={
            "pandas": "dev_bigquery_pandas",
            "bigframes": "dev_bigquery_bigframes",
        },
    ),
    ExampleConfig(
        name="macros_demo",
        path=ROOT / "examples" / "macros_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks",
            "bigquery": "dev_bigquery_pandas",
            "snowflake_snowpark": "dev_snowflake",
        },
        bigquery_env_by_backend={
            "pandas": "dev_bigquery_pandas",
            "bigframes": "dev_bigquery_bigframes",
        },
    ),
    ExampleConfig(
        name="materializations_demo",
        path=ROOT / "examples" / "materializations_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks",
            "bigquery": "dev_bigquery_pandas",
            "snowflake_snowpark": "dev_snowflake",
        },
        bigquery_env_by_backend={
            "pandas": "dev_bigquery_pandas",
            "bigframes": "dev_bigquery_bigframes",
        },
    ),
    # ExampleConfig(
    #     name="packages_demo",
    #     path=ROOT / "examples" / "packages_demo",
    #     make_target="demo",
    #     env_by_engine={
    #         "duckdb": "dev_duckdb",
    #     },
    # ),
    ExampleConfig(
        name="snapshot_demo",
        path=ROOT / "examples" / "snapshot_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks_parquet",
            "bigquery": "dev_bigquery_pandas",
            "snowflake_snowpark": "dev_snowflake",
        },
        spark_table_formats=["parquet", "delta", "iceberg"],
        bigquery_env_by_backend={
            "pandas": "dev_bigquery_pandas",
            "bigframes": "dev_bigquery_bigframes",
        },
    ),
]
