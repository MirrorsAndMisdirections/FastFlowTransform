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


EXAMPLES: list[ExampleConfig] = [
    ExampleConfig(
        name="basic_demo",
        path=ROOT / "examples" / "basic_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks",
        },
    ),
    ExampleConfig(
        name="api_demo",
        path=ROOT / "examples" / "api_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks",
        },
    ),
    ExampleConfig(
        name="incremental_demo",
        path=ROOT / "examples" / "incremental_demo",
        make_target="demo",
        env_by_engine={
            "duckdb": "dev_duckdb",
            "postgres": "dev_postgres",
            "databricks_spark": "dev_databricks",
        },
    ),
]
