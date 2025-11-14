# tests/integration/examples/test_examples_matrix.py
from __future__ import annotations

import json
import os
from pathlib import Path
from subprocess import CalledProcessError, run

import pytest
from tests.integration.examples.config import EXAMPLES


def _run_cmd(cmd: list[str], cwd: Path, extra_env: dict[str, str] | None = None) -> None:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    proc = run(cmd, check=False, cwd=str(cwd), env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        raise CalledProcessError(proc.returncode, cmd, proc.stdout, proc.stderr)


ENGINE_MARKS = {
    "duckdb": pytest.mark.duckdb,
    "postgres": pytest.mark.postgres,
    "databricks_spark": pytest.mark.databricks_spark,
}

# build only the actually-supported (example, engine) combinations
EXAMPLE_ENGINE_PARAMS = [
    pytest.param(
        example,
        engine,
        id=f"{example.name}[{engine}]",
        marks=ENGINE_MARKS[engine],
    )
    for example in EXAMPLES
    for engine in example.env_by_engine
]

ENGINE_ENV_FIXTURE = {
    "duckdb": "duckdb_engine_env",
    "postgres": "postgres_engine_env",
    "databricks_spark": "spark_engine_env",
}


@pytest.mark.integration
@pytest.mark.example
@pytest.mark.parametrize("example,engine", EXAMPLE_ENGINE_PARAMS)
def test_examples_with_all_engines(example, engine, request):
    fixture_name = ENGINE_ENV_FIXTURE[engine]
    engine_env: dict[str, str] = request.getfixturevalue(fixture_name)

    env = dict(engine_env)
    env["FFT_ACTIVE_ENV"] = example.env_by_engine[engine]

    cmd = ["make", example.make_target, f"ENGINE={engine}"]

    if engine == "databricks_spark":
        formats = example.spark_table_formats or ["parquet"]
        for fmt in formats:
            env_fmt = dict(env)
            env_fmt["DBR_TABLE_FORMAT"] = fmt
            _run_cmd(
                [*cmd, f"DBR_TABLE_FORMAT={fmt}"],
                cwd=example.path,
                extra_env=env_fmt,
            )
    else:
        _run_cmd(cmd, cwd=example.path, extra_env=env)

    target_dir = example.path / ".fastflowtransform" / "target"
    manifest = target_dir / "manifest.json"
    run_results = target_dir / "run_results.json"

    assert manifest.exists(), f"{example.name} ({engine}): manifest.json missing"
    assert run_results.exists(), f"{example.name} ({engine}): run_results.json missing"

    data = json.loads(run_results.read_text(encoding="utf-8"))
    results = data.get("results") or []

    assert results, f"{example.name} ({engine}): no results in run_results.json"

    all_status = {r.get("status") for r in results}
    assert all_status != {"error"}, (
        f"{example.name} ({engine}): all models failed according to run_results.json"
    )
