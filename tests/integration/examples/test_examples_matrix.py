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


ENGINE_PARAMS = [
    pytest.param("duckdb", marks=pytest.mark.duckdb, id="duckdb"),
    pytest.param("postgres", marks=pytest.mark.postgres, id="postgres"),
    pytest.param("databricks_spark", marks=pytest.mark.databricks_spark, id="databricks_spark"),
]

ENGINE_ENV_FIXTURE = {
    "duckdb": "duckdb_engine_env",
    "postgres": "postgres_engine_env",
    "databricks_spark": "spark_engine_env",
}


@pytest.mark.integration
@pytest.mark.parametrize("engine", ENGINE_PARAMS)
@pytest.mark.parametrize("example", EXAMPLES, ids=lambda e: e.name)
def test_examples_with_all_engines(example, engine, request):
    if engine not in example.env_by_engine:
        pytest.skip(f"{example.name} does not support engine={engine}")

    fixture_name = ENGINE_ENV_FIXTURE[engine]
    engine_env: dict[str, str] = request.getfixturevalue(fixture_name)

    env = dict(engine_env)
    env["FFT_ACTIVE_ENV"] = example.env_by_engine[engine]

    cmd = ["make", example.make_target, f"ENGINE={engine}"]
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
