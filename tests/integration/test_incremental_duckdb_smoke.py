from tests.common.utils import run


def test_incremental_smoke_duckdb(duckdb_project, duckdb_env):
    # Expects model with materialized='incremental'
    res = run(["fft", "run", str(duckdb_project), "--env", "dev"], duckdb_env)
    assert res.returncode == 0
