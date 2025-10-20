import pytest

from tests.common.utils import run


@pytest.mark.postgres
def test_reconcile_postgres_smoke(pg_project, pg_env, pg_seeded):
    # Same as DuckDB; relies on the example PG project mirroring objects
    res = run(["fft", "test", str(pg_project), "--env", "stg", "--select", "reconcile"], pg_env)
    assert "Totals" in res.stdout
