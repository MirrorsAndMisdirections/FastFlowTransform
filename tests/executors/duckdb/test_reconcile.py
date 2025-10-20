import pytest

from tests.common.utils import run


@pytest.mark.duckdb
def test_reconcile_duckdb_smoke(duckdb_project, duckdb_env, duckdb_seeded):
    # Run full test suite but select only reconcile-tagged checks
    env = dict(duckdb_env)
    res = run(["fft", "test", str(duckdb_project), "--env", "dev", "--select", "reconcile"], env)
    # Exit code 0 or 2 is handled in run(); we assert substring in stdout to ensure checks ran
    assert "orders_count_equals_mart" in res.stdout or "orders ⇔ mart_orders_enriched" in res.stdout
    assert "Totals" in res.stdout
