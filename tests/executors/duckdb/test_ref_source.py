# tests/duckdb/test_ref_source.py
from pathlib import Path

import pytest

from tests.common.utils import run, ROOT

PROJ = ROOT / "examples" / "simple_duckdb"
DB = PROJ / ".local" / "demo.duckdb"
ENV = {"FF_ENGINE": "duckdb", "FF_DUCKDB_PATH": str(DB)}


@pytest.mark.duckdb
def test_ref_and_source_duckdb(duckdb_seeded, duckdb_project, duckdb_env):
    # Seeds sind durch duckdb_seeded geladen
    run(["flowforge", "run", str(duckdb_project), "--env", "dev"], duckdb_env)

    # minimale Verifikation
    import duckdb

    con = duckdb.connect(duckdb_env["FF_DUCKDB_PATH"])
    orders_count = con.execute("select count(*) from orders").fetchone()
    assert orders_count is not None
    assert orders_count[0] >= 1  # source()

    mart_users_count = con.execute("select count(*) from mart_users").fetchone()
    assert mart_users_count is not None
    assert mart_users_count[0] >= 1  # ref() -> users_enriched
