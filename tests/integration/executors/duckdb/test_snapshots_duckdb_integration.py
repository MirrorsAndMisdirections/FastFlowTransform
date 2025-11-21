# tests/integration/executors/test_snapshots_duckdb_integration.py
from __future__ import annotations

import pandas as pd
import pytest
from tests.common.snapshot_helpers import (
    make_check_snapshot_node,
    make_timestamp_snapshot_node,
    scenario_check_strategy_detects_changes,
    scenario_snapshot_prune_keep_last,
    scenario_timestamp_first_and_second_run,
)

from fastflowtransform.executors.duckdb import DuckExecutor

# Common-ish SQL that DuckDB is happy with
SQL_TS_FIRST = """
select
  1 as id,
  'a' as value,
  timestamp '2024-01-01 00:00:00' as updated_at
"""

SQL_TS_SECOND = """
select
  1 as id,
  'b' as value,
  timestamp '2024-02-01 00:00:00' as updated_at
"""

SQL_CHECK_FIRST = """
select
  1 as id,
  'alpha' as value,
  timestamp '2024-01-01 00:00:00' as updated_at
"""

SQL_CHECK_SECOND = """
select
  1 as id,
  'beta' as value,
  timestamp '2024-01-01 00:00:00' as updated_at
"""


def _read_duckdb(ex: DuckExecutor, relation: str) -> pd.DataFrame:
    # Order by id, valid_from for deterministic tests
    return ex.con.execute(f'SELECT * FROM "{relation}" ORDER BY 1, 2').df()


@pytest.mark.duckdb
@pytest.mark.integration
def test_duckdb_snapshot_timestamp_first_and_second_run(jinja_env):
    ex = DuckExecutor(db_path=":memory:")
    node = make_timestamp_snapshot_node()

    scenario_timestamp_first_and_second_run(
        executor=ex,
        node=node,
        jinja_env=jinja_env,
        read_fn=_read_duckdb,
        sql_first=SQL_TS_FIRST,
        sql_second=SQL_TS_SECOND,
    )


@pytest.mark.duckdb
@pytest.mark.integration
def test_duckdb_snapshot_prune_keep_last(jinja_env):
    ex = DuckExecutor(db_path=":memory:")
    node = make_timestamp_snapshot_node()

    scenario_snapshot_prune_keep_last(
        executor=ex,
        node=node,
        jinja_env=jinja_env,
        read_fn=_read_duckdb,
        sql_first=SQL_TS_FIRST,
        sql_second=SQL_TS_SECOND,
        unique_key=["id"],
    )


@pytest.mark.duckdb
@pytest.mark.integration
def test_duckdb_snapshot_check_strategy(jinja_env):
    ex = DuckExecutor(db_path=":memory:")
    node = make_check_snapshot_node()

    scenario_check_strategy_detects_changes(
        executor=ex,
        node=node,
        jinja_env=jinja_env,
        read_fn=_read_duckdb,
        sql_first=SQL_CHECK_FIRST,
        sql_second=SQL_CHECK_SECOND,
    )
