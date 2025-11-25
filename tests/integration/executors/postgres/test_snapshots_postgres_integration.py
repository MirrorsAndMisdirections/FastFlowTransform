# tests/unit/test_snapshots_postgres.py
from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import text
from tests.common.snapshot_helpers import (
    make_check_snapshot_node,
    make_timestamp_snapshot_node,
    scenario_check_strategy_detects_changes,
    scenario_snapshot_prune_keep_last,
    scenario_timestamp_first_and_second_run,
)

from fastflowtransform.core import relation_for
from fastflowtransform.executors.postgres import PostgresExecutor

# Postgres-friendly SQL (same as DuckDB in practice)
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


def _reset_snapshot_table(executor: PostgresExecutor, node_name: str) -> None:
    """
    Drop the snapshot table for this node so each test starts from a clean state.
    """
    rel = relation_for(node_name)  # e.g. "users_snapshot"
    qrel = executor._qualified(rel)  # schema-qualified name

    with executor.engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {qrel} CASCADE"))


def _read_pg(ex: PostgresExecutor, relation: str) -> pd.DataFrame:
    qualified = ex._qualified(relation)
    with ex.engine.begin() as conn:
        ex._set_search_path(conn)
        return pd.read_sql_query(text(f"select * from {qualified} order by 1, 2"), conn)


@pytest.mark.postgres
@pytest.mark.integration
def test_postgres_snapshot_timestamp_first_and_second_run(jinja_env, postgres_exec):
    node = make_timestamp_snapshot_node()
    _reset_snapshot_table(executor=postgres_exec, node_name=node.name)

    scenario_timestamp_first_and_second_run(
        executor=postgres_exec,
        node=node,
        jinja_env=jinja_env,
        read_fn=_read_pg,
        sql_first=SQL_TS_FIRST,
        sql_second=SQL_TS_SECOND,
    )


@pytest.mark.postgres
@pytest.mark.integration
def test_postgres_snapshot_prune_keep_last(jinja_env, postgres_exec):
    node = make_timestamp_snapshot_node()
    _reset_snapshot_table(executor=postgres_exec, node_name=node.name)

    scenario_snapshot_prune_keep_last(
        executor=postgres_exec,
        node=node,
        jinja_env=jinja_env,
        read_fn=_read_pg,
        sql_first=SQL_TS_FIRST,
        sql_second=SQL_TS_SECOND,
        unique_key=["id"],
    )


@pytest.mark.postgres
@pytest.mark.integration
def test_postgres_snapshot_check_strategy(jinja_env, postgres_exec):
    node = make_check_snapshot_node()
    _reset_snapshot_table(executor=postgres_exec, node_name=node.name)

    scenario_check_strategy_detects_changes(
        executor=postgres_exec,
        node=node,
        jinja_env=jinja_env,
        read_fn=_read_pg,
        sql_first=SQL_CHECK_FIRST,
        sql_second=SQL_CHECK_SECOND,
    )
