import os

import pytest
from sqlalchemy import create_engine, text

from tests.common.utils import ROOT, run

PROJECT = ROOT / "examples" / "postgres"
DOCS = PROJECT / "site" / "dag"

PG_DSN = os.environ.get(
    "FF_PG_DSN",
    "postgresql+psycopg://postgres:postgres@localhost:5432/ffdb",
)


@pytest.mark.postgres
@pytest.mark.cli
@pytest.mark.slow
def test_pg_html_dag_generated(pg_env):
    run(["fft", "dag", str(PROJECT), "--env", "stg", "--html"], pg_env)
    assert (DOCS / "index.html").exists(), "index.html was not created"


@pytest.mark.postgres
@pytest.mark.cli
@pytest.mark.slow
def test_pg_batch_tests_green(pg_env):
    run(["fft", "test", str(PROJECT), "--env", "stg", "--select", "batch"], pg_env)


@pytest.mark.postgres
@pytest.mark.slow
def test_pg_result_exists():
    engine = create_engine(PG_DSN, future=True)
    with engine.begin() as conn:
        n = conn.execute(text("select count(*) from mart_users")).scalar()
        assert n is not None and n >= 1


@pytest.mark.postgres
@pytest.mark.cli
@pytest.mark.slow
def test_pg_run_builds_tables(pg_project, pg_env):
    # 1) Load seeds (uses FF_ENGINE=postgres & DSN)
    run(["fft", "seed", str(pg_project), "--env", "stg"], pg_env)

    # 2) Run the pipeline
    run(["fft", "run", str(pg_project), "--env", "stg"], pg_env)


@pytest.mark.postgres
@pytest.mark.slow
def test_pg_multi_dep_model_exists(pg_env):
    engine = create_engine(pg_env["FF_PG_DSN"], future=True)
    schema = pg_env.get("FF_PG_SCHEMA", "public")
    with engine.begin() as conn:
        conn.execute(text(f'set local search_path = "{schema}"'))
        n = conn.execute(text("select count(*) from mart_orders_enriched")).scalar()
        assert n is not None and n >= 1


@pytest.mark.postgres
@pytest.mark.slow
def test_pg_multi_dep_model_columns(pg_env):
    engine = create_engine(PG_DSN, future=True)
    with engine.begin() as conn:
        cols = [
            r[0]
            for r in conn.execute(
                text("""
            select column_name from information_schema.columns
            where table_name = 'mart_orders_enriched'
        """)
            ).fetchall()
        ]
    for c in ["order_id", "user_id", "email", "is_gmail", "amount", "valid_amt"]:
        assert c in cols
