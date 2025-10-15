# tests/postgres/test_ref_source_postgres.py
from sqlalchemy import create_engine, text

import pytest

from tests.common.utils import run


@pytest.mark.postgres
def test_ref_and_source_pg(pg_seeded, pg_project, pg_env):
    # gleiche ENV wie beim Seeding → gleiches DSN/Schema
    run(["flowforge", "run", str(pg_project), "--env", "stg"], pg_env)

    eng = create_engine(pg_env["FF_PG_DSN"], future=True)
    schema = pg_env.get("FF_PG_SCHEMA", "public")
    with eng.begin() as c:
        c.execute(text(f'set local search_path = "{schema}"'))
        orders_count = c.execute(text("select count(*) from orders")).scalar()
        assert orders_count and orders_count >= 1

        mart_users_count = c.execute(text("select count(*) from mart_users")).scalar()
        assert mart_users_count and mart_users_count >= 1
