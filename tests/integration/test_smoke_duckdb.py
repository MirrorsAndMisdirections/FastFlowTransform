import os
import shutil
import subprocess

import duckdb
import pytest

from tests.common.utils import ROOT, run

PROJECT = ROOT / "examples" / "simple_duckdb"
DOCS = PROJECT / "site" / "dag"
PROJECT_LOCAL = PROJECT / ".local"
DB = PROJECT_LOCAL / "demo.duckdb"

ENV = os.environ.copy()
ENV.setdefault("FF_ENGINE", "duckdb")
ENV.setdefault("FF_DUCKDB_PATH", str(DB))  # erzwinge konsistenten DB-Ort


def setup_module(module):
    # clean previous artifacts im Projektkontext
    if DOCS.exists():
        shutil.rmtree(DOCS)
    if DB.exists():
        DB.unlink()
    PROJECT_LOCAL.mkdir(exist_ok=True, parents=True)

    # Seed via Make (im Projektordner)
    try:
        subprocess.run(
            ["make", "-C", str(PROJECT), "seed"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=ENV,
        )
    except subprocess.CalledProcessError as e:
        pytest.skip(f"Seed fehlgeschlagen (DuckDB CLI fehlt?) - skippe Smoke-Tests.\n{e.stdout}")


@pytest.mark.duckdb
@pytest.mark.cli
@pytest.mark.slow
def test_run_builds_tables(duckdb_seeded, duckdb_project, duckdb_env):
    run(["flowforge", "run", str(duckdb_project), "--env", "dev"], duckdb_env)


@pytest.mark.duckdb
@pytest.mark.cli
@pytest.mark.slow
def test_batch_tests_green(duckdb_seeded, duckdb_project, duckdb_env):
    run(["flowforge", "test", str(duckdb_project), "--env", "dev", "--select", "batch"], duckdb_env)


@pytest.mark.duckdb
@pytest.mark.cli
@pytest.mark.slow
def test_html_dag_generated():
    run(["flowforge", "dag", str(PROJECT), "--env", "dev", "--html"])
    assert (DOCS / "index.html").exists(), "index.html was not created"


@pytest.mark.duckdb
@pytest.mark.cli
@pytest.mark.slow
def test_duckdb_end_to_end_with_multi_deps(
    duckdb_seeded, duckdb_project, duckdb_env, duckdb_db_path
):
    run(["flowforge", "run", str(duckdb_project), "--env", "dev"], duckdb_env)
    con = duckdb.connect(str(DB))
    # users (seed) -> users_enriched (python, 1 dep)
    users_rows = con.execute("select count(*) from users").fetchone()
    assert users_rows is not None
    assert users_rows[0] >= 1

    users_enriched_rows = con.execute("select count(*) from users_enriched").fetchone()
    assert users_enriched_rows is not None
    assert users_enriched_rows[0] >= 1

    cols_users_enriched = [r[0] for r in con.execute("describe users_enriched").fetchall()]
    assert {"id", "email", "is_gmail"}.issubset(set(cols_users_enriched))

    # orders (seed)
    orders_rows = con.execute("select count(*) from orders").fetchone()
    assert orders_rows is not None
    assert orders_rows[0] >= 1

    # mart_orders_enriched (python, >1 deps via dict[str,df])
    mart_orders_enriched_rows = con.execute("select count(*) from mart_orders_enriched").fetchone()
    assert mart_orders_enriched_rows is not None
    assert mart_orders_enriched_rows[0] >= 1

    cols_moe = [r[0] for r in con.execute("describe mart_orders_enriched").fetchall()]
    assert {"order_id", "user_id", "email", "is_gmail", "amount", "valid_amt"}.issubset(
        set(cols_moe)
    )

    # einfache Qualitätschecks auf dem Multi-Dep-Ergebnis
    mart_orders_enriched_neg_rows = con.execute(
        "select count(*) from mart_orders_enriched where amount < 0"
    ).fetchone()
    assert mart_orders_enriched_neg_rows is not None
    assert mart_orders_enriched_neg_rows[0] == 0
