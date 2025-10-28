from types import SimpleNamespace

from fastflowtransform.incremental import run_or_dispatch


class FakeEx:
    def __init__(self):
        self.sql = None
        self.ctas = 0
        self.ins = 0
        self.mrg = 0
        self.exists = False

    def exists_relation(self, r):
        return self.exists

    def render_sql(self, node, env, ref_resolver=None, source_resolver=None):
        return "select 1 as id"

    def _resolve_ref(self, nm, env):
        return nm

    def _resolve_source(self, *a, **k):
        return "src"

    def create_table_as(self, r, sql):
        self.ctas += 1
        self.sql = sql

    def incremental_insert(self, r, sql):
        self.ins += 1
        self.sql = sql

    def incremental_merge(self, r, sql, keys):
        self.mrg += 1
        self.sql = sql

    def run_sql(self, node, env):
        self.sql = "plain"


def test_incremental_cold_runs_ctas(jinja_env):
    node = SimpleNamespace(name="m", kind="sql", meta={"materialized": "incremental"})
    ex = FakeEx()
    run_or_dispatch(ex, node, jinja_env)
    assert ex.ctas == 1 and ex.ins == 0 and ex.mrg == 0


def test_incremental_warm_insert_without_key(jinja_env):
    node = SimpleNamespace(name="m", kind="sql", meta={"materialized": "incremental"})
    ex = FakeEx()
    ex.exists = True
    run_or_dispatch(ex, node, jinja_env)
    assert ex.ins == 1 and ex.mrg == 0


def test_incremental_warm_merge_with_key(jinja_env):
    node = SimpleNamespace(
        name="m", kind="sql", meta={"materialized": "incremental", "unique_key": "id"}
    )
    ex = FakeEx()
    ex.exists = True
    run_or_dispatch(ex, node, jinja_env)
    assert ex.mrg == 1
