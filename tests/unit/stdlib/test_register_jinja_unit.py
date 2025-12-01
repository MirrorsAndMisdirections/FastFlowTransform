import pytest
from jinja2 import Environment

from fastflowtransform.stdlib import register_jinja
from fastflowtransform.stdlib.dates import sql_date_trunc
from fastflowtransform.stdlib.engine import normalize_engine


@pytest.mark.unit
def test_register_jinja_registers_all_helpers():
    env = Environment()
    # Use a fixed engine key here
    register_jinja(env, engine="duckdb")

    # engine-aware helpers
    for name in [
        "ff_date_trunc",
        "ff_date_add",
        "ff_safe_cast",
        "ff_partition_filter",
        "ff_partition_in",
    ]:
        assert name in env.globals

    # raw helpers (match actual exported names)
    for name in ["ff_engine", "engine_family", "ff_is_engine"]:
        assert name in env.globals


@pytest.mark.unit
def test_register_jinja_binds_engine_for_date_trunc():
    env = Environment()
    register_jinja(env, engine="duckdb")

    fn = env.globals["ff_date_trunc"]
    # this should behave like sql_date_trunc(..., engine='duckdb')
    out = fn("order_ts", "day")
    expected = sql_date_trunc("order_ts", "day", engine="duckdb")
    assert out == expected


@pytest.mark.unit
def test_register_jinja_with_engine_resolver():
    env = Environment()

    def _resolver():
        return "bigquery"

    register_jinja(env, engine_resolver=_resolver)

    fn = env.globals["ff_date_trunc"]
    out = fn("order_ts", "day")
    # Should use bigquery semantics internally
    assert "DATE_TRUNC(" in out
    assert "CAST(order_ts AS TIMESTAMP)" in out

    # ff_engine helper should reflect the normalized key
    ff_engine = env.globals["ff_engine"]
    assert ff_engine("bigquery") == normalize_engine("bigquery")
