# tests/unit/test_fingerprint.py
from __future__ import annotations

from pathlib import Path

from fastflowtransform.core import Node
from fastflowtransform.fingerprint import (
    EnvCtx,
    build_env_ctx,
    fingerprint_py,
    fingerprint_sql,
    get_function_source,
    inspect,
    normalized_sources_blob,
)


def test_sources_normalization_stable():
    a = {"crm": {"users": {"identifier": "seed_users"}, "orders": {"identifier": "seed_orders"}}}
    b = {"crm": {"orders": {"identifier": "seed_orders"}, "users": {"identifier": "seed_users"}}}
    assert normalized_sources_blob(a) == normalized_sources_blob(b)


def test_env_ctx_respects_selected_env_keys(monkeypatch):
    monkeypatch.setenv("FF_ENGINE", "duckdb")
    monkeypatch.setenv("SECRET_TOKEN", "shh")
    ctx1 = build_env_ctx(engine="duckdb", profile_name="dev", relevant_env_keys=["FF_ENGINE"])
    ctx2 = build_env_ctx(engine="duckdb", profile_name="dev", relevant_env_keys=["FF_ENGINE"])
    assert ctx1.to_payload() == ctx2.to_payload()

    # Changing an excluded env var should not alter ctx payload
    monkeypatch.setenv("SECRET_TOKEN", "changed")
    ctx3 = build_env_ctx(engine="duckdb", profile_name="dev", relevant_env_keys=["FF_ENGINE"])
    assert ctx1.to_payload() == ctx3.to_payload()


def test_fingerprint_sql_changes_on_small_sql_edit():
    node = Node(name="users.ff", kind="sql", path=Path(__file__))
    ctx = EnvCtx(engine="duckdb", profile="dev", env_vars={}, sources_json="{}")
    fp1 = fingerprint_sql(node=node, rendered_sql="select 1 as x", env_ctx=ctx, dep_fps={})
    fp2 = fingerprint_sql(node=node, rendered_sql="select 2 as x", env_ctx=ctx, dep_fps={})
    assert fp1 != fp2


def test_fingerprint_sql_dep_cascade():
    node = Node(name="mart.ff", kind="sql", path=Path(__file__), deps=["users.ff"])
    ctx = EnvCtx(engine="duckdb", profile="dev", env_vars={}, sources_json="{}")
    fp_dep_a = "aaa"
    fp_dep_b = "bbb"
    fp1 = fingerprint_sql(
        node=node, rendered_sql="select * from users", env_ctx=ctx, dep_fps={"users.ff": fp_dep_a}
    )
    fp2 = fingerprint_sql(
        node=node, rendered_sql="select * from users", env_ctx=ctx, dep_fps={"users.ff": fp_dep_b}
    )
    assert fp1 != fp2


def _dummy_func_a(x):
    return x + 1


def _dummy_func_b(x):
    return x + 2


def test_get_function_source_is_stable_and_different_per_change():
    src_a = get_function_source(_dummy_func_a)
    src_b = get_function_source(_dummy_func_b)
    assert isinstance(src_a, str) and isinstance(src_b, str)
    assert src_a != src_b


def test_fingerprint_py_changes_with_source_and_deps():
    node = Node(name="py_model", kind="python", path=Path(__file__), deps=["users.ff"])
    ctx = EnvCtx(engine="duckdb", profile="dev", env_vars={}, sources_json="{}")
    src = get_function_source(_dummy_func_a)
    fp1 = fingerprint_py(node=node, func_src=src, env_ctx=ctx, dep_fps={"users.ff": "x"})
    fp2 = fingerprint_py(node=node, func_src=src, env_ctx=ctx, dep_fps={"users.ff": "y"})
    assert fp1 != fp2


def test_get_function_source_fallback(monkeypatch):
    # Force inspect.getsource to fail to exercise fallback path
    def boom(_):
        raise OSError("no source")

    monkeypatch.setattr(inspect, "getsource", boom, raising=True)

    src = get_function_source(_dummy_func_a)
    assert isinstance(src, str) and len(src) > 0
