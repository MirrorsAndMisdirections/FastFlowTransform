# fastflowtransform/incremental.py
from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import suppress
from typing import Any

from sqlalchemy import text as _sa_text

from fastflowtransform.core import relation_for
from fastflowtransform.errors import ModelExecutionError


def _normalize_unique_key(val: Any) -> list[str]:
    if val is None:
        return []
    if isinstance(val, str):
        s = val.strip()
        return [s] if s else []
    if isinstance(val, (list, tuple)):
        out: list[str] = []
        for x in val:
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
        return out
    return []


def _get_on_schema_change(meta: dict | None) -> str:
    v = (meta or {}).get("on_schema_change") or "ignore"
    v = str(v).strip().lower()
    if v not in {"ignore", "append_new_columns", "sync_all_columns"}:
        return "ignore"
    return v


# ---------- Helper ----------


def _exec_sql(exe: Any, sql: str) -> None:
    """Best-effort SQL execution across engines (DuckDB/PG/Snowflake/BQ shims)."""
    if hasattr(exe, "con") and hasattr(exe.con, "execute"):  # DuckDB
        exe.con.execute(sql)
        return
    if hasattr(exe, "engine"):  # SQLAlchemy Engine
        with exe.engine.begin() as conn:
            conn.execute(_sa_text(sql))
        return
    if hasattr(exe, "execute"):  # BigQuery-like shim
        exe.execute(sql)
        return
    if hasattr(exe, "run_sql_raw"):
        exe.run_sql_raw(sql)
        return
    raise RuntimeError("No suitable raw-SQL execution path on executor")


def _safe_exists(executor: Any, relation: Any) -> bool:
    try:
        return bool(executor.exists_relation(relation))
    except Exception:
        return False


def _env_with_incremental(jenv: Any, is_incr: bool) -> Any:
    _overlay = getattr(jenv, "overlay", None)
    env = _overlay() if callable(_overlay) else None
    if env is None:

        class _EnvShim:
            def __init__(self, base):
                self._base = base
                self.globals = dict(getattr(base, "globals", {}))

            def __getattr__(self, name):
                return getattr(self._base, name)

        env = _EnvShim(jenv)
    getattr(env, "globals", {}).update({"is_incremental": lambda: is_incr})
    return env


def _render_sql_safe(executor: Any, node: Any, env: Any) -> str:
    try:
        return executor.render_sql(
            node,
            env,
            ref_resolver=lambda nm: executor._resolve_ref(nm, env),
            source_resolver=executor._resolve_source,
        )
    except Exception:
        return executor.render_sql(node, env)


def _wrap_and_raise_factory(node_name: str, relation: Any, rendered_sql: str | None) -> Callable:
    def _wrap_and_raise(e: Exception) -> None:
        tail = rendered_sql[-600:].strip() if rendered_sql else None
        msg = f"{e.__class__.__name__}: {e}"
        raise ModelExecutionError(node_name, relation, msg, sql_snippet=tail) from e

    return _wrap_and_raise


def _maybe_schema_sync(executor: Any, relation: Any, rendered_sql: str, policy: str) -> None:
    if policy in {"append_new_columns", "sync_all_columns"} and hasattr(
        executor, "alter_table_sync_schema"
    ):
        with suppress(Exception):
            executor.alter_table_sync_schema(relation, rendered_sql, mode=policy)


def _create_table_as_or_replace(executor: Any, relation: Any, rendered_sql: str) -> None:
    _full_refresh_table(executor, relation, rendered_sql)


def _full_refresh_table(executor: Any, relation: Any, rendered_sql: str) -> None:
    """
    Engine-agnostic full refresh:
    - If the executor has a `full_refresh_table(...)` method, it is used.
    - Otherwise: first try `create_table_as`; on failure, fall back to raw SQL
    'create or replace table ... as ...' (for DuckDB, Postgres, Snowflake, etc.).
    """
    full_refresh = getattr(executor, "full_refresh_table", None)
    if callable(full_refresh):
        full_refresh(relation, rendered_sql)
        return

    try:
        executor.create_table_as(relation, rendered_sql)
    except Exception:
        _exec_sql(executor, f"create or replace table {relation} as {rendered_sql}")


UniqueKey = str | Sequence[str] | None


def _merge_or_insert_with_fallback(
    executor: Any,
    relation: Any,
    rendered_sql: str,
    unique_key: UniqueKey,
    *,
    fallback_sql: str | None = None,
    on_full_refresh_error: Callable[[Exception], None] | None = None,
) -> None:
    fallback_sql = fallback_sql or rendered_sql

    def _run_full_refresh() -> None:
        try:
            _full_refresh_table(executor, relation, fallback_sql)
        except Exception as exc:
            if on_full_refresh_error is not None:
                on_full_refresh_error(exc)
            else:
                raise

    if unique_key:
        if isinstance(unique_key, str):
            keys: list[str] = [unique_key]
        else:
            keys = list(unique_key)

        try:
            executor.incremental_merge(relation, rendered_sql, keys)
            return
        except Exception:
            # Sauberer Full-Refresh über Engine-Hook
            _run_full_refresh()
            return

    # kein unique_key -> insert-only
    try:
        executor.incremental_insert(relation, rendered_sql)
    except Exception:
        _run_full_refresh()


# ---------- Run or dispatch ----------


def run_or_dispatch(executor: Any, node: Any, jenv: Any) -> None:
    """
    Incremental materialization for materialized='incremental'.
    Method called from BaseExecutor.run_sql(...).
    """
    meta = getattr(node, "meta", {}) or {}
    materialized = meta.get("materialized")
    if not materialized and meta.get("incremental"):
        materialized = "incremental"

    relation = relation_for(node.name)

    if materialized != "incremental":
        rendered_sql = _render_sql_safe(executor, node, jenv)
        wrap_and_raise = _wrap_and_raise_factory(node.name, relation, rendered_sql)
        try:
            _create_table_as_or_replace(executor, relation, rendered_sql)
        except Exception as e:
            wrap_and_raise(e)
        return

    exists = _safe_exists(executor, relation)
    env = _env_with_incremental(jenv, exists)

    base_sql = _render_sql_safe(executor, node, env)

    delta_sql = meta.get("delta_sql")
    if exists and isinstance(delta_sql, str) and delta_sql.strip():
        rendered_sql = delta_sql.strip()
    else:
        rendered_sql = base_sql

    fallback_sql = rendered_sql
    if exists:
        non_incr_env = _env_with_incremental(jenv, False)
        fallback_sql = _render_sql_safe(executor, node, non_incr_env)

    wrap_incremental = _wrap_and_raise_factory(node.name, relation, rendered_sql)
    wrap_full_refresh = _wrap_and_raise_factory(node.name, relation, fallback_sql)

    unique_key = _normalize_unique_key(meta.get("unique_key"))
    schema_policy = _get_on_schema_change(meta)

    if not exists:
        try:
            _create_table_as_or_replace(executor, relation, fallback_sql)
        except Exception as e:
            wrap_full_refresh(e)
        return

    _maybe_schema_sync(executor, relation, rendered_sql, schema_policy)
    try:
        _merge_or_insert_with_fallback(
            executor,
            relation,
            rendered_sql,
            unique_key,
            fallback_sql=fallback_sql,
            on_full_refresh_error=wrap_full_refresh,
        )
    except ModelExecutionError:
        raise
    except Exception as e:
        wrap_incremental(e)
