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
) -> None:
    if unique_key:
        # auf Liste normalisieren
        if isinstance(unique_key, str):
            keys: list[str] = [unique_key]
        else:
            keys = list(unique_key)  # Sequence[str] -> List[str]

        try:
            # merge mit Keys
            executor.incremental_merge(relation, rendered_sql, keys)
            return
        except Exception:
            _exec_sql(executor, f"create or replace table {relation} as {rendered_sql}")
            return

    # kein unique_key -> insert
    try:
        executor.incremental_insert(relation, rendered_sql)
    except Exception:
        _exec_sql(executor, f"create or replace table {relation} as {rendered_sql}")


# ---------- Run or dispatch ----------


def run_or_dispatch(executor: Any, node: Any, jenv: Any) -> None:
    """
    Wrapper für executor.run_sql(...):
    - fängt materialized='incremental' ab und plant nenewu/merge/insert.
    - sonst Delegation an executor.
    """
    meta = getattr(node, "meta", {}) or {}
    materialized = meta.get("materialized", "table")

    if materialized != "incremental":
        try:
            return executor.run_sql(node, jenv)
        except Exception as e:
            rel = relation_for(node.name)
            msg = f"{e.__class__.__name__}: {e}"
            raise ModelExecutionError(node.name, rel, msg, sql_snippet=None) from e

    relation = relation_for(node.name)
    exists = _safe_exists(executor, relation)
    env = _env_with_incremental(jenv, exists)
    rendered_sql = _render_sql_safe(executor, node, env)
    wrap_and_raise = _wrap_and_raise_factory(node.name, relation, rendered_sql)

    unique_key = _normalize_unique_key(meta.get("unique_key"))
    schema_policy = _get_on_schema_change(meta)

    if not exists:
        try:
            _create_table_as_or_replace(executor, relation, rendered_sql)
        except Exception as e:
            wrap_and_raise(e)
        return

    # exists -> inkrementeller Pfad
    _maybe_schema_sync(executor, relation, rendered_sql, schema_policy)
    try:
        _merge_or_insert_with_fallback(executor, relation, rendered_sql, unique_key)
    except Exception as e:
        wrap_and_raise(e)
