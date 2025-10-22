from __future__ import annotations

from typing import Any

from sqlalchemy import text as _sa_text

from .core import relation_for


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


def run_or_dispatch(executor: Any, node: Any, jenv: Any) -> None:
    """
    Wrapper for executor.run_sql(...):
    - catches materialized='incremental' and plans nenewu/merge/insert.
    - else delegate to executor.
    """
    meta = getattr(node, "meta", {}) or {}
    materialized = meta.get("materialized", "table")
    if materialized != "incremental":
        # normaler Pfad
        return executor.run_sql(node, jenv)

    relation = relation_for(node.name)

    # ------- kleine Helfer zum robusten Ausführen von rohem SQL -------
    def _exec_sql(sql: str) -> None:
        """Best-effort SQL execution across engines (DuckDB/PG/Snowflake/BQ shims)."""
        # DuckDB
        if hasattr(executor, "con") and hasattr(executor.con, "execute"):
            executor.con.execute(sql)
            return
        # SQLAlchemy Engine (Postgres, Snowflake Snowpark shim)
        if hasattr(executor, "engine"):
            with executor.engine.begin() as conn:
                conn.execute(_sa_text(sql))
            return
        # BigQuery shim(s)
        if hasattr(executor, "execute"):
            executor.execute(sql)
            return
        if hasattr(executor, "run_sql_raw"):
            executor.run_sql_raw(sql)
            return
        raise RuntimeError("No suitable raw-SQL execution path on executor")

    exists = False
    try:
        exists = bool(executor.exists_relation(relation))
    except Exception:
        exists = False

    def _is_incr() -> bool:
        return exists

    _overlay = getattr(jenv, "overlay", None)
    loc_env = _overlay() if callable(_overlay) else None
    if loc_env is None:
        # Fallback: Shim with own globals dict
        class _EnvShim:
            def __init__(self, base):
                self._base = base
                self.globals = dict(getattr(base, "globals", {}))

            def __getattr__(self, name):
                return getattr(self._base, name)

        loc_env = _EnvShim(jenv)
    # 'this' = physisal relation
    getattr(loc_env, "globals", {}).update({"is_incremental": _is_incr, "this": relation})

    try:
        rendered_sql = executor.render_sql(
            node,
            loc_env,
            ref_resolver=lambda nm: executor._resolve_ref(nm, loc_env),
            source_resolver=executor._resolve_source,
        )
    except Exception:
        # Fallback
        rendered_sql = executor.render_sql(node, loc_env)

    unique_key = _normalize_unique_key(meta.get("unique_key"))
    schema_policy = _get_on_schema_change(meta)

    if not exists:
        # Cold start → CREATE TABLE AS SELECT
        try:
            executor.create_table_as(relation, rendered_sql)
        except Exception:
            # Fallback: raw SQL
            _exec_sql(f"create or replace table {relation} as {rendered_sql}")
    else:
        try:
            if schema_policy in {"append_new_columns", "sync_all_columns"} and hasattr(
                executor, "alter_table_sync_schema"
            ):
                executor.alter_table_sync_schema(relation, rendered_sql, mode=schema_policy)
        except Exception:
            pass

        # Incremental Load
        if unique_key:
            try:
                executor.incremental_merge(relation, rendered_sql, unique_key)
            except Exception:
                # Fallback: Full replace
                _exec_sql(f"create or replace table {relation} as {rendered_sql}")
        else:
            try:
                executor.incremental_insert(relation, rendered_sql)
            except Exception:
                # Fallback: full replace
                _exec_sql(f"create or replace table {relation} as {rendered_sql}")
