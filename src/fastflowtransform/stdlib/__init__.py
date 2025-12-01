# fastflowtransform/stdlib/__init__.py
from __future__ import annotations

from collections.abc import Callable, Mapping

from jinja2 import Environment

from .casts import sql_safe_cast
from .dates import sql_date_add, sql_date_trunc
from .engine import engine_family, is_engine, normalize_engine
from .partitions import sql_partition_filter, sql_partition_in
from .sql import sql_literal

"""
FastFlowTransform stdlib - engine-aware SQL helper functions.

These are meant to be exposed into Jinja as:

    {{ ff_date_trunc('day', 'order_date') }}
    {{ ff_date_add('day', 'order_date', 7) }}
    {{ ff_safe_cast('raw_value', 'INTEGER', default='0') }}
    {{ ff_partition_filter('ds', var('from_date'), var('to_date')) }}
    {{ ff_partition_in('ds', var('partitions')) }}

The `register_jinja(...)` helper wires everything into a Jinja Environment
so that adding new stdlib helpers only requires changes inside this package.
"""

__all__ = [
    "engine_family",
    "is_engine",
    "normalize_engine",
    "register_jinja",
    "sql_date_add",
    "sql_date_trunc",
    "sql_literal",
    "sql_partition_filter",
    "sql_partition_in",
    "sql_safe_cast",
]


# --- Registration metadata -------------------------------------------------

# Functions that should get an implicit `engine=<current>` kwarg when called
# from templates (but templates can still override engine=... explicitly).
_STD_FUNCS_ENGINE_DEFAULT: Mapping[str, Callable] = {
    "ff_date_trunc": sql_date_trunc,
    "ff_date_add": sql_date_add,
    "ff_safe_cast": sql_safe_cast,
    "ff_partition_filter": sql_partition_filter,
    "ff_partition_in": sql_partition_in,
}

# Helpers that are engine-agnostic and can be exposed as-is.
_STD_FUNCS_RAW: Mapping[str, Callable] = {
    "normalize_engine": normalize_engine,
    "engine_family": engine_family,
    "is_engine": is_engine,
}


def register_jinja(
    env: Environment,
    *,
    engine_resolver: Callable[[], str | None] | None = None,
    engine: str | None = None,
) -> None:
    """
    Register all stdlib helpers into a Jinja `Environment`.

    Either pass:
      - engine_resolver: a callable returning the current engine key, OR
      - engine: a fixed engine key string.

    Example from core:
        from fastflowtransform import stdlib as ff_stdlib
        ff_stdlib.register_jinja(env, engine_resolver=self._current_engine)
    """
    if engine is None and engine_resolver is not None:
        try:
            engine = engine_resolver()
        except Exception:
            engine = None

    # normalized current engine (e.g. "duckdb", "bigquery", "postgres", "generic")
    engine_key = normalize_engine(engine)

    def _bind_engine(fn: Callable) -> Callable:
        """
        Wrap a stdlib function so that Jinja templates automatically get
        the current engine injected as a default kwarg:

            {{ ff_date_trunc('day', 'col') }}

        becomes effectively:

            sql_date_trunc('day', 'col', engine=engine_key)
        """

        def wrapper(*args, **kwargs):
            kwargs.setdefault("engine", engine_key)
            return fn(*args, **kwargs)

        return wrapper

    # Register the engine-bound helpers
    for name, fn in _STD_FUNCS_ENGINE_DEFAULT.items():
        env.globals[name] = _bind_engine(fn)

    # Register low-level engine helpers (no auto-engine injection)
    for name, fn in _STD_FUNCS_RAW.items():
        env.globals[name] = fn

    # Template-friendly helpers that know the *current* engine:
    #   {{ ff_engine() }}
    #   {% if ff_is_engine('bigquery') %} ... {% endif %}
    def _ff_engine(default: str | None = None) -> str:
        # Prefer the active engine; fall back to a normalized default or "generic"
        if engine_key != "generic":
            return engine_key
        if default is not None:
            return normalize_engine(default)
        return engine_key

    def _ff_is_engine(*candidates: str) -> bool:
        return is_engine(engine_key, *candidates)

    env.globals["ff_engine"] = _ff_engine
    env.globals["ff_is_engine"] = _ff_is_engine
