from __future__ import annotations

from typing import Any

from fastflowtransform.decorators import model


@model(
    name="py_enrich.ff",
    kind="python",
    deps=["fct_events.ff"],
    tags=[
        "example:ci_demo",
        "scope:engine",
        "engine:duckdb",
        "engine:postgres",
        "engine:databricks_spark",
        "engine:bigquery",
        "engine:snowflake_snowpark",
    ],
)
def build(fct_events: Any) -> Any:
    """
    Minimal Python model: takes the fct_events frame/table and adds a dummy flag.
    The executor decides how 'fct_events' is provided (DuckDB.df, pandas, etc.).
    """
    # Assume fct_events is something DataFrame-like with .assign
    try:
        return fct_events.assign(is_enriched=True)  # pandas / polars-style
    except AttributeError:
        # Fallback: if it's not DataFrame-like, just return as-is
        return fct_events
