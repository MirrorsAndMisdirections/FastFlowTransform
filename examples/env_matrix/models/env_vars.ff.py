# models/env_vars.ff.py
# Purpose: Minimal, engine-agnostic Python model that echoes which environment
#          variables are active for the current run. Optimized for the
#          "all-DuckDB" setup (dev/stg/prod each point to a different file).

from __future__ import annotations
import os
import pathlib
import pandas as pd

from fastflowtransform import model  # your existing decorator


@model(
    name="env_vars.ff",
    tags=["demo", "env"],
    kind="python",
    materialized="table",
)
def build(_: pd.DataFrame | None) -> pd.DataFrame:
    """
    Return a single-row DataFrame showing the key environment variables
    for the DuckDB-only setup.

    Columns:
      - active_env_hint: optional marker from your .env.<env> file (if you set FFT_ACTIVE_ENV).
      - ff_engine: should be "duckdb" for all environments in this demo.
      - duckdb_path: the DB file path in use for the active environment.
      - duckdb_exists: whether that file currently exists on disk.
      - duckdb_size_bytes: file size if it exists (0 otherwise).
    """
    duck_path = os.getenv("FF_DUCKDB_PATH", "")
    p = pathlib.Path(duck_path)
    exists = bool(p and p.exists())
    size = int(p.stat().st_size) if exists else 0

    row = {
        "active_env_hint": os.getenv(
            "FFT_ACTIVE_ENV"
        ),  # set in .env.dev/.env.stg/.env.prod (optional)
        "ff_engine": os.getenv("FF_ENGINE", "duckdb"),
        "duckdb_path": duck_path,
        "duckdb_exists": exists,
        "duckdb_size_bytes": size,
    }
    return pd.DataFrame([row])
