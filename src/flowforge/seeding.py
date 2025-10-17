# src/flowforge/seeding.py
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import duckdb as _dd
import pandas as pd
import yaml


def _read_seed_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in (".parquet", ".pq"):
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported seed file format: {path.name}")


def _apply_schema(df: pd.DataFrame, table: str, schema_cfg: dict | None) -> pd.DataFrame:
    if not schema_cfg:
        return df
    cfg = schema_cfg.get(table) or {}
    dtypes: dict[str, str] = cfg.get("dtypes") or {}
    if dtypes:
        # Apply pandas dtypes, ignore unknown columns
        cast_map = {col: dtype for col, dtype in dtypes.items() if col in df.columns}
        try:
            return df.astype(cast_map)
        except Exception:
            # Soft-fail: load the data instead of aborting - logging might be useful here
            pass
    return df


def materialize_seed(
    table: str, df: pd.DataFrame, executor: Any, schema: str | None = None
) -> None:
    """
    Engine-agnostic materialization:
    - DuckDB: register + CREATE OR REPLACE TABLE
    - Postgres/SA-Engines: pandas.to_sql(..., if_exists='replace', schema=...)
    """
    # DuckDB (robust detection)
    con = getattr(executor, "con", None)
    if con is not None:
        try:
            is_duck_con = isinstance(con, _dd.DuckDBPyConnection)
        except Exception:
            # Fallback: treat as "duckdb-like" when register/execute exist
            is_duck_con = all(hasattr(con, m) for m in ("register", "execute"))
        if is_duck_con:
            tmp = f"_ff_seed_{uuid.uuid4().hex[:8]}"
            con.register(tmp, df)
            con.execute(f"create or replace table {table} as select * from {tmp}")
            try:
                con.unregister(tmp)  # duckdb >= 0.8
            except Exception:
                con.execute(f"drop view if exists {tmp}")
            return

    # SQLAlchemy-Engine?
    eng = getattr(executor, "engine", None)
    if eng is not None and "sqlalchemy" in eng.__class__.__module__:
        df.to_sql(table, eng, if_exists="replace", index=False, schema=schema, method="multi")
        return

    # Fallback: try feeding executor.execute(sql) with VALUES statements (for small seeds)
    # (Optional - usually not needed if handled above)
    raise RuntimeError("No compatible executor connection for seeding found.")


def seed_project(project_dir: Path, executor: Any, default_schema: str | None = None) -> int:
    """
    Load every file under <project>/seeds/*.{csv,parquet} and write it as a table
    (file name becomes table name). Returns the number of seeds loaded.
    """
    seeds_dir = project_dir / "seeds"
    if not seeds_dir.exists():
        return 0

    schema_cfg = None
    schema_file = seeds_dir / "schema.yml"
    if schema_file.exists():
        schema_cfg = yaml.safe_load(schema_file.read_text(encoding="utf-8"))

    count = 0
    for path in sorted(seeds_dir.iterdir()):
        if path.suffix.lower() not in (".csv", ".parquet", ".pq"):
            continue
        table = path.stem  # e.g. seed_users
        df = _read_seed_file(path)
        df = _apply_schema(df, table, schema_cfg)
        # Schema only for engines that need it (PG). DuckDB ignores it in materialize_seed.
        schema = getattr(executor, "schema", None) or default_schema
        materialize_seed(table, df, executor, schema=schema)
        count += 1
    return count
