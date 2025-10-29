# src/fastflowtransform/seeding.py
from __future__ import annotations

import math
import shutil
import uuid
from collections.abc import Callable, Iterable
from contextlib import suppress
from pathlib import Path
from time import perf_counter
from typing import Any, NamedTuple
from urllib.parse import unquote, urlparse

import pandas as pd
import yaml

from fastflowtransform.logging import echo

try:  # Optional Spark dependency
    from pyspark.errors.exceptions.base import AnalysisException as _SparkAnalysisException
except Exception:  # pragma: no cover - Spark not installed
    _SparkAnalysisException = Exception  # type: ignore

# If you use this in a CLI, your code elsewhere should provide _prepare_context.


# ----------------------------- File I/O & Schema (dtypes) -----------------------------


def _read_seed_file(path: Path) -> pd.DataFrame:
    """Read a seed file (.csv, .parquet, .pq) into a pandas DataFrame."""
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in (".parquet", ".pq"):
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported seed file format: {path.name}")


def _apply_schema(df: pd.DataFrame, table: str, schema_cfg: dict | None) -> pd.DataFrame:
    """
    Apply optional pandas dtypes from seeds/schema.yml for a given table key.
    Expected structure:
      dtypes:
        <table_key>:
          col_a: string
          col_b: int64
    Soft-fails on casting errors to avoid blocking loads.
    """
    if not schema_cfg:
        return df
    cfg = schema_cfg.get("dtypes") or {}
    dtypes: dict[str, str] = cfg.get(table) or {}
    if not dtypes:
        return df

    cast_map = {col: dtype for col, dtype in dtypes.items() if col in df.columns}
    try:
        return df.astype(cast_map)
    except Exception:
        # Prefer loading data over failing the run; you may log a warning here.
        return df


# -------------------------------- Identifier utilities --------------------------------


def _dq(ident: str) -> str:
    """Double-quote an SQL identifier (DuckDB/Postgres compatible)."""
    return '"' + ident.replace('"', '""') + '"'


def _is_qualified(name: str) -> bool:
    """Return True if the provided table name appears to be schema-qualified."""
    return "." in name


def _qualify(table: str, schema: str | None) -> str:
    """
    Return a safely quoted, optionally schema-qualified identifier.
    - Respects already-qualified names like raw.users or "raw"."users".
    - Quotes each identifier part individually.
    """
    if _is_qualified(table):
        return ".".join(_dq(p) for p in table.split("."))
    if schema:
        return f"{_dq(schema)}.{_dq(table)}"
    return _dq(table)


def _spark_warehouse_base(spark: Any) -> Path | None:
    """Resolve the Spark warehouse directory if it points to the local filesystem."""
    try:
        conf_val = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    except Exception:
        conf_val = "spark-warehouse"

    if not isinstance(conf_val, str):
        conf_val = str(conf_val)
    parsed = urlparse(conf_val)
    scheme = (parsed.scheme or "").lower()

    if scheme and scheme != "file":
        return None

    if scheme == "file":
        # Treat file:// URIs as local filesystem paths.
        if parsed.netloc and parsed.netloc not in {"", "localhost"}:
            return None
        raw_path = unquote(parsed.path or "")
        if not raw_path:
            return None
        base = Path(raw_path)
    else:
        base = Path(conf_val)

    if not base.is_absolute():
        base = Path.cwd() / base
    return base


def _spark_table_location(parts: list[str], spark: Any) -> Path | None:
    """
    Best-effort guess of the filesystem location for a managed Spark table.
    Works for default schema, schema.table, and catalog.schema.table patterns.
    """
    base = _spark_warehouse_base(spark)
    if base is None or not parts:
        return None

    filtered = [p for p in parts if p]
    if not filtered:
        return None

    # Drop common catalog prefixes while retaining the schema name.
    catalog_cutoff = 3
    if len(filtered) >= catalog_cutoff and filtered[0].lower() in {"spark_catalog", "spark"}:
        filtered = filtered[1:]

    table = filtered[-1]
    schema_cutoff = 2
    schema = filtered[-2] if len(filtered) >= schema_cutoff else None

    location = base
    if schema:
        location = location / f"{schema}.db"
    return location / table


# -------------------------------- Pretty echo helpers ---------------------------------


def _human_int(n: int) -> str:
    """Format integers with thin-space grouping (12 345)."""
    return f"{n:,}".replace(",", " ")


def _human_bytes(n: int) -> str:
    """Coarse byte-size formatting for user hints."""
    mb_threshold = 1024
    if n < mb_threshold:
        return f"{n} B"
    units = ["KB", "MB", "GB", "TB", "PB"]
    exp = min(int(math.log(n, 1024)), len(units))
    val = n / (1024**exp)
    unit = units[exp - 1] if exp > 0 else "KB"
    return f"{val:.1f} {unit}"


def _echo_seed_line(
    full_name: str,
    rows: int,
    cols: int,
    engine: str,
    ms: int,
    created_schema: bool = False,
    action: str = "replaced",
    extra: str | None = None,
) -> None:
    """
    Emit a single pretty seed log line, e.g.:
      — ✓ raw.users • 12 345x6 • 1.2 MB • 138 ms [duckdb] (+schema)
    """
    size_hint: str | None = None
    with suppress(Exception):
        # Heuristic: 8 bytes per cell. Good enough as a hint, not exact.
        size_hint = _human_bytes(rows * max(cols, 1) * 8)

    parts = [
        f"✓ {full_name}",
        f"{_human_int(rows)}×{cols}",  # Noqa RUF001
        *([size_hint] if size_hint else []),
        f"{ms} ms",
        f"[{engine}]",
        *(["(+schema)"] if created_schema else []),
        *([extra] if extra else []),
    ]
    echo("— " + " • ".join(parts))


# ------------------------------ Target resolution (CFG) -------------------------------


class SeedTarget(NamedTuple):
    """Resolved seed target (schema, table)."""

    schema: str | None
    table: str


def _engine_name_from_executor(executor: Any) -> str:
    """Infer a human/CFG-facing engine name from the executor object."""
    if getattr(executor, "spark", None) is not None:
        return "spark"
    eng = getattr(executor, "engine", None)
    if eng is not None:
        name = getattr(getattr(eng, "dialect", None), "name", None)
        if name:
            return str(name)
    if getattr(executor, "con", None) is not None:
        return "duckdb"
    return "unknown"


def _seed_id(seeds_dir: Path, path: Path) -> str:
    """
    Build a unique seed ID from the path relative to `seeds/`, without the extension.
    Examples:
      seeds/raw/users.csv      -> "raw/users"
      seeds/staging/users.parquet -> "staging/users"
      seeds/users.csv          -> "users"
    """
    rel = path.relative_to(seeds_dir)
    return rel.with_suffix("").as_posix()


def _resolve_schema_and_table_by_cfg(
    seed_id: str,
    stem: str,
    schema_cfg: dict | None,
    executor: Any,
    default_schema: str | None,
) -> tuple[str | None, str]:
    """
    Resolve (schema, table) using seeds/schema.yml with a clear priority:
      1) targets[<seed_id>] (recommended; path-based ID e.g. "raw/users")
      2) targets[<seed_id with dots>] (optional convenience; "raw.users")
      3) targets[<stem>] (legacy; only safe if the stem is unique)
      4) default_schema (profile/executor-supplied)
    Supports:
      targets:
        raw/users:
          schema: raw
          table: users
          schema_by_engine:
            postgres: raw
            duckdb: main
    """
    schema = default_schema
    table = stem
    if not schema_cfg:
        return schema, table

    targets: dict[str, dict] = schema_cfg.get("targets") or {}
    engine = _engine_name_from_executor(executor)

    entry = targets.get(seed_id)
    if not entry:
        entry = targets.get(seed_id.replace("/", "."))  # optional "raw.users" key

    # stem-based only if present (uniqueness checked by caller)
    if not entry and stem in targets:
        entry = targets[stem]

    if not entry:
        return schema, table

    table = entry.get("table", table)
    by_engine = entry.get("schema_by_engine") or {}
    schema = by_engine.get(engine, entry.get("schema", schema))
    return schema, table


# ------------------------------ Materialization (engines) ------------------------------


# def materialize_seed(
#     table: str, df: pd.DataFrame, executor: Any, schema: str | None = None
# ) -> None:
#     """
#     Materialize a DataFrame as a database table across engines.

#     Behavior:
#       DuckDB:
#         - Registers a temporary view for the DataFrame and performs
#           ``CREATE OR REPLACE TABLE <schema>.<table> AS SELECT * FROM <tmp>``.
#         - Ensures ``CREATE SCHEMA IF NOT EXISTS`` when requested.

#       SQLAlchemy engines (e.g., Postgres):
#         - Uses ``pandas.DataFrame.to_sql(if_exists="replace", schema=schema)``.

#     Args:
#         table: Target table name.
#         df: Data to persist.
#         executor: Backend connection/engine (e.g., DuckDB connection
#         or SQLAlchemy Engine/Connection).
#         schema: Optional schema name for the target table.

#     Raises:
#         RuntimeError: If no supported executor connection is detected.
#     """
#     # DuckDB path (robust detection)
#     con = getattr(executor, "con", None)
#     if con is not None:
#         try:
#             is_duck_con = isinstance(con, _dd.DuckDBPyConnection)
#         except Exception:
#             is_duck_con = all(hasattr(con, m) for m in ("register", "execute"))

#         if is_duck_con:
#             full_name = _qualify(table, schema)
#             created_schema = False
#             if schema and not _is_qualified(table):
#                 con.execute(f"create schema if not exists {_dq(schema)}")
#                 created_schema = True

#             t0 = perf_counter()
#             tmp = f"_ff_seed_{uuid.uuid4().hex[:8]}"
#             con.register(tmp, df)
#             try:
#                 con.execute(f"create or replace table {full_name} as select * from {_dq(tmp)}")
#             finally:
#                 try:
#                     con.unregister(tmp)  # duckdb >= 0.8
#                 except Exception:
#                     con.execute(f"drop view if exists {_dq(tmp)}")
#             dt_ms = int((perf_counter() - t0) * 1000)

#             _echo_seed_line(
#                 full_name=full_name,
#                 rows=len(df),
#                 cols=df.shape[1],
#                 engine="duckdb",
#                 ms=dt_ms,
#                 created_schema=created_schema,
#                 action="replaced",
#             )
#             return

#     # SQLAlchemy Engine path
#     eng = getattr(executor, "engine", None)
#     if eng is not None and "sqlalchemy" in eng.__class__.__module__:
#         t0 = perf_counter()
#         df.to_sql(table, eng, if_exists="replace", index=False, schema=schema, method="multi")
#         dt_ms = int((perf_counter() - t0) * 1000)
#         dialect = getattr(getattr(eng, "dialect", None), "name", "sqlalchemy")
#         _echo_seed_line(
#             full_name=_qualify(table, schema),
#             rows=len(df),
#             cols=df.shape[1],
#             engine=dialect,
#             ms=dt_ms,
#             created_schema=False,
#             action="replaced",
#         )
#         return

#     # Spark / Databricks path
#     spark = getattr(executor, "spark", None)
#     if spark is not None:
#         def _spark_ident(name: str) -> str:
#             return name.replace("`", "``")

#         created_schema = False
#         if schema and not _is_qualified(table):
#             spark.sql(f"CREATE DATABASE IF NOT EXISTS `{_spark_ident(schema)}`")
#             created_schema = True
#             parts = [schema, table]
#         else:
#             parts = table.split(".")

#         parts = [p for p in parts if p]
#         target_identifier = ".".join(parts)
#         target_sql = ".".join(f"`{_spark_ident(p)}`" for p in parts)
#         target_location = _spark_table_location(parts, spark)

#         t0 = perf_counter()
#         sdf = spark.createDataFrame(df)
#         cleanup_hint = None

#         with suppress(Exception):
#             spark.sql(f"DROP TABLE IF EXISTS {target_sql}")
#         if target_location and target_location.exists():
#             with suppress(Exception):
#                 shutil.rmtree(target_location, ignore_errors=True)
#                 cleanup_hint = "reset location"

#         table_format = getattr(executor, "spark_table_format", None)
#         table_options = getattr(executor, "spark_table_options", None) or {}

#         def _write() -> None:
#             writer = sdf.write.mode("overwrite")
#             if table_format:
#                 writer = writer.format(table_format)
#             if table_options:
#                 writer = writer.options(**table_options)
#             writer.saveAsTable(target_identifier)

#         try:
#             _write()
#         except _SparkAnalysisException as exc:  # type: ignore[misc]
#             message = str(exc)
#             if target_location and "LOCATION_ALREADY_EXISTS" in message.upper():
#                 with suppress(Exception):
#                     shutil.rmtree(target_location, ignore_errors=True)
#                 cleanup_hint = "reset location"
#                 _write()
#             else:
#                 raise RuntimeError(f"Spark seed load failed for {target_sql}: {exc}") from exc
#         except Exception as exc:
#             raise RuntimeError(f"Spark seed load failed for {target_sql}: {exc}") from exc
#         dt_ms = int((perf_counter() - t0) * 1000)

#         _echo_seed_line(
#             full_name=target_sql,
#             rows=len(df),
#             cols=df.shape[1],
#             engine="spark",
#             ms=dt_ms,
#             created_schema=created_schema,
#             action="replaced",
#             extra=cleanup_hint,
#         )
#         return

#     # Fallback (not implemented): you could emit VALUES via executor.execute(sql) for tiny seeds.
#     raise RuntimeError("No compatible executor connection for seeding found.")


# ------------------------------------------------------------
# Engine-specifig Handlers
# ------------------------------------------------------------


def _handle_duckdb(table: str, df: pd.DataFrame, executor: Any, schema: str | None) -> bool:
    """Versucht DuckDB zu erkennen und zu bedienen. Gibt True zurück, wenn ausgeführt."""
    con = getattr(executor, "con", None)
    if con is None:
        return False

    try:
        import duckdb as _dd  # Noqa PLC0415

        is_duck_con = isinstance(con, _dd.DuckDBPyConnection)
    except Exception:
        is_duck_con = all(hasattr(con, m) for m in ("register", "execute"))

    if not is_duck_con:
        return False

    full_name = _qualify(table, schema)
    created_schema = False
    if schema and not _is_qualified(table):
        con.execute(f"create schema if not exists {_dq(schema)}")
        created_schema = True

    t0 = perf_counter()
    tmp = f"_ff_seed_{uuid.uuid4().hex[:8]}"
    con.register(tmp, df)
    try:
        con.execute(f"create or replace table {full_name} as select * from {_dq(tmp)}")
    finally:
        with suppress(Exception):
            con.unregister(tmp)  # duckdb >= 0.8
        with suppress(Exception):
            con.execute(f"drop view if exists {_dq(tmp)}")

    dt_ms = int((perf_counter() - t0) * 1000)
    _echo_seed_line(
        full_name=full_name,
        rows=len(df),
        cols=df.shape[1],
        engine="duckdb",
        ms=dt_ms,
        created_schema=created_schema,
        action="replaced",
    )
    return True


def _handle_sqlalchemy(table: str, df: pd.DataFrame, executor: Any, schema: str | None) -> bool:
    """Versucht SQLAlchemy-Engine/-Connection zu erkennen und zu bedienen."""
    eng = getattr(executor, "engine", None)
    if eng is None:
        return False
    # heuristik: viele SQLAlchemy-Engines haben 'sqlalchemy' im Modulpfad der Klasse
    if "sqlalchemy" not in getattr(eng.__class__, "__module__", ""):
        return False

    t0 = perf_counter()
    # pandas übernimmt die DDL/DML — replace-Semantik wie im Original
    df.to_sql(table, eng, if_exists="replace", index=False, schema=schema, method="multi")
    dt_ms = int((perf_counter() - t0) * 1000)

    dialect = getattr(getattr(eng, "dialect", None), "name", "sqlalchemy")
    _echo_seed_line(
        full_name=_qualify(table, schema),
        rows=len(df),
        cols=df.shape[1],
        engine=dialect,
        ms=dt_ms,
        created_schema=False,
        action="replaced",
    )
    return True


def _handle_spark(table: str, df: pd.DataFrame, executor: Any, schema: str | None) -> bool:
    """Versucht Spark/Databricks zu erkennen und zu bedienen."""
    spark = getattr(executor, "spark", None)
    if spark is None:
        return False

    def _spark_ident(name: str) -> str:
        return name.replace("`", "``")

    created_schema = False
    if schema and not _is_qualified(table):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{_spark_ident(schema)}`")
        created_schema = True
        parts = [schema, table]
    else:
        parts = table.split(".")

    parts = [p for p in parts if p]
    target_identifier = ".".join(parts)
    target_sql = ".".join(f"`{_spark_ident(p)}`" for p in parts)
    target_location = _spark_table_location(parts, spark)

    t0 = perf_counter()
    sdf = spark.createDataFrame(df)
    cleanup_hint = None

    with suppress(Exception):
        spark.sql(f"DROP TABLE IF EXISTS {target_sql}")
    if target_location and target_location.exists():
        with suppress(Exception):
            shutil.rmtree(target_location, ignore_errors=True)
            cleanup_hint = "reset location"

    table_format = getattr(executor, "spark_table_format", None)
    table_options = getattr(executor, "spark_table_options", None) or {}

    def _write() -> None:
        writer = sdf.write.mode("overwrite")
        if table_format:
            writer = writer.format(table_format)
        if table_options:
            writer = writer.options(**table_options)
        writer.saveAsTable(target_identifier)

    try:
        _write()
    except _SparkAnalysisException as exc:
        message = str(exc)
        if target_location and "LOCATION_ALREADY_EXISTS" in message.upper():
            with suppress(Exception):
                shutil.rmtree(target_location, ignore_errors=True)
            cleanup_hint = "reset location"
            _write()
        else:
            raise RuntimeError(f"Spark seed load failed for {target_sql}: {exc}") from exc
    except Exception as exc:
        raise RuntimeError(f"Spark seed load failed for {target_sql}: {exc}") from exc

    dt_ms = int((perf_counter() - t0) * 1000)
    _echo_seed_line(
        full_name=target_sql,
        rows=len(df),
        cols=df.shape[1],
        engine="spark",
        ms=dt_ms,
        created_schema=created_schema,
        action="replaced",
        extra=cleanup_hint,
    )
    return True


# ------------------------------------------------------------
# Dispatcher
# ------------------------------------------------------------

Handler = Callable[[str, pd.DataFrame, Any, str | None], bool]

_HANDLERS: Iterable[Handler] = (
    _handle_duckdb,
    _handle_sqlalchemy,
    _handle_spark,
)


def materialize_seed(
    table: str, df: pd.DataFrame, executor: Any, schema: str | None = None
) -> None:
    """
    Materialize a DataFrame as a database table across engines.

    Engine-spezifische Logik ist in dedizierten Handlern gekapselt
    (_handle_duckdb/_handle_sqlalchemy/_handle_spark). Der Dispatcher
    ruft sie der Reihe nach auf, bis einer übernimmt.
    """
    for handler in _HANDLERS:
        if handler(table, df, executor, schema):
            return

    raise RuntimeError("No compatible executor connection for seeding found.")


# ----------------------------------- Seeding runner -----------------------------------


def seed_project(project_dir: Path, executor: Any, default_schema: str | None = None) -> int:
    """
    Load every seed file under <project>/seeds recursively and materialize it.

    Supports configuration in seeds/schema.yml:
      - targets:
          <seed-id>:                 # e.g., "raw/users" (path-based, recommended)
            schema: <schema-name>    # global target schema
            table: <table-name>      # optional rename
            schema_by_engine:        # optional engine overrides
              postgres: raw
              duckdb: main
      - dtypes:
          <table-key>:
            column_a: string
            column_b: int64

    Resolution priority for (schema, table):
      1) targets[<seed-id>]  (e.g., "raw/users")
      2) targets[<seed-id with dots>] (e.g., "raw.users")
      3) targets[<stem>] (*only* if stem is unique)
      4) executor.schema or default_schema

    Returns:
      Number of successfully materialized seed tables.

    Raises:
      ValueError: if schema.yml uses a plain stem key while multiple files share that stem.
    """
    seeds_dir = project_dir / "seeds"
    if not seeds_dir.exists():
        return 0

    schema_cfg = None
    schema_file = seeds_dir / "schema.yml"
    if schema_file.exists():
        schema_cfg = yaml.safe_load(schema_file.read_text(encoding="utf-8"))

    # Collect seed files recursively to allow folder-based schema conventions.
    paths: list[Path] = [
        p
        for p in sorted(seeds_dir.rglob("*"))
        if p.is_file() and p.suffix.lower() in (".csv", ".parquet", ".pq")
    ]
    if not paths:
        return 0

    # Check for ambiguous stems (same filename in different folders).
    stem_counts: dict[str, int] = {}
    for p in paths:
        stem_counts[p.stem] = stem_counts.get(p.stem, 0) + 1

    count = 0
    for path in paths:
        seedid = _seed_id(seeds_dir, path)
        stem = path.stem

        # Default schema may come from executor or caller.
        base_schema = getattr(executor, "schema", None) or default_schema
        schema, table = _resolve_schema_and_table_by_cfg(
            seedid, stem, schema_cfg, executor, base_schema
        )

        # If schema.yml uses a bare stem while that stem exists multiple times,
        # force disambiguation.
        if (
            schema_cfg
            and (schema_cfg.get("targets") or {}).get(stem)
            and stem_counts.get(stem, 0) > 1
        ):
            raise ValueError(
                f'Seed stem "{stem}" appears multiple times. '
                f"Please configure using the path-based seed ID "
                f'(e.g., "{seedid}") in seeds/schema.yml.'
            )

        df = _read_seed_file(path)
        # Use the resolved *table* key for dtypes (allows rename-aware dtype mapping in cfg).
        df = _apply_schema(df, table, schema_cfg)

        materialize_seed(table, df, executor, schema=schema)
        count += 1

    return count
