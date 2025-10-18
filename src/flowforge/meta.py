# src/flowforge/meta.py
"""
Engine-aware metadata store and relation-existence helpers.

This module persists a per-engine `_ff_meta` table with the following columns:
  - node_name (PK where supported)
  - relation
  - fp
  - engine
  - built_at (server-side timestamp)

APIs:
  ensure_meta_table(executor)
  upsert_meta(executor, node_name, relation, fp, engine)
  get_meta(executor, node_name) -> tuple[str, str, object, str] | None
  relation_exists(executor, relation) -> bool

Supported engines:
  - DuckDB  (executor.con)
  - Postgres (executor.engine, optional .schema)
  - BigQuery (executor.client, .dataset, optional .project)
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

# --------------------------- Engine detection ---------------------------


def _is_duckdb(ex: Any) -> bool:
    return hasattr(ex, "con") and hasattr(ex.con, "execute")


def _is_postgres(ex: Any) -> bool:
    return hasattr(ex, "engine") and hasattr(ex.engine, "begin")


def _is_bigquery(ex: Any) -> bool:
    return hasattr(ex, "client") and hasattr(ex, "dataset")


# --------------------------- Qualifier helpers ---------------------------


def _duck_name(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _pg_qual_meta(ex: Any) -> str:
    schema = getattr(ex, "schema", None)
    if schema:
        return f'"{schema}"."__ff_meta"' if schema.startswith("__") else f'"{schema}"."_ff_meta"'
    return '"_ff_meta"'


def _bq_qual_meta(ex: Any) -> str:
    dataset = getattr(ex, "dataset", None)
    project = getattr(ex, "project", None)
    if not dataset:
        # best effort fallback (caller will fail later anyway)
        return "`_ff_meta`"
    if project:
        return f"`{project}.{dataset}._ff_meta`"
    return f"`{dataset}._ff_meta`"


# --------------------------- Public API ---------------------------


def ensure_meta_table(executor: Any) -> None:
    """
    Create the _ff_meta table if it does not exist for the active engine.
    """
    if _is_duckdb(executor):
        sql = (
            'create table if not exists "_ff_meta" ('
            "  node_name text primary key,"
            "  relation text,"
            "  fp text,"
            "  engine text,"
            "  built_at timestamp default current_timestamp"
            ")"
        )
        executor.con.execute(sql)
        return

    if _is_postgres(executor):
        qual = _pg_qual_meta(executor)
        ddl = (
            f"create table if not exists {qual} ("
            "  node_name text primary key,"
            "  relation text,"
            "  fp text,"
            "  engine text,"
            "  built_at timestamptz default now()"
            ")"
        )
        with executor.engine.begin() as conn:
            conn.execute(text(ddl))
        return

    if _is_bigquery(executor):
        # BigQuery supports IF NOT EXISTS in standard SQL DDL
        qual = _bq_qual_meta(executor)
        ddl = (
            f"create table if not exists {qual} ("
            "  node_name string,"
            "  relation string,"
            "  fp string,"
            "  engine string,"
            "  built_at timestamp"
            ")"
        )
        executor.client.query(ddl).result()
        return

    # Unknown engine: no-op


def upsert_meta(executor: Any, node_name: str, relation: str, fp: str, engine: str) -> None:
    """
    Insert or update `_ff_meta` for a given node.
    """
    ensure_meta_table(executor)

    if _is_duckdb(executor):
        # DuckDB: emulate upsert via delete + insert inside the same connection.
        executor.con.execute('delete from "_ff_meta" where node_name = ?', [node_name])
        executor.con.execute(
            'insert into "_ff_meta"(node_name, relation, fp, engine, built_at) '
            "values (?, ?, ?, ?, current_timestamp)",
            [node_name, relation, fp, engine],
        )
        return

    if _is_postgres(executor):
        qual = _pg_qual_meta(executor)
        sql = (
            f"insert into {qual}(node_name, relation, fp, engine, built_at) "
            "values (:n, :r, :f, :e, now()) "
            "on conflict (node_name) do update set "
            "  relation = excluded.relation, "
            "  fp = excluded.fp, "
            "  engine = excluded.engine, "
            "  built_at = now()"
        )
        with executor.engine.begin() as conn:
            conn.execute(text(sql), {"n": node_name, "r": relation, "f": fp, "e": engine})
        return

    if _is_bigquery(executor):
        qual = _bq_qual_meta(executor)

        # Use MERGE to emulate upsert
        # Parameterization with BigQuery QueryJobConfig is optional; build a safe literal instead.
        def _q(s: str) -> str:
            return s.replace("\\", "\\\\").replace("`", "\\`").replace("'", "\\'")

        sql = f"""merge {qual} T
        using (
          select '{_q(node_name)}' as node_name,
                 '{_q(relation)}'  as relation,
                 '{_q(fp)}'        as fp,
                 '{_q(engine)}'    as engine
        ) S
        on T.node_name = S.node_name
        when matched then update set
          relation = S.relation,
          fp       = S.fp,
          engine   = S.engine,
          built_at = current_timestamp()
        when not matched then insert (node_name, relation, fp, engine, built_at)
          values (S.node_name, S.relation, S.fp, S.engine, current_timestamp())
        """
        executor.client.query(sql).result()
        return

    # Unknown engine: no-op


def get_meta(executor: Any, node_name: str) -> tuple[str, str, Any, str] | None:
    """
    Return (fp, relation, built_at, engine) for the node, or None if not found.
    """
    if _is_duckdb(executor):
        row = executor.con.execute(
            'select fp, relation, built_at, engine from "_ff_meta" where node_name = ? limit 1',
            [node_name],
        ).fetchone()
        return (row[0], row[1], row[2], row[3]) if row else None

    if _is_postgres(executor):
        qual = _pg_qual_meta(executor)
        with executor.engine.begin() as conn:
            row = conn.execute(
                text(
                    f"select fp, relation, built_at, engine from {qual} "
                    "where node_name = :n limit 1"
                ),
                {"n": node_name},
            ).fetchone()
        return (row[0], row[1], row[2], row[3]) if row else None

    if _is_bigquery(executor):
        qual = _bq_qual_meta(executor)
        # Parameterized query would need google.cloud.bigquery; keep it dependency-light.
        node = node_name.replace("\\", "\\\\").replace("`", "\\`").replace("'", "\\'")
        sql = (
            f"select fp, relation, built_at, engine from {qual} where node_name = '{node}' limit 1"
        )
        rows = list(executor.client.query(sql).result())
        if not rows:
            return None
        r = rows[0]
        # Access by field name if available, else positional
        try:
            return (r["fp"], r["relation"], r["built_at"], r["engine"])
        except Exception:
            return (r[0], r[1], r[2], r[3])

    return None


def relation_exists(executor: Any, relation: str) -> bool:
    """
    Check whether a materialized relation exists on the active engine.
    """
    if _is_duckdb(executor):
        try:
            rows = executor.con.execute(
                "select 1 from information_schema.tables "
                + "where table_schema in ('main','temp') and table_name = ?",
                [relation],
            ).fetchall()
            return bool(rows)
        except Exception:
            return True  # be permissive on unexpected errors

    if _is_postgres(executor):
        try:
            with executor.engine.begin() as conn:
                rows = conn.execute(
                    text(
                        "select 1 from information_schema.tables "
                        + "where table_schema = current_schema() and table_name = :t"
                    ),
                    {"t": relation},
                ).fetchall()
            return bool(rows)
        except Exception:
            return True

    if _is_bigquery(executor):
        try:
            dataset = getattr(executor, "dataset", None)
            project = getattr(executor, "project", None)
            if not dataset:
                return True
            qual = f"`{project}.{dataset}`" if project else f"`{dataset}`"
            rel = relation.replace("`", "\\`").replace("'", "\\'")
            sql = (
                f"select 1 from {qual}.INFORMATION_SCHEMA.TABLES where table_name = '{rel}' limit 1"
            )
            rows = list(executor.client.query(sql).result())
            return bool(rows)
        except Exception:
            return True

    return True
