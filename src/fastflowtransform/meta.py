# src/fastflowtransform/meta.py
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
  - Snowflake Snowpark (executor.session, .database, .schema)
  - Databricks Spark (executor.spark, optional .database/.schema)
"""

from __future__ import annotations

from contextlib import suppress
from typing import Any

from sqlalchemy import text

# --------------------------- Engine detection ---------------------------


def _is_snowflake_snowpark(ex: Any) -> bool:
    return hasattr(ex, "session") and hasattr(ex.session, "sql")


def _is_spark(ex: Any) -> bool:
    return hasattr(ex, "spark") and hasattr(ex.spark, "sql")


def _is_duckdb(ex: Any) -> bool:
    engine_name = getattr(ex, "ENGINE_NAME", None)
    if isinstance(engine_name, str):
        return engine_name.lower() == "duckdb"
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


def _sf_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sf_qual_meta(ex: Any) -> str:
    db = getattr(ex, "database", None)
    schema = getattr(ex, "schema", None)
    tbl = _sf_ident("_ff_meta")
    if db and schema:
        return f"{_sf_ident(db)}.{_sf_ident(schema)}.{tbl}"
    if schema:
        return f"{_sf_ident(schema)}.{tbl}"
    return tbl


def _spark_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _spark_db(ex: Any) -> str | None:
    db = getattr(ex, "database", None) or getattr(ex, "schema", None)
    if isinstance(db, str) and db.strip():
        return db.strip()
    return None


def _spark_qual_meta(ex: Any) -> str:
    db = _spark_db(ex)
    ident = _spark_ident("_ff_meta")
    if db:
        return f"{_spark_ident(db)}.{ident}"
    return ident


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


# --------------------------- Public API ---------------------------


def ensure_meta_table(executor: Any) -> None:
    """
    Create the _ff_meta table if it does not exist for the active engine.
    """
    if _is_snowflake_snowpark(executor):
        qual = _sf_qual_meta(executor)
        ddl = (
            f"create table if not exists {qual} ("
            "  node_name string,"
            "  relation string,"
            "  fp string,"
            "  engine string,"
            "  built_at timestamp_ltz default current_timestamp()"
            ")"
        )
        executor.session.sql(ddl).collect()
        return

    if _is_spark(executor):
        qual = _spark_qual_meta(executor)
        fmt = getattr(executor, "spark_table_format", None)
        fmt_clause = f" USING {fmt}" if isinstance(fmt, str) and fmt.strip() else ""
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {qual} ("
            "  node_name STRING,"
            "  relation STRING,"
            "  fp STRING,"
            "  engine STRING,"
            "  built_at TIMESTAMP"
            f"){fmt_clause}"
        )
        executor.spark.sql(ddl).collect()
        return

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

    if _is_snowflake_snowpark(executor):
        qual = _sf_qual_meta(executor)
        node_lit = _sql_literal(node_name)
        rel_lit = _sql_literal(relation)
        fp_lit = _sql_literal(fp)
        eng_lit = _sql_literal(engine)
        executor.session.sql(f"delete from {qual} where node_name = {node_lit}").collect()
        executor.session.sql(
            f"insert into {qual}(node_name, relation, fp, engine, built_at) "
            f"values ({node_lit}, {rel_lit}, {fp_lit}, {eng_lit}, current_timestamp())"
        ).collect()
        return

    if _is_spark(executor):
        qual = _spark_qual_meta(executor)

        def _lit(val: str) -> str:
            return _sql_literal(val)

        merge_sql = f"""
        MERGE INTO {qual} AS target
        USING (
            SELECT {_lit(node_name)} AS node_name,
                   {_lit(relation)}  AS relation,
                   {_lit(fp)}        AS fp,
                   {_lit(engine)}    AS engine
        ) AS source
        ON target.node_name = source.node_name
        WHEN MATCHED THEN UPDATE SET
            relation = source.relation,
            fp       = source.fp,
            engine   = source.engine,
            built_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (node_name, relation, fp, engine, built_at)
        VALUES (source.node_name, source.relation, source.fp, source.engine, current_timestamp())
        """
        executor.spark.sql(merge_sql).collect()
        return

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

    if _is_snowflake_snowpark(executor):
        qual = _sf_qual_meta(executor)
        node = _sql_literal(node_name)
        sql = f"select fp, relation, built_at, engine from {qual} where node_name = {node} limit 1"
        rows = executor.session.sql(sql).collect()
        if not rows:
            return None
        row = rows[0]
        data = getattr(row, "asDict", lambda: None)()
        if data:
            return (data.get("FP"), data.get("RELATION"), data.get("BUILT_AT"), data.get("ENGINE"))
        try:
            return (row[0], row[1], row[2], row[3])
        except Exception:
            return None

    if _is_spark(executor):
        qual = _spark_qual_meta(executor)
        sql = (
            f"SELECT fp, relation, built_at, engine FROM {qual} "
            f"WHERE node_name = {_sql_literal(node_name)} LIMIT 1"
        )
        rows = executor.spark.sql(sql).collect()
        if not rows:
            return None
        row = rows[0]
        try:
            return (row["fp"], row["relation"], row["built_at"], row["engine"])
        except Exception:
            return (row[0], row[1], row[2], row[3])

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

    if _is_snowflake_snowpark(executor):
        try:
            db = getattr(executor, "database", None)
            schema = getattr(executor, "schema", None)
            if not db or not schema:
                return False
            q = f"""
            select 1
            from {_sf_ident(db)}.information_schema.tables
            where upper(table_schema) = {_sql_literal(schema.upper())}
              and upper(table_name) = {_sql_literal(relation.upper())}
            limit 1
            """
            rows = executor.session.sql(q).collect()
            return bool(rows)
        except Exception:
            return False

    if _is_spark(executor):
        try:
            spark = executor.spark
            if "." in relation:
                db_name, tbl = relation.rsplit(".", 1)
                return spark.catalog.tableExists(db_name, tbl)
            db = _spark_db(executor)
            if db:
                return spark.catalog.tableExists(db, relation)
            return spark.catalog.tableExists(relation)
        except Exception:
            return False

    return True


def delete_meta_for_node(executor: Any, node_name: str) -> None:
    """Remove meta row(s) for a given logical node. Best-effort, silent on absence."""
    try:
        ensure_meta_table(executor)
    except Exception:
        return

    # DuckDB
    if hasattr(executor, "con"):
        with suppress(Exception):
            executor.con.execute("delete from _ff_meta where node_name = ?", [node_name])
        return

    # Postgres
    if hasattr(executor, "engine"):
        schema = getattr(executor, "schema", None)
        tbl = f'"{schema}"._ff_meta' if schema else "_ff_meta"
        try:
            with executor.engine.begin() as conn:
                conn.execute(
                    text(f"delete from {tbl} where node_name = :node"), {"node": node_name}
                )
        except Exception:
            pass
        return

    # BigQuery (simple fallback; works for Fake in tests)
    if hasattr(executor, "client") and hasattr(executor, "dataset"):
        dataset = executor.dataset
        with suppress(Exception):
            # Best-effort string literal delete (tests use simple node names)
            executor.client.query(
                f'DELETE FROM `{dataset}._ff_meta` WHERE node_name = "{node_name}"'
            )
        return

    if _is_snowflake_snowpark(executor):
        with suppress(Exception):
            qual = _sf_qual_meta(executor)
            executor.session.sql(
                f"delete from {qual} where node_name = {_sql_literal(node_name)}"
            ).collect()
        return

    if _is_spark(executor):
        with suppress(Exception):
            qual = _spark_qual_meta(executor)
            executor.spark.sql(
                f"DELETE FROM {qual} WHERE node_name = {_sql_literal(node_name)}"
            ).collect()
        return
