# fastflowtransform/executors/postgres.py
import json
from collections.abc import Iterable
from contextlib import suppress
from time import perf_counter
from typing import Any

import pandas as pd
from jinja2 import Environment
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError

from fastflowtransform.core import Node, relation_for
from fastflowtransform.errors import ModelExecutionError, ProfileConfigError
from fastflowtransform.executors._shims import SAConnShim
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.executors.budget import BudgetGuard
from fastflowtransform.executors.query_stats import QueryStats
from fastflowtransform.logging import echo
from fastflowtransform.meta import ensure_meta_table, upsert_meta
from fastflowtransform.snapshots import resolve_snapshot_config


class PostgresExecutor(BaseExecutor[pd.DataFrame]):
    ENGINE_NAME = "postgres"
    _DEFAULT_PG_ROW_WIDTH = 128
    _BUDGET_GUARD = BudgetGuard(
        env_var="FF_PG_MAX_BYTES",
        estimator_attr="_estimate_query_bytes",
        engine_label="Postgres",
        what="query",
    )

    def __init__(self, dsn: str, schema: str | None = None):
        """
        Initialize Postgres executor.

        dsn     e.g.: postgresql+psycopg://user:pass@localhost:5432/dbname
        schema  default schema for reads/writes (also used for search_path)
        """
        if not dsn:
            raise ProfileConfigError(
                "Postgres DSN not set. Hint: profiles.yml → postgres.dsn or env FF_PG_DSN."
            )
        self.engine: Engine = create_engine(dsn, future=True)
        self.schema = schema

        if self.schema:
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self._q_ident(self.schema)}"))
            except SQLAlchemyError as exc:
                raise ProfileConfigError(
                    f"Failed to ensure schema '{self.schema}' exists: {exc}"
                ) from exc

        # ⇣ fastflowtransform.testing expects executor.con.execute("SQL")
        self.con = SAConnShim(self.engine, schema=self.schema)

    def _execute_sql_core(
        self,
        sql: str,
        *args: Any,
        conn: Connection,
        **kwargs: Any,
    ) -> Any:
        """
        Lowest-level SQL executor:

        - sets search_path
        - executes the statement via given connection
        - NO budget guard
        - NO timing / stats

        Used by both the high-level _execute_sql and maintenance helpers.
        """
        self._set_search_path(conn)
        return conn.execute(text(sql), *args, **kwargs)

    def _execute_sql_maintenance(
        self,
        sql: str,
        *args: Any,
        conn: Connection | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Utility/maintenance SQL:

        - sets search_path
        - NO budget guard
        - NO stats

        Intended for:
          - utest cleanup
          - ANALYZE
          - DDL that shouldn't be budget-accounted
        """
        if conn is None:
            with self.engine.begin() as local_conn:
                return self._execute_sql_core(sql, *args, conn=local_conn, **kwargs)
        else:
            return self._execute_sql_core(sql, *args, conn=conn, **kwargs)

    def _execute_sql(
        self,
        sql: str,
        *args: Any,
        conn: Connection | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Central Postgres SQL runner.

        All model-driven SQL in this executor should go through here.

        If `conn` is provided, reuse that connection (important for temp tables /
        snapshots). Otherwise, open a fresh transaction via engine.begin().

        Also records simple per-query stats for run_results.json.
        """
        estimated_bytes = self._apply_budget_guard(self._BUDGET_GUARD, sql)
        if estimated_bytes is None and not self._is_budget_guard_active():
            with suppress(Exception):
                estimated_bytes = self._estimate_query_bytes(sql)
        t0 = perf_counter()

        if conn is None:
            # Standalone use: open our own transaction
            with self.engine.begin() as local_conn:
                result = self._execute_sql_core(sql, *args, conn=local_conn, **kwargs)
        else:
            # Reuse existing connection / transaction (e.g. in run_snapshot_sql)
            result = self._execute_sql_core(sql, *args, conn=conn, **kwargs)

        dt_ms = int((perf_counter() - t0) * 1000)

        # rows: best-effort from Result.rowcount (DML only; SELECT is often -1)
        rows: int | None = None
        try:
            rc = getattr(result, "rowcount", None)
            if isinstance(rc, int) and rc >= 0:
                rows = rc
        except Exception:
            rows = None

        self._record_query_stats(
            QueryStats(
                bytes_processed=estimated_bytes,
                rows=rows,
                duration_ms=dt_ms,
            )
        )
        return result

    def _analyze_relations(
        self,
        relations: Iterable[str],
        conn: Connection | None = None,
    ) -> None:
        """
        Run ANALYZE on the given relations.

        - Never goes through _execute_sql (avoids the budget guard recursion).
        - Uses passed-in conn if given, otherwise opens its own transaction.
        - Best-effort: logs and continues on failure.
        """
        owns_conn = False
        if conn is None:
            conn_ctx = self.engine.begin()
            conn = conn_ctx.__enter__()
            owns_conn = True
        try:
            self._set_search_path(conn)
            for rel in relations:
                try:
                    # If it already looks qualified, leave it; otherwise qualify.
                    qrel = self._qualified(rel) if "." not in rel else rel
                    conn.execute(text(f"ANALYZE {qrel}"))
                except Exception:
                    pass
        finally:
            if owns_conn:
                conn_ctx.__exit__(None, None, None)

    # --- Cost estimation for the shared BudgetGuard -----------------

    def _estimate_query_bytes(self, sql: str) -> int | None:
        """
        Best-effort bytes estimate for a SELECT-ish query using
        EXPLAIN (FORMAT JSON).

        Approximation: estimated_rows * avg_row_width (in bytes).
        Returns None if:
          - the query is not SELECT/CTE
          - EXPLAIN fails
          - the JSON structure is not what we expect
        """
        body = self._extract_select_like(sql)
        lower = body.lstrip().lower()
        if not lower.startswith(("select", "with")):
            # Only try to estimate for read-like queries
            return None

        explain_sql = f"EXPLAIN (FORMAT JSON) {body}"

        try:
            with self.engine.begin() as conn:
                self._set_search_path(conn)
                raw = conn.execute(text(explain_sql)).scalar()
        except Exception:
            return None

        if raw is None:
            return None

        try:
            data = json.loads(raw)
        except Exception:
            data = raw

        # Postgres JSON format: list with a single object
        if isinstance(data, list) and data:
            root = data[0]
        elif isinstance(data, dict):
            root = data
        else:
            return None

        plan = root.get("Plan")
        if not isinstance(plan, dict):
            if isinstance(root, dict) and "Node Type" in root:
                plan = root
            else:
                return None

        return self._estimate_bytes_from_plan(plan)

    def _estimate_bytes_from_plan(self, plan: dict[str, Any]) -> int | None:
        """
        Estimate bytes for the *model output* from the root plan node.

        Approximation: root.Plan Rows * root.Plan Width (or DEFAULT_PG_ROW_WIDTH
        if width is missing).
        """

        def _to_int(node: dict[str, Any], keys: tuple[str, ...]) -> int | None:
            for key in keys:
                val = node.get(key)
                if val is None:
                    continue
                try:
                    return int(val)
                except (TypeError, ValueError):
                    continue
            return None

        rows = _to_int(plan, ("Plan Rows", "Plan_Rows", "Rows"))
        width = _to_int(plan, ("Plan Width", "Plan_Width", "Width"))

        if rows is None and width is None:
            return None

        candidate: int | None

        if rows is not None and width is not None:
            candidate = rows * width
        elif rows is not None:
            candidate = rows * self._DEFAULT_PG_ROW_WIDTH
        else:
            candidate = width

        if candidate is None or candidate <= 0:
            return None

        return int(candidate)

    # --- Helpers ---------------------------------------------------------
    def _q_ident(self, ident: str) -> str:
        # Simple, safe quoting for identifiers
        return '"' + ident.replace('"', '""') + '"'

    def _qualified(self, relname: str, schema: str | None = None) -> str:
        sch = schema or self.schema
        if sch:
            return f"{self._q_ident(sch)}.{self._q_ident(relname)}"
        return self._q_ident(relname)

    def _set_search_path(self, conn: Connection | SAConnShim) -> None:
        if self.schema:
            conn.execute(text(f"SET LOCAL search_path = {self._q_ident(self.schema)}"))

    def _extract_select_like(self, sql_or_body: str) -> str:
        """
        Normalize a SELECT/CTE body:
        - Accept full statements and strip everything before the first WITH/SELECT.
        - Strip trailing semicolons/whitespace.
        """
        s = (sql_or_body or "").lstrip()
        lower = s.lower()
        pos_with = lower.find("with")
        pos_select = lower.find("select")
        if pos_with == -1 and pos_select == -1:
            return s.rstrip(";\n\t ")
        start = min([p for p in (pos_with, pos_select) if p != -1])
        return s[start:].rstrip(";\n\t ")

    # ---------- IO ----------
    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> pd.DataFrame:
        qualified = self._qualified(relation)
        try:
            with self.engine.begin() as conn:
                if self.schema:
                    conn.execute(text(f'SET LOCAL search_path = "{self.schema}"'))
                return pd.read_sql_query(text(f"select * from {qualified}"), conn)
        except ProgrammingError as e:
            raise e

    def _materialize_relation(self, relation: str, df: pd.DataFrame, node: Node) -> None:
        self._write_dataframe_with_stats(relation, df, node)

    def _write_dataframe_with_stats(self, relation: str, df: pd.DataFrame, node: Node) -> None:
        start = perf_counter()
        try:
            df.to_sql(
                relation,
                self.engine,
                if_exists="replace",
                index=False,
                schema=self.schema,
                method="multi",
            )
        except SQLAlchemyError as e:
            raise ModelExecutionError(
                node_name=node.name, relation=self._qualified(relation), message=str(e)
            ) from e
        else:
            self._analyze_relations([relation])
            self._record_dataframe_stats(df, int((perf_counter() - start) * 1000))

    def _record_dataframe_stats(self, df: pd.DataFrame, duration_ms: int) -> None:
        rows = len(df)
        bytes_estimate = int(df.memory_usage(deep=True).sum()) if rows > 0 else 0
        bytes_val = bytes_estimate if bytes_estimate > 0 else None
        self._record_query_stats(
            QueryStats(
                bytes_processed=bytes_val,
                rows=rows if rows > 0 else None,
                duration_ms=duration_ms,
            )
        )

    # ---------- Python view helper ----------
    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        q_view = self._qualified(view_name)
        q_back = self._qualified(backing_table)
        try:
            with self.engine.begin() as conn:
                self._execute_sql(f"DROP VIEW IF EXISTS {q_view} CASCADE", conn=conn)
                self._execute_sql(
                    f"CREATE OR REPLACE VIEW {q_view} AS SELECT * FROM {q_back}", conn=conn
                )

        except Exception as e:
            raise ModelExecutionError(node.name, q_view, str(e)) from e

    def _frame_name(self) -> str:
        return "pandas"

    # ---- SQL hooks ----
    def _format_relation_for_ref(self, name: str) -> str:
        return self._qualified(relation_for(name))

    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        if cfg.get("location"):
            raise NotImplementedError("Postgres executor does not support path-based sources.")

        ident = cfg.get("identifier")
        if not ident:
            raise KeyError(f"Source {source_name}.{table_name} missing identifier")

        relation = self._qualified(ident, schema=cfg.get("schema"))
        database = cfg.get("database") or cfg.get("catalog")
        if database:
            return f"{self._q_ident(database)}.{relation}"
        return relation

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        try:
            self._execute_sql(f"DROP VIEW IF EXISTS {target_sql} CASCADE")
            self._execute_sql(f"CREATE OR REPLACE VIEW {target_sql} AS {select_body}")
        except Exception as e:
            preview = f"-- target={target_sql}\n{select_body}"
            raise ModelExecutionError(node.name, target_sql, str(e), sql_snippet=preview) from e

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        """
        Postgres does NOT support 'CREATE OR REPLACE TABLE'.
        Use DROP TABLE IF EXISTS + CREATE TABLE AS, and accept CTE bodies.
        """
        try:
            self._execute_sql(f"DROP TABLE IF EXISTS {target_sql} CASCADE")
            self._execute_sql(f"CREATE TABLE {target_sql} AS {select_body}")
            self._analyze_relations([target_sql])
        except Exception as e:
            preview = f"-- target={target_sql}\n{select_body}"
            raise ModelExecutionError(node.name, target_sql, str(e), sql_snippet=preview) from e

    # ---------- meta ----------
    def on_node_built(self, node: Node, relation: str, fingerprint: str) -> None:
        """
        Write/update _ff_meta in the current schema after a successful build.
        """
        ensure_meta_table(self)
        upsert_meta(self, node.name, relation, fingerprint, "postgres")

    # ── Incremental API ────────────────────────────────────────────────────
    def exists_relation(self, relation: str) -> bool:
        """
        Return True if a table OR view exists for 'relation' in current schema.
        """
        sql = """
            select 1
            from information_schema.tables
            where table_schema = current_schema()
              and lower(table_name) = lower(:t)
            union all
            select 1
            from information_schema.views
            where table_schema = current_schema()
              and lower(table_name) = lower(:t)
            limit 1
            """

        return bool(self._execute_sql(sql, {"t": relation}).fetchone())

    def create_table_as(self, relation: str, select_sql: str) -> None:
        body = self._extract_select_like(select_sql)
        qrel = self._qualified(relation)
        self._execute_sql(f"create table {qrel} as {body}")
        self._analyze_relations([relation])

    def full_refresh_table(self, relation: str, select_sql: str) -> None:
        """
        Full refresh for incremental fallbacks:
        DROP TABLE IF EXISTS + CREATE TABLE AS.
        """
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        qrel = self._qualified(relation)
        self._execute_sql(f"drop table if exists {qrel}")
        self._execute_sql(f"create table {qrel} as {body}")
        self._analyze_relations([relation])

    def incremental_insert(self, relation: str, select_sql: str) -> None:
        body = self._extract_select_like(select_sql)
        qrel = self._qualified(relation)
        self._execute_sql(f"insert into {qrel} {body}")
        self._analyze_relations([relation])

    def incremental_merge(self, relation: str, select_sql: str, unique_key: list[str]) -> None:
        """
        Portable fallback: staging + delete + insert.
        """
        body = self._extract_select_like(select_sql)
        qrel = self._qualified(relation)
        pred = " AND ".join([f"t.{k}=s.{k}" for k in unique_key])
        self._execute_sql(f"create temporary table ff_stg as {body}")
        try:
            self._execute_sql(f"delete from {qrel} t using ff_stg s where {pred}")
            self._execute_sql(f"insert into {qrel} select * from ff_stg")
            self._analyze_relations([relation])
        finally:
            self._execute_sql("drop table if exists ff_stg")

    def alter_table_sync_schema(
        self, relation: str, select_sql: str, *, mode: str = "append_new_columns"
    ) -> None:
        """
        Add new columns present in SELECT but missing on target (as text).
        """
        body = self._extract_select_like(select_sql)
        qrel = self._qualified(relation)

        with self.engine.begin() as conn:
            # Probe output columns
            cols = [r[0] for r in self._execute_sql(f"select * from ({body}) q limit 0")]

            # Existing columns in target table
            existing = {
                r[0]
                for r in self._execute_sql(
                    """
                    select column_name
                    from information_schema.columns
                    where table_schema = current_schema()
                    and lower(table_name)=lower(:t)
                    """,
                    {"t": relation},
                ).fetchall()
            }

            add = [c for c in cols if c not in existing]
            for c in add:
                self._execute_sql(f'alter table {qrel} add column "{c}" text', conn=conn)

    # ── Snapshot API ──────────────────────────────────────────────────────

    def run_snapshot_sql(self, node: Node, env: Environment) -> None:
        """
        Snapshot materialization for Postgres.

        Config:
          - materialized='snapshot'
          - snapshot={...} and/or top-level strategy/updated_at/check_cols
          - unique_key / primary_key

        Behaviour:
          - First run: create table with one current row per unique key.
          - Subsequent runs:
              * close changed current rows (set valid_to, is_current=false)
              * insert new current rows for new/changed keys.
        """
        if node.kind != "sql":
            raise TypeError(
                f"Snapshot materialization is only supported for SQL models, "
                f"got kind={node.kind!r} for {node.name}."
            )

        meta = getattr(node, "meta", {}) or {}
        if not self._meta_is_snapshot(meta):
            raise ValueError(f"Node {node.name} is not configured with materialized='snapshot'.")

        # Shared normalisation: supports nested 'snapshot={...}' OR flattened config.
        cfg = resolve_snapshot_config(node, meta)
        strategy = cfg.strategy
        unique_key = cfg.unique_key
        updated_at = cfg.updated_at
        check_cols = cfg.check_cols

        # ---- Render SQL and extract SELECT body ----
        sql_rendered = self.render_sql(
            node,
            env,
            ref_resolver=lambda name: self._resolve_ref(name, env),
            source_resolver=self._resolve_source,
        )
        sql = self._strip_leading_config(sql_rendered).strip()
        body = self._selectable_body(sql).rstrip(" ;\n\t")

        rel_name = relation_for(node.name)
        target = self._qualified(rel_name)

        vf = BaseExecutor.SNAPSHOT_VALID_FROM_COL
        vt = BaseExecutor.SNAPSHOT_VALID_TO_COL
        is_cur = BaseExecutor.SNAPSHOT_IS_CURRENT_COL
        hash_col = BaseExecutor.SNAPSHOT_HASH_COL
        upd_meta = BaseExecutor.SNAPSHOT_UPDATED_AT_COL

        # ---- First run: create snapshot table ----
        if not self.exists_relation(rel_name):
            if strategy == "timestamp":
                # valid_from + updated_at come from the source updated_at column
                create_sql = f"""
create table {target} as
select
    s.*,
    s.{updated_at} as {upd_meta},
    s.{updated_at} as {vf},
    cast(null as timestamp) as {vt},
    true as {is_cur},
    cast(null as text) as {hash_col}
from ({body}) as s
"""
            else:  # strategy == "check"
                # Hash over check_cols to detect changes
                col_exprs = [f"coalesce(cast(s.{col} as text), '')" for col in check_cols]
                concat_expr = " || '||' || ".join(col_exprs)
                hash_expr = f"md5({concat_expr})"
                upd_expr = f"s.{updated_at}" if updated_at else "current_timestamp"
                create_sql = f"""
create table {target} as
select
    s.*,
    {upd_expr} as {upd_meta},
    current_timestamp as {vf},
    cast(null as timestamp) as {vt},
    true as {is_cur},
    {hash_expr} as {hash_col}
from ({body}) as s
"""
            self._execute_sql(create_sql)
            return

        # ---- Incremental snapshot update ----

        # Stage current source rows in a temporary table for reuse
        src_name = f"__ff_snapshot_src_{rel_name}".replace(".", "_")
        src_q = self._q_ident(src_name)

        with self.engine.begin() as conn:
            # (Re-)create temp staging table
            self._execute_sql(f"drop table if exists {src_q}", conn=conn)
            self._execute_sql(f"create temporary table {src_q} as {body}", conn=conn)

            # Join predicate on unique keys
            keys_pred = " AND ".join([f"t.{k} = s.{k}" for k in unique_key])

            # Change condition & hash for staging rows
            if strategy == "timestamp":
                change_condition = f"s.{updated_at} > t.{upd_meta}"
                hash_expr_s = "NULL"
                new_upd_expr = f"s.{updated_at}"
                new_valid_from_expr = f"s.{updated_at}"
                new_hash_expr = "NULL"
            else:
                col_exprs_s = [f"coalesce(cast(s.{col} as text), '')" for col in check_cols]
                concat_expr_s = " || '||' || ".join(col_exprs_s)
                hash_expr_s = f"md5({concat_expr_s})"
                change_condition = (
                    f"coalesce({hash_expr_s}, '') <> coalesce(t.{hash_col}::text, '')"
                )
                new_upd_expr = f"s.{updated_at}" if updated_at else "current_timestamp"
                new_valid_from_expr = "current_timestamp"
                new_hash_expr = hash_expr_s

            # 1) Close changed current rows
            close_sql = f"""
update {target} as t
set
    {vt} = current_timestamp,
    {is_cur} = false
from {src_q} as s
where
    {keys_pred}
    and t.{is_cur} = true
    and {change_condition};
"""
            self._execute_sql(close_sql, conn=conn)

            # 2) Insert new current versions (new keys or changed rows)
            first_key = unique_key[0]
            insert_sql = f"""
insert into {target}
select
    s.*,
    {new_upd_expr} as {upd_meta},
    {new_valid_from_expr} as {vf},
    cast(null as timestamp) as {vt},
    true as {is_cur},
    {new_hash_expr} as {hash_col}
from {src_q} as s
left join {target} as t
  on {keys_pred}
  and t.{is_cur} = true
where
    t.{first_key} is null
    or {change_condition};
"""
            self._execute_sql(insert_sql, conn=conn)

            # Temp table will be dropped automatically at end of session; dropping
            # explicitly here is harmless and keeps the connection clean for tests.
            self._execute_sql(f"drop table if exists {src_q}", conn=conn)
            self._analyze_relations([target], conn=conn)

    def snapshot_prune(
        self,
        relation: str,
        unique_key: list[str],
        keep_last: int,
        *,
        dry_run: bool = False,
    ) -> None:
        """
        Delete older snapshot versions while keeping the most recent `keep_last`
        rows per business key (including the current row).
        """
        if keep_last <= 0:
            return

        vf = BaseExecutor.SNAPSHOT_VALID_FROM_COL
        keys = [k for k in unique_key if k]
        if not keys:
            return

        target = self._qualified(relation)
        part_by = ", ".join(keys)
        key_select = ", ".join(keys)

        ranked_sql = f"""
select
  {key_select},
  {vf},
  row_number() over (
    partition by {part_by}
    order by {vf} desc
  ) as rn
from {target}
"""

        if dry_run:
            sql = f"""
with ranked as (
  {ranked_sql}
)
select count(*) as rows_to_delete
from ranked
where rn > {int(keep_last)}
"""
            res = self._execute_sql(sql).fetchone()
            rows = int(res[0]) if res else 0
            echo(
                f"[DRY-RUN] snapshot_prune({relation}): would delete {rows} row(s) "
                f"(keep_last={keep_last})"
            )
            return

        delete_sql = f"""
delete from {target} t
using (
  {ranked_sql}
) r
where
  r.rn > {int(keep_last)}
  and {" AND ".join([f"t.{k} = r.{k}" for k in keys])}
  and t.{vf} = r.{vf};
"""
        self._execute_sql(delete_sql)
        self._analyze_relations([relation])

    def execute_hook_sql(self, sql: str) -> None:
        """
        Execute one or multiple SQL statements for pre/post/on_run hooks.

        Accepts a string that may contain ';'-separated statements.
        """
        self._execute_sql(sql)

    # ---- Unit-test helpers -------------------------------------------------

    def utest_load_relation_from_rows(self, relation: str, rows: list[dict]) -> None:
        """
        Load rows into a Postgres table for unit tests (replace if exists),
        without using pandas.to_sql.
        """
        qualified = self._qualified(relation)

        if not rows:
            # Ensure an empty table exists (corner case).
            try:
                with self.engine.begin() as conn:
                    self._execute_sql_maintenance(
                        f"DROP TABLE IF EXISTS {qualified} CASCADE",
                        conn=conn,
                    )
                    self._execute_sql_maintenance(
                        f"CREATE TABLE {qualified} ()",
                        conn=conn,
                    )
            except SQLAlchemyError as e:
                raise ModelExecutionError(
                    node_name=f"utest::{relation}",
                    relation=self._qualified(relation),
                    message=str(e),
                ) from e
            return

        first = rows[0]
        if not isinstance(first, dict):
            raise ModelExecutionError(
                node_name=f"utest::{relation}",
                relation=self._qualified(relation),
                message=f"Expected list[dict] for rows, got {type(first).__name__}",
            )

        cols = list(first.keys())
        col_list_sql = ", ".join(self._q_ident(c) for c in cols)
        select_exprs = ", ".join(f":{c} AS {self._q_ident(c)}" for c in cols)
        insert_values_sql = ", ".join(f":{c}" for c in cols)

        try:
            with self.engine.begin() as conn:
                # Replace any existing table
                self._execute_sql_maintenance(
                    f"DROP TABLE IF EXISTS {qualified} CASCADE",
                    conn=conn,
                )

                # Create table from first row
                create_sql = f"CREATE TABLE {qualified} AS SELECT {select_exprs}"
                self._execute_sql_maintenance(create_sql, first, conn=conn)

                # Insert remaining rows
                if len(rows) > 1:
                    insert_sql = (
                        f"INSERT INTO {qualified} ({col_list_sql}) VALUES ({insert_values_sql})"
                    )
                    for row in rows[1:]:
                        self._execute_sql_maintenance(insert_sql, row, conn=conn)

        except SQLAlchemyError as e:
            raise ModelExecutionError(
                node_name=f"utest::{relation}",
                relation=self._qualified(relation),
                message=str(e),
            ) from e

    def utest_read_relation(self, relation: str) -> pd.DataFrame:
        """
        Read a relation as a DataFrame for unit-test assertions.
        """
        qualified = self._qualified(relation)
        with self.engine.begin() as conn:
            self._set_search_path(conn)
            return pd.read_sql_query(text(f"select * from {qualified}"), conn)

    def utest_clean_target(self, relation: str) -> None:
        """
        For unit tests: drop any view or table with this name in the configured schema.

        We avoid WrongObjectType by:
          - querying information_schema for existing table/view with this name
          - dropping only the matching kinds.
        """
        with self.engine.begin() as conn:
            # Use the same search_path logic as the rest of the executor
            self._set_search_path(conn)

            # Decide which schema to inspect
            cur_schema = conn.execute(text("select current_schema()")).scalar()
            schema = self.schema or cur_schema

            # Find objects named <relation> in that schema
            info_sql = """
                select kind, table_schema, table_name from (
                  select 'table' as kind, table_schema, table_name
                  from information_schema.tables
                  where lower(table_schema) = lower(:schema)
                    and lower(table_name) = lower(:rel)
                  union all
                  select 'view' as kind, table_schema, table_name
                  from information_schema.views
                  where lower(table_schema) = lower(:schema)
                    and lower(table_name) = lower(:rel)
                ) s
                order by kind;
            """
            rows = conn.execute(
                text(info_sql),
                {"schema": schema, "rel": relation},
            ).fetchall()

            for kind, table_schema, table_name in rows:
                qualified = f'"{table_schema}"."{table_name}"'
                if kind == "view":
                    conn.execute(text(f"DROP VIEW IF EXISTS {qualified} CASCADE"))
                else:  # table
                    conn.execute(text(f"DROP TABLE IF EXISTS {qualified} CASCADE"))
