# src/fastflowtransform/executors/snowflake_snowpark.py
from __future__ import annotations

import json
from collections.abc import Iterable
from contextlib import suppress
from time import perf_counter
from typing import Any, cast

from jinja2 import Environment

from fastflowtransform.core import Node, relation_for
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.executors.budget import BudgetGuard
from fastflowtransform.executors.query_stats import QueryStats
from fastflowtransform.logging import echo
from fastflowtransform.meta import ensure_meta_table, upsert_meta
from fastflowtransform.snapshots import resolve_snapshot_config
from fastflowtransform.typing import SNDF, SnowparkSession as Session


class SnowflakeSnowparkExecutor(BaseExecutor[SNDF]):
    ENGINE_NAME = "snowflake_snowpark"
    """Snowflake executor operating on Snowpark DataFrames (no pandas)."""
    _BUDGET_GUARD = BudgetGuard(
        env_var="FF_SF_MAX_BYTES",
        estimator_attr="_estimate_query_bytes",
        engine_label="Snowflake",
        what="query",
    )

    def __init__(self, cfg: dict):
        # cfg: {account, user, password, warehouse, database, schema, role?}
        self.session = Session.builder.configs(cfg).create()
        self.database = cfg["database"]
        self.schema = cfg["schema"]

        self.allow_create_schema: bool = bool(cfg["allow_create_schema"])
        self._ensure_schema()

        # Provide a tiny testing shim so tests can call executor.con.execute("SQL")
        self.con = _SFCursorShim(self.session)

    # ---------- Cost estimation & central execution ----------

    def _estimate_query_bytes(self, sql: str) -> int | None:
        """
        Best-effort Snowflake bytes estimation.

        Uses `EXPLAIN USING TEXT` and tries to extract a "bytes=<n>"-style
        metric from the textual plan. If parsing fails or Snowflake doesn't
        expose such info, returns None and the guard is effectively disabled.
        """
        try:
            body = self._selectable_body(sql)
        except Exception:
            body = sql

        try:
            rows = self.session.sql(f"EXPLAIN USING JSON {body}").collect()
            if not rows:
                return None

            parts: list[str] = []
            for r in rows:
                try:
                    parts.append(str(r[0]))
                except Exception:
                    as_dict: dict[str, Any] = getattr(r, "asDict", lambda: {})()
                    if as_dict:
                        parts.extend(str(v) for v in as_dict.values())

            plan_text = "\n".join(parts).strip()
            if not plan_text:
                return None

            try:
                plan_data = json.loads(plan_text)
            except Exception:
                return None

            bytes_val = self._extract_bytes_from_plan(plan_data)
            if bytes_val is None or bytes_val <= 0:
                return None
            return bytes_val
        except Exception:
            # Any parsing / EXPLAIN issues → no estimate, guard skipped
            return None

    def _extract_bytes_from_plan(self, plan_data: Any) -> int | None:
        def _to_int(value: Any) -> int | None:
            if value is None:
                return None
            try:
                return int(value)
            except Exception:
                return None

        if isinstance(plan_data, dict):
            global_stats = plan_data.get("GlobalStats") or plan_data.get("globalStats")
            if isinstance(global_stats, dict):
                candidate = _to_int(
                    global_stats.get("bytesAssigned") or global_stats.get("bytes_assigned")
                )
                if candidate:
                    return candidate
            for val in plan_data.values():
                bytes_val = self._extract_bytes_from_plan(val)
                if bytes_val:
                    return bytes_val
        elif isinstance(plan_data, list):
            for item in plan_data:
                bytes_val = self._extract_bytes_from_plan(item)
                if bytes_val:
                    return bytes_val
        return None

    def _execute_sql(self, sql: str) -> SNDF:
        """
        Central Snowflake SQL runner.

        - Returns a Snowpark DataFrame (same as session.sql).
        - Records best-effort query stats for run_results.json.
        """
        estimated_bytes = self._apply_budget_guard(self._BUDGET_GUARD, sql)
        if estimated_bytes is None and not self._is_budget_guard_active():
            with suppress(Exception):
                estimated_bytes = self._estimate_query_bytes(sql)
        t0 = perf_counter()
        df = self.session.sql(sql)
        dt_ms = int((perf_counter() - t0) * 1000)

        # We *don't* call df.count() here - that would execute the query again.
        # For Snowflake we also don't cheaply access bytes/rows here; the cost
        # guard already did a dry-run EXPLAIN if FF_SF_MAX_BYTES is set.
        self._record_query_stats(
            QueryStats(
                bytes_processed=estimated_bytes,
                rows=None,
                duration_ms=dt_ms,
            )
        )
        return df

    # ---------- Helpers ----------
    def _q(self, s: str) -> str:
        return '"' + s.replace('"', '""') + '"'

    def _qualified(self, rel: str) -> str:
        # DATABASE.SCHEMA.TABLE  (no quotes)
        return f"{self.database}.{self.schema}.{rel}"

    def _ensure_schema(self) -> None:
        """
        Best-effort schema creation when allow_create_schema=True.

        Mirrors BigQuery's `_ensure_dataset` behaviour:
        - If the flag is false → do nothing.
        - If true → `CREATE SCHEMA IF NOT EXISTS "DB"."SCHEMA"`.
        """
        if not getattr(self, "allow_create_schema", False):
            return
        if not self.database or not self.schema:
            # Misconfigured; let downstream errors surface naturally.
            return

        db = self._q(self.database)
        sch = self._q(self.schema)
        with suppress(Exception):
            # Fully qualified CREATE SCHEMA is allowed in Snowflake.
            self.session.sql(f"CREATE SCHEMA IF NOT EXISTS {db}.{sch}").collect()
            # Best-effort; permission issues or race conditions shouldn't crash the executor.
            # If the schema truly doesn't exist and we can't create it, later queries will fail
            # with a clearer engine error.

    # ---------- Frame-Hooks ----------
    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> SNDF:
        df = self.session.table(self._qualified(relation))
        # Present a *logical* lowercase schema to Python models:
        lowered = [c.lower() for c in df.schema.names]
        return df.toDF(*lowered)

    def _materialize_relation(self, relation: str, df: SNDF, node: Node) -> None:
        if not self._is_frame(df):
            raise TypeError("Snowpark model must return a Snowpark DataFrame")

        # Normalize to uppercase for storage in Snowflake
        cols = list(df.schema.names)
        upper_cols = [c.upper() for c in cols]
        if cols != upper_cols:
            df = df.toDF(*upper_cols)

        start = perf_counter()
        df.write.save_as_table(self._qualified(relation), mode="overwrite")
        duration_ms = int((perf_counter() - start) * 1000)
        bytes_est = self._estimate_frame_bytes(df)
        self._record_query_stats(
            QueryStats(
                bytes_processed=bytes_est,
                rows=None,
                duration_ms=duration_ms,
            )
        )

    def _estimate_frame_bytes(self, df: SNDF) -> int | None:
        """
        Best-effort bytes estimate for a Snowpark DataFrame.

        Strategy:
        1) Use DataFrame.queries["queries"] (public Snowpark API) to get SQL.
        2) Optionally fall back to df._plan.sql() if queries is missing/empty.
        3) Run our existing _estimate_query_bytes(sql_text).
        """
        try:
            sql_text = self._snowpark_df_sql(df)
            if not isinstance(sql_text, str) or not sql_text.strip():
                return None
            return self._estimate_query_bytes(sql_text)
        except Exception:
            return None

    def _snowpark_df_sql(self, df: Any) -> str | None:
        """
        Extract the main SQL statement for a Snowpark DataFrame.

        Uses the documented public APIs:
        - DataFrame.queries -> {"queries": [sql1, sql2, ...], "post_actions": [...]}
        - Optionally falls back to df._plan.sql() if needed.
        """
        # 1) Primary source: DataFrame.queries
        queries_dict = getattr(df, "queries", None)

        if isinstance(queries_dict, dict):
            queries = queries_dict.get("queries")
            if isinstance(queries, list) and queries:
                # Pick the most likely "main" query.
                # Snowflake examples use queries['queries'][0],
                # but we can be a bit safer and pick the longest non-empty SQL.
                candidates = [q.strip() for q in queries if isinstance(q, str) and q.strip()]
                if candidates:
                    # Heuristic: longest SQL string is usually the main SELECT/CTE.
                    return max(candidates, key=len)

        # 2) Fallback: internal plan (undocumented but widely used)
        plan = getattr(df, "_plan", None)
        if plan is not None:
            # Prefer simplified plan if available
            with suppress(Exception):
                simplify = getattr(plan, "simplify", None)
                if callable(simplify):
                    simplified = simplify()
                    to_sql = getattr(simplified, "sql", None)
                    if callable(to_sql):
                        sql = to_sql()
                        if isinstance(sql, str) and sql.strip():
                            return sql.strip()

            # Raw plan.sql()
            with suppress(Exception):
                to_sql = getattr(plan, "sql", None)
                if callable(to_sql):
                    sql = to_sql()
                    if isinstance(sql, str) and sql.strip():
                        return sql.strip()

        return None

    def _create_view_over_table(self, view_name: str, backing_table: str, node: Node) -> None:
        qv = self._qualified(view_name)
        qb = self._qualified(backing_table)
        self._execute_sql(f"CREATE OR REPLACE VIEW {qv} AS SELECT * FROM {qb}").collect()

    def _validate_required(
        self, node_name: str, inputs: Any, requires: dict[str, set[str]]
    ) -> None:
        if not requires:
            return

        def cols(df: SNDF) -> set[str]:
            # Compare in lowercase to be case-insensitive for Snowflake
            return {c.lower() for c in df.schema.names}

        # Normalize the required sets too
        normalized_requires = {rel: {c.lower() for c in needed} for rel, needed in requires.items()}

        errors: list[str] = []

        if isinstance(inputs, SNDF):
            need = next(iter(normalized_requires.values()), set())
            missing = need - cols(inputs)
            if missing:
                errors.append(f"- missing columns: {sorted(missing)} | have={sorted(cols(inputs))}")
        else:
            for rel, need in normalized_requires.items():
                if rel not in inputs:
                    errors.append(f"- missing dependency key '{rel}'")
                    continue
                missing = need - cols(inputs[rel])
                if missing:
                    errors.append(
                        f"- [{rel}] missing: {sorted(missing)} | have={sorted(cols(inputs[rel]))}"
                    )

        if errors:
            raise ValueError(
                "Required columns check failed for Snowpark model "
                f"'{node_name}'.\n" + "\n".join(errors)
            )

    def _columns_of(self, frame: SNDF) -> list[str]:
        return list(frame.schema.names)

    def _is_frame(self, obj: Any) -> bool:
        # Accept real Snowpark DataFrames and test doubles with a compatible surface.
        schema = getattr(obj, "schema", None)
        return isinstance(obj, SNDF) or (
            schema is not None
            and hasattr(schema, "names")
            and callable(getattr(obj, "collect", None))
        )

    def _frame_name(self) -> str:
        return "Snowpark"

    # ---- SQL hooks ----
    def _format_relation_for_ref(self, name: str) -> str:
        return self._qualified(relation_for(name))

    def _this_identifier(self, node: Node) -> str:
        """
        Identifier for {{ this }} in SQL models.
        Use fully-qualified DB.SCHEMA.TABLE so all build/read/test paths agree.
        """
        return self._qualified(relation_for(node.name))

    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        if cfg.get("location"):
            raise NotImplementedError("Snowflake executor does not support path-based sources.")

        ident = cfg.get("identifier")
        if not ident:
            raise KeyError(f"Source {source_name}.{table_name} missing identifier")

        db = cfg.get("database") or cfg.get("catalog") or self.database
        sch = cfg.get("schema") or self.schema
        if not db or not sch:
            raise KeyError(
                f"Source {source_name}.{table_name} missing database/schema for Snowflake"
            )
        return f"{db}.{sch}.{ident}"

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        self._execute_sql(f"CREATE OR REPLACE VIEW {target_sql} AS {select_body}").collect()

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        self._execute_sql(f"CREATE OR REPLACE TABLE {target_sql} AS {select_body}").collect()

    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        view_id = self._qualified(view_name)
        back_id = self._qualified(backing_table)
        self._execute_sql(f"CREATE OR REPLACE VIEW {view_id} AS SELECT * FROM {back_id}").collect()

    def _format_test_table(self, table: str | None) -> str | None:
        formatted = super()._format_test_table(table)
        if formatted is None:
            return None

        # If it's already qualified (DB.SCHEMA.TABLE) or quoted, leave it alone.
        if "." in formatted or '"' in formatted:
            return formatted

        # Otherwise, treat it as a logical relation name and fully-qualify it
        # with the executor's configured database/schema.
        return self._qualified(formatted)

    # ---- Meta hook ----
    def on_node_built(self, node: Node, relation: str, fingerprint: str) -> None:
        """After successful materialization, upsert _ff_meta (best-effort)."""
        ensure_meta_table(self)
        upsert_meta(self, node.name, relation, fingerprint, "snowflake_snowpark")

    # ── Incremental API (parity with DuckDB/PG) ──────────────────────────
    def exists_relation(self, relation: str) -> bool:
        """Check existence via information_schema.tables."""
        db = self._q(self.database)
        schema_lit = f"'{self.schema.upper()}'"
        rel_lit = f"'{relation.upper()}'"
        q = f"""
        select 1
        from {db}.information_schema.tables
        where upper(table_schema) = {schema_lit}
            and upper(table_name) = {rel_lit}
        limit 1
        """
        try:
            return bool(self._execute_sql(q).collect())
        except Exception:
            return False

    def create_table_as(self, relation: str, select_sql: str) -> None:
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        self._execute_sql(
            f"CREATE OR REPLACE TABLE {self._qualified(relation)} AS {body}"
        ).collect()

    def full_refresh_table(self, relation: str, select_sql: str) -> None:
        """
        Engine-specific full refresh for incremental fallbacks.
        """
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        self._execute_sql(
            f"CREATE OR REPLACE TABLE {self._qualified(relation)} AS {body}"
        ).collect()

    def incremental_insert(self, relation: str, select_sql: str) -> None:
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        self._execute_sql(f"INSERT INTO {self._qualified(relation)} {body}").collect()

    def incremental_merge(self, relation: str, select_sql: str, unique_key: list[str]) -> None:
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        pred = " AND ".join([f"t.{k}=s.{k}" for k in unique_key]) or "FALSE"
        qrel = self._qualified(relation)

        # 1) Delete matching keys
        delete_sql = f"""
        DELETE FROM {qrel} AS t
        USING ({body}) AS s
        WHERE {pred}
        """
        self._execute_sql(delete_sql).collect()

        # 2) Insert all rows from the delta
        insert_sql = f"INSERT INTO {qrel} SELECT * FROM ({body})"
        self._execute_sql(insert_sql).collect()

    def alter_table_sync_schema(
        self, relation: str, select_sql: str, *, mode: str = "append_new_columns"
    ) -> None:
        """
        Best-effort additive schema sync:
        - infer SELECT schema via LIMIT 0
        - add missing columns as STRING
        """
        if mode not in {"append_new_columns", "sync_all_columns"}:
            return

        qrel = self._qualified(relation)

        # Use identifiers in FROM, but *string literals* in WHERE
        db_ident = self._q(self.database)
        schema_lit = self.schema.replace("'", "''")
        rel_lit = relation.replace("'", "''")

        try:
            existing = {
                r[0]
                for r in self._execute_sql(
                    f"""
                    select column_name
                    from {db_ident}.information_schema.columns
                    where upper(table_schema) = upper('{schema_lit}')
                    and upper(table_name)   = upper('{rel_lit}')
                    """
                ).collect()
            }
        except Exception:
            existing = set()

        # Probe SELECT columns
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        probe = self.session.sql(f"SELECT * FROM ({body}) q WHERE 1=0")
        probe_cols = list(probe.schema.names)

        to_add = [c for c in probe_cols if c not in existing]
        if not to_add:
            return

        # Column names are identifiers → _q is correct here
        cols_sql = ", ".join(f"{self._q(c)} STRING" for c in to_add)
        self._execute_sql(f"ALTER TABLE {qrel} ADD COLUMN {cols_sql}").collect()

    # ── Snapshot API ─────────────────────────────────────────────────────
    def run_snapshot_sql(self, node: Node, env: Environment) -> None:
        """
        Snapshot materialization for Snowflake Snowpark.

        Uses the shared snapshot config resolver so all engines share the
        same semantics and validation.
        """
        if node.kind != "sql":
            raise TypeError(
                f"Snapshot materialization is only supported for SQL models, "
                f"got kind={node.kind!r} for {node.name}."
            )

        meta = getattr(node, "meta", {}) or {}
        if not self._meta_is_snapshot(meta):
            raise ValueError(f"Node {node.name} is not configured with materialized='snapshot'.")

        cfg = resolve_snapshot_config(node, meta)

        # Render model SQL and extract the SELECT body
        rendered = self.render_sql(
            node,
            env,
            ref_resolver=lambda name: self._resolve_ref(name, env),
            source_resolver=self._resolve_source,
        )
        sql = self._strip_leading_config(rendered).strip()
        body = self._selectable_body(sql).rstrip(";\n\t ")

        rel_name = relation_for(node.name)
        target = self._qualified(rel_name)

        vf = BaseExecutor.SNAPSHOT_VALID_FROM_COL
        vt = BaseExecutor.SNAPSHOT_VALID_TO_COL
        is_cur = BaseExecutor.SNAPSHOT_IS_CURRENT_COL
        hash_col = BaseExecutor.SNAPSHOT_HASH_COL
        upd_meta = BaseExecutor.SNAPSHOT_UPDATED_AT_COL

        # ---- First run: create snapshot table ----
        if not self.exists_relation(rel_name):
            if cfg.strategy == "timestamp":
                # cfg.updated_at is guaranteed non-None by resolve_snapshot_config
                if cfg.updated_at is None:  # defensive, for type-checkers
                    raise ValueError(
                        "strategy='timestamp' snapshot requires a non-null updated_at column."
                    )

                create_sql = f"""
CREATE OR REPLACE TABLE {target} AS
SELECT
  s.*,
  s.{cfg.updated_at} AS {upd_meta},
  s.{cfg.updated_at} AS {vf},
  CAST(NULL AS TIMESTAMP) AS {vt},
  TRUE AS {is_cur},
  CAST(NULL AS VARCHAR) AS {hash_col}
FROM ({body}) AS s
"""
            else:  # strategy == "check"
                # hash over check_cols to detect changes
                col_exprs = [f"COALESCE(CAST(s.{col} AS VARCHAR), '')" for col in cfg.check_cols]
                concat_expr = " || '||' || ".join(col_exprs) or "''"
                hash_expr = f"CAST(MD5({concat_expr}) AS VARCHAR)"
                upd_expr = (
                    f"s.{cfg.updated_at}" if cfg.updated_at is not None else "CURRENT_TIMESTAMP()"
                )

                create_sql = f"""
CREATE OR REPLACE TABLE {target} AS
SELECT
  s.*,
  {upd_expr} AS {upd_meta},
  CURRENT_TIMESTAMP() AS {vf},
  CAST(NULL AS TIMESTAMP) AS {vt},
  TRUE AS {is_cur},
  {hash_expr} AS {hash_col}
FROM ({body}) AS s
"""
            self._execute_sql(create_sql).collect()
            return

        # ---- Incremental snapshot update ----
        src_name = f"__ff_snapshot_src_{rel_name}".replace(".", "_")

        # Use a temporary view for the current source rows
        self._execute_sql(f"CREATE OR REPLACE TEMPORARY VIEW {src_name} AS {body}").collect()

        try:
            keys_pred = " AND ".join([f"t.{k} = s.{k}" for k in cfg.unique_key]) or "FALSE"

            if cfg.strategy == "timestamp":
                if cfg.updated_at is None:
                    raise ValueError(
                        "strategy='timestamp' snapshot requires a non-null updated_at column."
                    )
                change_condition = f"s.{cfg.updated_at} > t.{upd_meta}"
                hash_expr_s = "NULL"
                new_upd_expr = f"s.{cfg.updated_at}"
                new_valid_from_expr = f"s.{cfg.updated_at}"
                new_hash_expr = "NULL"
            else:
                col_exprs_s = [f"COALESCE(CAST(s.{col} AS VARCHAR), '')" for col in cfg.check_cols]
                concat_expr_s = " || '||' || ".join(col_exprs_s) or "''"
                hash_expr_s = f"CAST(MD5({concat_expr_s}) AS VARCHAR)"
                change_condition = f"COALESCE({hash_expr_s}, '') <> COALESCE(t.{hash_col}, '')"
                new_upd_expr = (
                    f"s.{cfg.updated_at}" if cfg.updated_at is not None else "CURRENT_TIMESTAMP()"
                )
                new_valid_from_expr = "CURRENT_TIMESTAMP()"
                new_hash_expr = hash_expr_s

            # 1) Close changed current rows
            close_sql = f"""
UPDATE {target} AS t
SET
  {vt} = CURRENT_TIMESTAMP(),
  {is_cur} = FALSE
FROM {src_name} AS s
WHERE
  {keys_pred}
  AND t.{is_cur} = TRUE
  AND {change_condition}
"""
            self._execute_sql(close_sql).collect()

            # 2) Insert new current versions (new keys or changed rows)
            first_key = cfg.unique_key[0]
            insert_sql = f"""
INSERT INTO {target}
SELECT
  s.*,
  {new_upd_expr} AS {upd_meta},
  {new_valid_from_expr} AS {vf},
  CAST(NULL AS TIMESTAMP) AS {vt},
  TRUE AS {is_cur},
  {new_hash_expr} AS {hash_col}
FROM {src_name} AS s
LEFT JOIN {target} AS t
  ON {keys_pred}
 AND t.{is_cur} = TRUE
WHERE
  t.{first_key} IS NULL
  OR {change_condition}
"""
            self._execute_sql(insert_sql).collect()
        finally:
            with suppress(Exception):
                self._execute_sql(f"DROP VIEW IF EXISTS {src_name}").collect()

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

        keys = [k for k in unique_key if k]
        if not keys:
            return

        target = self._qualified(relation)
        vf = BaseExecutor.SNAPSHOT_VALID_FROM_COL

        part_by = ", ".join(keys)
        key_select = ", ".join(keys)

        ranked_sql = f"""
SELECT
  {key_select},
  {vf},
  ROW_NUMBER() OVER (
    PARTITION BY {part_by}
    ORDER BY {vf} DESC
  ) AS rn
FROM {target}
"""

        if dry_run:
            sql = f"""
WITH ranked AS (
  {ranked_sql}
)
SELECT COUNT(*) AS rows_to_delete
FROM ranked
WHERE rn > {int(keep_last)}
"""
            res_raw = self._execute_sql(sql).collect()
            # Snowflake returns a list of Row objects; treat them as tuples for typing.
            res = cast("list[tuple[Any, ...]]", res_raw)
            rows = int(res[0][0]) if res else 0

            echo(
                f"[DRY-RUN] snapshot_prune({relation}): would delete {rows} row(s) "
                f"(keep_last={keep_last})"
            )
            return

        delete_sql = f"""
DELETE FROM {target} t
USING (
  {ranked_sql}
) r
WHERE
  r.rn > {int(keep_last)}
  AND {" AND ".join([f"t.{k} = r.{k}" for k in keys])}
  AND t.{vf} = r.{vf}
"""
        self._execute_sql(delete_sql).collect()


# ────────────────────────── local testing shim ───────────────────────────
class _SFCursorShim:
    """Very small shim to expose .execute(...).fetch* for tests."""

    def __init__(self, session: Session):
        self._session = session

    def execute(self, sql: str, params: Any | None = None) -> _SFResult:
        if params:
            # Parametrized SQL not needed in our internal calls
            raise NotImplementedError("Snowflake shim does not support parametrized SQL")
        rows = self._session.sql(sql).collect()

        if rows:
            cols = list(rows[0].asDict().keys())
            as_tuples = [tuple(row.asDict()[c] for c in cols) for row in rows]
        else:
            as_tuples = []

        return _SFResult(as_tuples)


class _SFResult:
    def __init__(self, rows: list[tuple]):
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return self._rows

    def fetchone(self) -> tuple | None:
        return self._rows[0] if self._rows else None
