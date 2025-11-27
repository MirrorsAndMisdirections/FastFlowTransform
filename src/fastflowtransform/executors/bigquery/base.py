# fastflowtransform/executors/bigquery/base.py
from __future__ import annotations

from typing import Any, TypeVar

from fastflowtransform.core import Node, relation_for
from fastflowtransform.executors._shims import BigQueryConnShim
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.executors.bigquery._bigquery_mixin import BigQueryIdentifierMixin
from fastflowtransform.executors.budget import BudgetGuard
from fastflowtransform.executors.query_stats import _TrackedQueryJob
from fastflowtransform.logging import echo
from fastflowtransform.meta import ensure_meta_table, upsert_meta
from fastflowtransform.snapshots import resolve_snapshot_config
from fastflowtransform.typing import BadRequest, Client, NotFound, bigquery

TFrame = TypeVar("TFrame")


class BigQueryBaseExecutor(BigQueryIdentifierMixin, BaseExecutor[TFrame]):
    """
    Shared BigQuery executor logic (SQL, incremental, meta, DQ helpers).

    Subclasses are responsible for:
      - frame type (pandas / BigFrames / ...)
      - _read_relation()
      - _materialize_relation()
      - _is_frame()
      - _frame_name()
    """

    # Subclasses override ENGINE_NAME ("bigquery", "bigquery_batch", ...)
    ENGINE_NAME = "bigquery_base"
    _BUDGET_GUARD = BudgetGuard(
        env_var="FF_BQ_MAX_BYTES",
        estimator_attr="_estimate_query_bytes",
        engine_label="BigQuery",
        what="query",
    )

    def __init__(
        self,
        project: str,
        dataset: str,
        location: str | None = None,
        client: Client | None = None,
        allow_create_dataset: bool = False,
    ):
        self.project = project
        self.dataset = dataset
        self.location = location
        self.allow_create_dataset = allow_create_dataset
        self.client: Client = client or bigquery.Client(
            project=self.project,
            location=self.location,
        )
        # Testing-API: con.execute(...)
        self.con = BigQueryConnShim(
            self.client,
            location=self.location,
            project=self.project,
            dataset=self.dataset,
        )

    def _execute_sql(self, sql: str) -> _TrackedQueryJob:
        """
        Central BigQuery query runner.

        - All 'real' SQL statements in this executor should go through here.
        - Returns the QueryJob so callers can call .result().
        """
        self._apply_budget_guard(self._BUDGET_GUARD, sql)
        # job = self.client.query(sql, location=self.location)
        job_config = bigquery.QueryJobConfig()
        if self.dataset:
            # Let unqualified tables resolve to project.dataset.table
            job_config.default_dataset = bigquery.DatasetReference(self.project, self.dataset)

        job = self.client.query(
            sql,
            job_config=job_config,
            location=self.location,
        )
        return _TrackedQueryJob(job, on_complete=self._record_query_job_stats)

    # --- Cost estimation for the shared BudgetGuard -----------------

    def _estimate_query_bytes(self, sql: str) -> int | None:
        """
        Estimate bytes for a BigQuery SQL statement using a dry-run.

        Returns the estimated bytes, or None if estimation is not possible.
        """
        cfg = bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False,
        )
        if self.dataset:
            # Let unqualified tables resolve to project.dataset.table
            cfg.default_dataset = bigquery.DatasetReference(self.project, self.dataset)

        job = self.client.query(
            sql,
            job_config=cfg,
            location=self.location,
        )
        # Dry-run is free; we just need the job metadata
        job.result()
        return int(getattr(job, "total_bytes_processed", 0) or 0)

    # ---- DQ test table formatting (fft test) ----
    def _format_test_table(self, table: str | None) -> str | None:
        """
        Ensure tests use fully-qualified BigQuery identifiers in fft test.
        """
        table = super()._format_test_table(table)
        if not isinstance(table, str) or not table.strip():
            return table
        return self._qualified_identifier(table.strip())

    # ---- SQL hooks ----
    def _format_relation_for_ref(self, name: str) -> str:
        return self._qualified_identifier(relation_for(name))

    def _this_identifier(self, node: Node) -> str:
        """
        Ensure {{ this }} renders as a fully-qualified identifier so BigQuery
        incremental SQL (e.g., subqueries against {{ this }}) includes project
        and dataset.
        """
        return self._qualified_identifier(relation_for(node.name))

    def _format_source_reference(
        self,
        cfg: dict[str, Any],
        source_name: str,
        table_name: str,
    ) -> str:
        if cfg.get("location"):
            raise NotImplementedError("BigQuery executor does not support path-based sources.")

        ident = cfg.get("identifier")
        if not ident:
            raise KeyError(f"Source {source_name}.{table_name} missing identifier")

        proj = cfg.get("project") or cfg.get("database") or cfg.get("catalog") or self.project
        dset = cfg.get("dataset") or cfg.get("schema") or self.dataset
        return self._qualified_identifier(ident, project=proj, dataset=dset)

    def _apply_sql_materialization(
        self,
        node: Node,
        target_sql: str,
        select_body: str,
        materialization: str,
    ) -> None:
        self._ensure_dataset()
        try:
            super()._apply_sql_materialization(node, target_sql, select_body, materialization)
        except BadRequest as e:
            raise RuntimeError(
                f"BigQuery SQL failed for {target_sql}:\n{select_body}\n\n{e}"
            ) from e

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        self._execute_sql(f"CREATE OR REPLACE VIEW {target_sql} AS {select_body}").result()

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        self._execute_sql(f"CREATE OR REPLACE TABLE {target_sql} AS {select_body}").result()

    def _create_or_replace_view_from_table(
        self,
        view_name: str,
        backing_table: str,
        node: Node,
    ) -> None:
        view_id = self._qualified_identifier(view_name)
        back_id = self._qualified_identifier(backing_table)
        self._ensure_dataset()
        self._execute_sql(f"CREATE OR REPLACE VIEW {view_id} AS SELECT * FROM {back_id}").result()

    # ---- Meta hook ----
    def on_node_built(self, node: Node, relation: str, fingerprint: str) -> None:
        """
        Write/update dataset._ff_meta after a successful build.
        Both pandas + BigFrames executors use the logical engine key 'bigquery'.
        """
        ensure_meta_table(self)
        upsert_meta(self, node.name, relation, fingerprint, "bigquery")

    # ── Incremental API (shared across BigQuery executors) ───────────────
    def exists_relation(self, relation: str) -> bool:
        """
        Check presence in INFORMATION_SCHEMA for tables/views.
        """
        proj = self.project
        dset = self.dataset
        rel = relation
        q = f"""
        SELECT 1
        FROM `{proj}.{dset}.INFORMATION_SCHEMA.TABLES`
        WHERE LOWER(table_name)=LOWER(@rel)
        UNION ALL
        SELECT 1
        FROM `{proj}.{dset}.INFORMATION_SCHEMA.VIEWS`
        WHERE LOWER(table_name)=LOWER(@rel)
        LIMIT 1
        """
        job = self.client.query(
            q,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("rel", "STRING", rel)]
            ),
            location=self.location,
        )
        return bool(list(job.result()))

    def create_table_as(self, relation: str, select_sql: str) -> None:
        """
        CREATE TABLE AS with cleaned SELECT body (no trailing semicolons).
        """
        self._ensure_dataset()
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        target = self._qualified_identifier(
            relation,
            project=self.project,
            dataset=self.dataset,
        )
        self._execute_sql(f"CREATE TABLE {target} AS {body}").result()

    def incremental_insert(self, relation: str, select_sql: str) -> None:
        """
        INSERT INTO with cleaned SELECT body.
        """
        self._ensure_dataset()
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        target = self._qualified_identifier(
            relation,
            project=self.project,
            dataset=self.dataset,
        )
        self._execute_sql(f"INSERT INTO {target} {body}").result()

    def incremental_merge(self, relation: str, select_sql: str, unique_key: list[str]) -> None:
        """
        Portable fallback without native MERGE:
          - DELETE collisions via WHERE EXISTS against the cleaned SELECT body
          - INSERT new rows from the same body
        """
        self._ensure_dataset()
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        target = self._qualified_identifier(
            relation,
            project=self.project,
            dataset=self.dataset,
        )
        pred = " AND ".join([f"t.{k}=s.{k}" for k in unique_key]) or "FALSE"

        delete_sql = f"""
        DELETE FROM {target} t
        WHERE EXISTS (SELECT 1 FROM ({body}) s WHERE {pred})
        """
        self._execute_sql(delete_sql).result()

        insert_sql = f"INSERT INTO {target} SELECT * FROM ({body})"
        self._execute_sql(insert_sql).result()

    def alter_table_sync_schema(
        self,
        relation: str,
        select_sql: str,
        *,
        mode: str = "append_new_columns",
    ) -> None:
        """
        Best-effort additive schema sync:
          - infer select schema via LIMIT 0 query
          - add missing columns as NULLABLE using inferred BigQuery types
        """
        if mode not in {"append_new_columns", "sync_all_columns"}:
            return
        self._ensure_dataset()

        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")

        # Infer schema using a no-row query (lets BigQuery type the expressions)
        probe = self.client.query(
            f"SELECT * FROM ({body}) WHERE 1=0",
            job_config=bigquery.QueryJobConfig(dry_run=False, use_query_cache=False),
            location=self.location,
        )
        probe.result()
        out_fields = {f.name: f for f in (probe.schema or [])}

        # Existing table schema
        table_ref = f"{self.project}.{self.dataset}.{relation}"
        try:
            tbl = self.client.get_table(table_ref)
        except NotFound:
            return
        existing_cols = {f.name for f in (tbl.schema or [])}

        to_add = [name for name in out_fields if name not in existing_cols]
        if not to_add:
            return

        target = self._qualified_identifier(
            relation,
            project=self.project,
            dataset=self.dataset,
        )
        for col in to_add:
            f = out_fields[col]
            typ = str(f.field_type) if hasattr(f, "field_type") else "STRING"
            self._execute_sql(f"ALTER TABLE {target} ADD COLUMN {col} {typ}").result()

    # ── Snapshots API (shared for pandas + BigFrames) ─────────────────────
    def run_snapshot_sql(self, node: Node, env: Any) -> None:
        """
        Snapshot materialization for BigQuery SQL models.

        Uses the same semantics as the DuckDB/Postgres/Snowflake executors:
          - First run: create table with snapshot metadata columns.
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

        cfg = resolve_snapshot_config(node, meta)
        strategy = cfg.strategy  # "timestamp" | "check"
        unique_key = cfg.unique_key  # list[str]
        updated_at = cfg.updated_at  # str | None
        check_cols = cfg.check_cols  # list[str]

        if not unique_key:
            raise ValueError(f"{node.path}: snapshot models require a non-empty unique_key list.")

        # ---- Render SQL and extract SELECT body ----
        sql_rendered = self.render_sql(
            node,
            env,
            ref_resolver=lambda name: self._resolve_ref(name, env),
            source_resolver=self._resolve_source,
        )
        sql_clean = self._strip_leading_config(sql_rendered).strip()
        body = self._selectable_body(sql_clean).rstrip(" ;\n\t")

        rel_name = relation_for(node.name)
        target = self._qualified_identifier(rel_name)

        vf = BaseExecutor.SNAPSHOT_VALID_FROM_COL
        vt = BaseExecutor.SNAPSHOT_VALID_TO_COL
        is_cur = BaseExecutor.SNAPSHOT_IS_CURRENT_COL
        hash_col = BaseExecutor.SNAPSHOT_HASH_COL
        upd_meta = BaseExecutor.SNAPSHOT_UPDATED_AT_COL

        self._ensure_dataset()

        # ---- First run: create snapshot table ----
        if not self.exists_relation(rel_name):
            if strategy == "timestamp":
                if not updated_at:
                    raise ValueError(
                        f"{node.path}: strategy='timestamp' snapshots require an updated_at column."
                    )
                create_sql = f"""
CREATE TABLE {target} AS
SELECT
  s.*,
  s.{updated_at} AS {upd_meta},
  s.{updated_at} AS {vf},
  CAST(NULL AS TIMESTAMP) AS {vt},
  TRUE AS {is_cur},
  CAST(NULL AS STRING) AS {hash_col}
FROM ({body}) AS s
"""
            else:  # strategy == "check"
                if not check_cols:
                    raise ValueError(
                        f"{node.path}: strategy='check' snapshots require non-empty check_cols."
                    )
                col_exprs = [f"COALESCE(CAST(s.{col} AS STRING), '')" for col in check_cols]
                concat_expr = " || '||' || ".join(col_exprs)
                hash_expr = f"TO_HEX(MD5({concat_expr}))"
                upd_expr = f"s.{updated_at}" if updated_at else "CURRENT_TIMESTAMP()"
                create_sql = f"""
CREATE TABLE {target} AS
SELECT
  s.*,
  {upd_expr} AS {upd_meta},
  CURRENT_TIMESTAMP() AS {vf},
  CAST(NULL AS TIMESTAMP) AS {vt},
  TRUE AS {is_cur},
  {hash_expr} AS {hash_col}
FROM ({body}) AS s
"""
            self._execute_sql(create_sql).result()
            return

        # ---- Incremental snapshot update ----
        keys_pred = " AND ".join([f"t.{k} = s.{k}" for k in unique_key])

        if strategy == "timestamp":
            if not updated_at:
                raise ValueError(
                    f"{node.path}: strategy='timestamp' snapshots require an updated_at column."
                )
            change_condition = f"s.{updated_at} > t.{upd_meta}"
            new_upd_expr = f"s.{updated_at}"
            new_valid_from_expr = f"s.{updated_at}"
            new_hash_expr = "NULL"
        else:
            col_exprs_s = [f"COALESCE(CAST(s.{col} AS STRING), '')" for col in check_cols]
            concat_expr_s = " || '||' || ".join(col_exprs_s)
            hash_expr_s = f"TO_HEX(MD5({concat_expr_s}))"
            change_condition = f"COALESCE({hash_expr_s}, '') <> COALESCE(t.{hash_col}, '')"
            new_upd_expr = f"s.{updated_at}" if updated_at else "CURRENT_TIMESTAMP()"
            new_valid_from_expr = "CURRENT_TIMESTAMP()"
            new_hash_expr = hash_expr_s

        # 1) Close changed current rows
        close_sql = f"""
UPDATE {target} AS t
SET
  {vt} = CURRENT_TIMESTAMP(),
  {is_cur} = FALSE
FROM ({body}) AS s
WHERE
  {keys_pred}
  AND t.{is_cur} = TRUE
  AND {change_condition}
"""
        self._execute_sql(close_sql).result()

        # 2) Insert new current versions (new keys or changed rows)
        first_key = unique_key[0]
        insert_sql = f"""
INSERT INTO {target}
SELECT
  s.*,
  {new_upd_expr} AS {upd_meta},
  {new_valid_from_expr} AS {vf},
  CAST(NULL AS TIMESTAMP) AS {vt},
  TRUE AS {is_cur},
  {new_hash_expr} AS {hash_col}
FROM ({body}) AS s
LEFT JOIN {target} AS t
  ON {keys_pred}
  AND t.{is_cur} = TRUE
WHERE
  t.{first_key} IS NULL
  OR {change_condition}
"""
        self._execute_sql(insert_sql).result()

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

        target = self._qualified_identifier(
            relation,
            project=self.project,
            dataset=self.dataset,
        )
        vf = BaseExecutor.SNAPSHOT_VALID_FROM_COL
        key_select = ", ".join(keys)
        part_by = ", ".join(keys)

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
            job = self.client.query(sql, location=self.location)
            rows = list(job.result())
            count = int(rows[0][0]) if rows else 0

            echo(
                f"[DRY-RUN] snapshot_prune({relation}): would delete {count} row(s) "
                f"(keep_last={keep_last})"
            )
            return

        join_pred = " AND ".join([f"t.{k} = r.{k}" for k in keys])
        delete_sql = f"""
DELETE FROM {target} AS t
WHERE EXISTS (
  WITH ranked AS (
    {ranked_sql}
  )
  SELECT 1
  FROM ranked AS r
  WHERE
    r.rn > {int(keep_last)}
    AND {join_pred}
    AND t.{vf} = r.{vf}
)
"""
        self._execute_sql(delete_sql).result()

    def execute_hook_sql(self, sql: str) -> None:
        """
        Execute one SQL statement for pre/post/on_run hooks.
        """
        self._execute_sql(sql).result()
