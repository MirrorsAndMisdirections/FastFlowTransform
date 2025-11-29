# fastflowtransform/executors/bigquery/base.py
from __future__ import annotations

from typing import TypeVar

from fastflowtransform.core import Node, relation_for
from fastflowtransform.executors._budget_runner import run_sql_with_budget
from fastflowtransform.executors._shims import BigQueryConnShim
from fastflowtransform.executors._snapshot_sql_mixin import SnapshotSqlMixin
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.executors.bigquery._bigquery_mixin import BigQueryIdentifierMixin
from fastflowtransform.executors.budget import BudgetGuard
from fastflowtransform.executors.query_stats import _TrackedQueryJob
from fastflowtransform.meta import ensure_meta_table, upsert_meta
from fastflowtransform.typing import BadRequest, Client, NotFound, bigquery

TFrame = TypeVar("TFrame")


class BigQueryBaseExecutor(BigQueryIdentifierMixin, SnapshotSqlMixin, BaseExecutor[TFrame]):
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

        def _exec() -> _TrackedQueryJob:
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

        return run_sql_with_budget(
            self,
            sql,
            guard=self._BUDGET_GUARD,
            exec_fn=_exec,
            estimate_fn=self._estimate_query_bytes,
            record_stats=False,
        )

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
    def _this_identifier(self, node: Node) -> str:
        """
        Ensure {{ this }} renders as a fully-qualified identifier so BigQuery
        incremental SQL (e.g., subqueries against {{ this }}) includes project
        and dataset.
        """
        return self._qualify_identifier(relation_for(node.name))

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

    # ---- Snapshot mixin hooks ----
    def _snapshot_prepare_target(self) -> None:
        self._ensure_dataset()

    def _snapshot_target_identifier(self, rel_name: str) -> str:
        return self._qualified_identifier(rel_name)

    def _snapshot_current_timestamp(self) -> str:
        return "CURRENT_TIMESTAMP()"

    def _snapshot_null_timestamp(self) -> str:
        return "CAST(NULL AS TIMESTAMP)"

    def _snapshot_null_hash(self) -> str:
        return "CAST(NULL AS STRING)"

    def _snapshot_hash_expr(self, check_cols: list[str], src_alias: str) -> str:
        concat_expr = self._snapshot_concat_expr(check_cols, src_alias)
        return f"TO_HEX(MD5({concat_expr}))"

    def _snapshot_cast_as_string(self, expr: str) -> str:
        return f"CAST({expr} AS STRING)"

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

    def execute_hook_sql(self, sql: str) -> None:
        """
        Execute one SQL statement for pre/post/on_run hooks.
        """
        self._execute_sql(sql).result()
