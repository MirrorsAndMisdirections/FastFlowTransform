# src/fastflowtransform/executors/bigquery_exec.py
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from google.cloud.bigquery import Client, LoadJobConfig

from fastflowtransform.core import Node, relation_for
from fastflowtransform.executors._bigquery_mixin import BigQueryIdentifierMixin
from fastflowtransform.executors._shims import BigQueryConnShim
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.meta import ensure_meta_table, upsert_meta


# ---- Executor --------------------------------------------------------------
class BigQueryExecutor(BigQueryIdentifierMixin, BaseExecutor[pd.DataFrame]):
    """
    BigQuery executor (pandas DataFrames).
    ENV/Profiles typically use:
      - FF_BQ_PROJECT
      - FF_BQ_DATASET
      - FF_BQ_LOCATION (optional)
    """

    def __init__(
        self,
        project: str,
        dataset: str,
        location: str | None = None,
        client: Client | None = None,
    ):
        self.project = project
        self.dataset = dataset
        self.location = location
        self.client: Client = client or bigquery.Client(
            project=self.project, location=self.location
        )
        # Testing-API: con.execute(...)
        self.con = BigQueryConnShim(
            self.client, location=self.location, project=self.project, dataset=self.dataset
        )

    # ---------- Helpers ----------
    # ---------- Python (Frames) ----------
    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> pd.DataFrame:
        q = f"SELECT * FROM {self._qualified_identifier(relation)}"
        try:
            job = self.client.query(q, location=self.location)
            return job.result().to_dataframe(create_bqstorage_client=True)
        except NotFound as e:
            # list existing tables to aid debugging
            tables = list(self.client.list_tables(f"{self.project}.{self.dataset}"))
            existing = [t.table_id for t in tables]
            raise RuntimeError(
                f"Dependency table not found: {self.project}.{self.dataset}.{relation}\n"
                f"Deps: {list(deps)}\nExisting in dataset: {existing}\n"
                "Hinweis: Seeds/Upstream-Modelle erzeugt? DATASET korrekt?"
            ) from e

    def _materialize_relation(self, relation: str, df: pd.DataFrame, node: Node) -> None:
        self._ensure_dataset()
        table_id = f"{self.project}.{self.dataset}.{relation}"
        job_config = LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        # Optionally extend dtype mapping here (NUMERIC/STRING etc.)
        try:
            job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config, location=self.location
            )
            job.result()
        except BadRequest as e:
            raise RuntimeError(f"BigQuery write failed: {table_id}\n{e}") from e

    def _create_view_over_table(self, view_name: str, backing_table: str, node: Node) -> None:
        view_id = self._qualified_identifier(view_name)
        back_id = self._qualified_identifier(backing_table)
        self._ensure_dataset()
        job = self.client.query(
            f"CREATE OR REPLACE VIEW {view_id} AS SELECT * FROM {back_id}",
            location=self.location,
        )
        job.result()

    def _frame_name(self) -> str:
        return "pandas"

    # ---- SQL hooks ----
    def _format_relation_for_ref(self, name: str) -> str:
        return self._qualified_identifier(relation_for(name))

    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        ident = cfg["identifier"]
        proj = cfg.get("project", self.project)
        dset = cfg.get("dataset", self.dataset)
        return self._qualified_identifier(ident, project=proj, dataset=dset)

    def _apply_sql_materialization(
        self, node: Node, target_sql: str, select_body: str, materialization: str
    ) -> None:
        self._ensure_dataset()
        try:
            super()._apply_sql_materialization(node, target_sql, select_body, materialization)
        except BadRequest as e:
            raise RuntimeError(
                f"BigQuery SQL failed for {target_sql}:\n{select_body}\n\n{e}"
            ) from e

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        job = self.client.query(
            f"CREATE OR REPLACE VIEW {target_sql} AS {select_body}",
            location=self.location,
        )
        job.result()

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        job = self.client.query(
            f"CREATE OR REPLACE TABLE {target_sql} AS {select_body}",
            location=self.location,
        )
        job.result()

    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        view_id = self._qualified_identifier(view_name)
        back_id = self._qualified_identifier(backing_table)
        self._ensure_dataset()
        job = self.client.query(
            f"CREATE OR REPLACE VIEW {view_id} AS SELECT * FROM {back_id}",
            location=self.location,
        )
        job.result()

    def on_node_built(self, node: Node, relation: str, fingerprint: str) -> None:
        """
        Write/update dataset._ff_meta after a successful build.
        """
        try:
            ensure_meta_table(self)
            upsert_meta(self, node.name, relation, fingerprint, "bigquery")
        except Exception:
            pass

    # ── Incremental API (parity with DuckDB/PG) ───────────────────────────
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
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        target = self._qualified_identifier(relation, project=self.project, dataset=self.dataset)
        self.client.query(
            f"CREATE TABLE {target} AS {body}",
            location=self.location,
        ).result()

    def incremental_insert(self, relation: str, select_sql: str) -> None:
        """
        INSERT INTO with cleaned SELECT body.
        """
        self._ensure_dataset()
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        target = self._qualified_identifier(relation, project=self.project, dataset=self.dataset)
        self.client.query(
            f"INSERT INTO {target} {body}",
            location=self.location,
        ).result()

    def incremental_merge(self, relation: str, select_sql: str, unique_key: list[str]) -> None:
        """
        Portable fallback without native MERGE:
          - DELETE collisions via WHERE EXISTS against the cleaned SELECT body
          - INSERT new rows from the same body
        """
        self._ensure_dataset()
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        target = self._qualified_identifier(relation, project=self.project, dataset=self.dataset)
        pred = " AND ".join([f"t.{k}=s.{k}" for k in unique_key]) or "FALSE"

        delete_sql = f"""
        DELETE FROM {target} t
        WHERE EXISTS (SELECT 1 FROM ({body}) s WHERE {pred})
        """
        self.client.query(delete_sql, location=self.location).result()

        insert_sql = f"INSERT INTO {target} SELECT * FROM ({body})"
        self.client.query(insert_sql, location=self.location).result()

    def alter_table_sync_schema(
        self, relation: str, select_sql: str, *, mode: str = "append_new_columns"
    ) -> None:
        """
        Best-effort additive schema sync:
          - infer select schema via LIMIT 0 query
          - add missing columns as NULLABLE using inferred BigQuery types
        """
        if mode not in {"append_new_columns", "sync_all_columns"}:
            return
        self._ensure_dataset()

        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
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
        target = self._qualified_identifier(relation, project=self.project, dataset=self.dataset)
        for col in to_add:
            f = out_fields[col]
            typ = str(f.field_type) if hasattr(f, "field_type") else "STRING"
            self.client.query(
                f"ALTER TABLE {target} ADD COLUMN {col} {typ}",
                location=self.location,
            ).result()
