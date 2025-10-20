# src/fastflowtransform/executors/bigquery_exec.py
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from google.cloud.bigquery import Client, LoadJobConfig

from fastflowtransform.core import Node, relation_for
from fastflowtransform.meta import ensure_meta_table, upsert_meta

from ._bigquery_mixin import BigQueryIdentifierMixin
from ._shims import BigQueryConnShim
from .base import BaseExecutor


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
