# Shared helpers for BigQuery-based executors.
from __future__ import annotations

from google.api_core.exceptions import NotFound
from google.cloud import bigquery


class BigQueryIdentifierMixin:
    """
    Mixin that provides common BigQuery helpers (identifier quoting, dataset creation).
    Expect subclasses to define: self.project, self.dataset, self.client.
    """

    project: str
    dataset: str
    client: bigquery.Client

    @staticmethod
    def _bq_quote(value: str) -> str:
        return value.replace("`", "\\`")

    def _qualified_identifier(
        self, relation: str, project: str | None = None, dataset: str | None = None
    ) -> str:
        proj = project or self.project
        dset = dataset or self.dataset
        return f"`{self._bq_quote(proj)}.{self._bq_quote(dset)}.{self._bq_quote(relation)}`"

    def _ensure_dataset(self) -> None:
        ds_id = f"{self.project}.{self.dataset}"
        try:
            self.client.get_dataset(ds_id)
        except NotFound:
            self.client.create_dataset(bigquery.Dataset(ds_id))
