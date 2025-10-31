# tests/helpers/fake_bigquery.py
from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Fake types
# ---------------------------------------------------------------------------


class FakeField:
    def __init__(self, name: str, field_type: str = "STRING"):
        self.name = name
        self.field_type = field_type


class FakeDFResult:
    """
    Wird vom pandas-Executor gebraucht: job.result().to_dataframe(...)
    Wir bauen ein minimales pandas-DataFrame daraus.
    """

    def __init__(self, rows: list[tuple] | None = None, schema: list[FakeField] | None = None):
        self._rows = rows or []
        self._schema = schema or []

    def to_dataframe(self, create_bqstorage_client: bool = True):
        if not self._rows:
            return pd.DataFrame([])
        cols = [f.name for f in self._schema] if self._schema else []
        data = [dict(zip(cols, r, strict=False)) for r in self._rows]
        return pd.DataFrame(data)


class FakeJob:
    def __init__(self, rows: list[Any] | None = None, schema: list[FakeField] | None = None):
        self._rows = rows or []
        self._schema = schema or []

    def result(self):
        return FakeDFResult(self._rows, self._schema)

    @property
    def schema(self):
        return self._schema


class FakeQueryJobConfig:
    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs


class FakeScalarQueryParameter:
    def __init__(self, name: str, typ: str, val: Any):
        self.name = name
        self.type_ = typ
        self.value = val


class FakeDataset:
    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.location: str | None = None


class FakeBadRequest(Exception):
    pass


class FakeNotFound(Exception):
    pass


class FakeClient:
    """
    Gemeinsamer Client für beide Executor-Tests.
    Kann:
      - query(...)
      - list_tables(...)
      - get_table(...)
      - get_dataset(...)
      - create_dataset(...)
      - load_table_from_dataframe(...)  (für pandas-Executor)
    und hat:
      - _datasets: set[str]
      - _tables: dict[str, list[Any]]
    """

    def __init__(self, project: str, location: str | None = None):
        self.project = project
        self.location = location
        self.queries: list[tuple[str, str | None, Any | None]] = []
        self._datasets: set[str] = set()
        self._tables: dict[str, list[Any]] = {}

    # ---- Test helper ----
    def add_dataset(self, ds_id: str) -> None:
        self._datasets.add(ds_id)

    def add_table(self, dataset_id: str, table_id: str) -> None:
        self._tables.setdefault(dataset_id, []).append(SimpleNamespace(table_id=table_id))

    # ---- Emulator methods ----
    def query(self, sql: str, location: str | None = None, job_config: Any | None = None):
        self.queries.append((sql, location, job_config))

        # INFORMATION_SCHEMA → 1 Row back
        if "INFORMATION_SCHEMA.TABLES" in sql or "INFORMATION_SCHEMA.VIEWS" in sql:
            return FakeJob(rows=[(1,)])

        # Probe-Query (SELECT ... WHERE 1=0) → Schema back
        if "WHERE 1=0" in sql:
            return FakeJob(schema=[FakeField("id"), FakeField("new_col", "INT64")])

        # ALTER TABLE ... ADD COLUMN ...
        if sql.lstrip().upper().startswith("ALTER TABLE"):
            return FakeJob()

        # everything else → empty return
        return FakeJob()

    def list_tables(self, dataset_id: str):
        return self._tables.get(dataset_id, [])

    def get_table(self, table_ref: str):
        if table_ref.endswith(".existing"):
            return SimpleNamespace(schema=[FakeField("id")])

        ds = ".".join(table_ref.split(".")[:2])
        name = table_ref.split(".")[-1]
        for t in self._tables.get(ds, []):
            if t.table_id == name:
                return SimpleNamespace(schema=[FakeField("id")])

        raise FakeNotFound(f"table {table_ref} not found")

    def get_dataset(self, ds_id: str):
        if ds_id not in self._datasets:
            raise FakeNotFound(f"dataset {ds_id} not found")
        return FakeDataset(ds_id)

    def create_dataset(self, ds_obj: Any):
        ds_id = getattr(ds_obj, "dataset_id", ds_obj)
        self._datasets.add(ds_id)
        ds = FakeDataset(ds_id)
        ds.location = getattr(ds_obj, "location", None)
        return ds

    def load_table_from_dataframe(self, df, table_id: str, job_config: Any, location: str | None):
        return FakeJob()


class FakeWriteDisposition:
    WRITE_TRUNCATE = "WRITE_TRUNCATE"


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def make_fake_bigquery_module() -> types.ModuleType:
    mod = types.ModuleType("google.cloud.bigquery")
    mod.Client = FakeClient  # type: ignore[attr-defined]
    mod.QueryJobConfig = FakeQueryJobConfig  # type: ignore[attr-defined]
    mod.ScalarQueryParameter = FakeScalarQueryParameter  # type: ignore[attr-defined]
    mod.Dataset = FakeDataset  # type: ignore[attr-defined]
    mod.BadRequest = FakeBadRequest  # type: ignore[attr-defined]
    mod.NotFound = FakeNotFound  # type: ignore[attr-defined]
    mod.WriteDisposition = FakeWriteDisposition  # type: ignore[attr-defined]
    return mod


# ---------------------------------------------------------------------------
# Helper fixture
# ---------------------------------------------------------------------------


def install_fake_bigquery(monkeypatch, target_modules: list[types.ModuleType]) -> types.ModuleType:
    """
    Installiert unser Fake-bigquery sowohl in sys.modules als auch in den angegebenen
    Zielmodulen (per monkeypatch.setattr(mod, "bigquery", ...)).
    Gibt das Fake-Modul zurück.
    """
    fake_bq = make_fake_bigquery_module()

    gc_mod = sys.modules.setdefault("google.cloud", types.ModuleType("google.cloud"))
    gc_mod.bigquery = fake_bq  # type: ignore[attr-defined]
    sys.modules["google.cloud.bigquery"] = fake_bq

    for m in target_modules:
        monkeypatch.setattr(m, "bigquery", fake_bq, raising=True)

    return fake_bq
