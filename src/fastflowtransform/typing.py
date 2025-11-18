# Centralized optional-dependency imports for typing and runtime hints.
# Provides best-effort imports with lightweight fallbacks so modules can
# reference these names without duplicating TYPE_CHECKING blocks.
from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

__all__ = [
    "SDF",
    "SNDF",
    "BFDataFrame",
    "BadRequest",
    "BigQueryOptions",
    "Client",
    "DataType",
    "LoadJobConfig",
    "NotFound",
    "SnowparkSession",
    "SparkAnalysisException",
    "SparkSession",
    "WriteDisposition",
    "bf_global_session",
    "bigframes",
    "bigquery",
]

# --- Google client + exceptions ---
if TYPE_CHECKING:  # pragma: no cover - typing only
    from google.api_core.exceptions import BadRequest, NotFound
else:  # pragma: no cover - runtime import
    try:
        from google.api_core.exceptions import BadRequest, NotFound  # type: ignore
    except Exception:

        class BadRequest(Exception):
            """Fallback when google.api_core is unavailable."""

        class NotFound(Exception):
            """Fallback when google.api_core is unavailable."""


if TYPE_CHECKING:  # pragma: no cover - typing only
    from google.cloud import bigquery
    from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition
else:  # pragma: no cover - runtime import
    try:
        from google.cloud import bigquery  # type: ignore
        from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition  # type: ignore
    except Exception:
        # Minimal stubs so imports don't fail without google installed.
        class _DatasetStub:
            def __init__(self, dataset_id: str):
                self.dataset_id = dataset_id
                self.location: str | None = None

        class _WriteDispositionStub:
            WRITE_TRUNCATE = "WRITE_TRUNCATE"

        class _QueryJobConfigStub:
            def __init__(self, **kwargs: Any):
                self.kwargs = kwargs

        class _ScalarQueryParameterStub:
            def __init__(self, name: str, typ: str, val: Any):
                self.name = name
                self.type_ = typ
                self.value = val

        bigquery = cast(
            Any,
            SimpleNamespace(
                Dataset=_DatasetStub,
                WriteDisposition=_WriteDispositionStub,
                QueryJobConfig=_QueryJobConfigStub,
                ScalarQueryParameter=_ScalarQueryParameterStub,
                LoadJobConfig=lambda **kwargs: SimpleNamespace(**kwargs),
            ),
        )
        Client = Any
        LoadJobConfig = Any
        WriteDisposition = _WriteDispositionStub

# --- BigFrames (BigQuery DataFrames) ---
if TYPE_CHECKING:  # pragma: no cover - typing only
    import bigframes  # Package: google-cloud-bigquery-dataframes
    import bigframes.core.global_session as bf_global_session
    from bigframes._config.bigquery_options import BigQueryOptions
    from bigframes.dataframe import DataFrame as BFDataFrame
else:  # pragma: no cover - runtime import
    try:
        import bigframes  # type: ignore
        import bigframes.core.global_session as bf_global_session  # type: ignore
        from bigframes._config.bigquery_options import (  # type: ignore
            BigQueryOptions as _BFBigQueryOptions,
        )
    except Exception:
        # Provide minimal stubs so imports succeed; tests monkeypatch these.
        class _BFBigQueryOptions:
            def __init__(self, *a: Any, **kw: Any):
                self.args = (a, kw)

        class _SessionStub:
            def __init__(self, *a: Any, **kw: Any):
                self.args = (a, kw)

        class _GlobalSessionContext:
            def __init__(self, session: Any):
                self.session = session

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        bigframes = cast(Any, SimpleNamespace(Session=_SessionStub))
        bf_global_session = cast(Any, SimpleNamespace(_GlobalSessionContext=_GlobalSessionContext))

    BigQueryOptions = _BFBigQueryOptions  # type: ignore[assignment]

    try:
        from bigframes.dataframe import DataFrame as BFDataFrame  # type: ignore
    except Exception:
        BFDataFrame = Any  # type: ignore[assignment]

# --- Spark ---
if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.errors.exceptions.base import AnalysisException as SparkAnalysisException
    from pyspark.sql import DataFrame as SDF, SparkSession
    from pyspark.sql.types import DataType
else:  # pragma: no cover - runtime import
    try:
        from pyspark.sql import DataFrame as SDF, SparkSession  # type: ignore
    except Exception:
        SDF = Any  # type: ignore[assignment]
        SparkSession = Any  # type: ignore[assignment]

    try:
        from pyspark.sql.types import DataType  # type: ignore
    except Exception:
        DataType = Any  # type: ignore[assignment]

    try:
        from pyspark.errors.exceptions.base import (  # type: ignore
            AnalysisException as SparkAnalysisException,
        )
    except Exception:

        class SparkAnalysisException(Exception):
            """Fallback if pyspark is unavailable."""


# --- Snowflake Snowpark ---
if TYPE_CHECKING:  # pragma: no cover - typing only
    from snowflake.snowpark import DataFrame as SNDF, Session as SnowparkSession
else:  # pragma: no cover - runtime import
    try:
        from snowflake.snowpark import DataFrame as SNDF, Session as SnowparkSession  # type: ignore
    except Exception:

        class SnowparkSession:  # type: ignore[misc]
            """Fallback stub when snowflake.snowpark is unavailable."""

            builder = SimpleNamespace(configs=lambda cfg: SimpleNamespace(create=lambda: None))

        class SNDF:  # type: ignore[misc]
            """Fallback Snowpark DataFrame stub."""
