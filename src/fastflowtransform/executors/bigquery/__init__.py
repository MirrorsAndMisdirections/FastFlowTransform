from __future__ import annotations

import importlib
from typing import Any

_EXECUTORS: dict[str, tuple[str, str, str]] = {
    "BigQueryBaseExecutor": (
        "fastflowtransform.executors.bigquery.base",
        "BigQueryBaseExecutor",
        "bigquery",
    ),
    "BigQueryExecutor": (
        "fastflowtransform.executors.bigquery.pandas",
        "BigQueryExecutor",
        "bigquery",
    ),
    "BigQueryBFExecutor": (
        "fastflowtransform.executors.bigquery.bigframes",
        "BigQueryBFExecutor",
        "bigquery_bf",
    ),
}

__all__: list[str] = list(_EXECUTORS.keys())  # pyright: ignore[reportUnsupportedDunderAll]


def _load_executor(name: str) -> Any:
    module_path, attr, extra = _EXECUTORS[name]
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:  # pragma: no cover - import guard
        if exc.name and exc.name.split(".")[0] in {"google", "bigframes"}:
            raise ImportError(
                f"{name} requires the optional dependency set '{extra}'. "
                f"Install it with `pip install fastflowtransform[{extra}]`."
            ) from exc
        raise
    return getattr(module, attr)


def __getattr__(name: str) -> Any:  # pragma: no cover - import guard
    if name in _EXECUTORS:
        return _load_executor(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:  # pragma: no cover - import guard
    return sorted(list(globals().keys()) + list(_EXECUTORS.keys()))
