# fastflowtransform/hooks/types.py

from __future__ import annotations

from typing import Any, NotRequired, TypedDict


class RunContext(TypedDict, total=False):
    """
    Information about the entire fft run.
    """

    run_id: str
    env_name: str
    engine_name: str
    started_at: str  # ISO timestamp
    status: str | None  # 'success', 'error', ...
    row_count: int | None
    error: str | None  # error message if any


class ModelContext(TypedDict, total=False):
    """
    Information about a specific model execution.
    """

    name: str  # model name
    path: str  # path to the model file
    tags: list[str]  # normalised list of tags
    meta: dict[str, Any]  # raw meta from the model

    status: str | None  # 'success', 'error', ...
    rows_affected: int | None
    elapsed_ms: float | None  # execution time if available
    error: str | None


class RunStatsContext(TypedDict, total=False):
    """
    Aggregate summary for the run (optional, usually on_run_end).
    """

    models_built: int
    models_skipped: int
    models_failed: int
    run_status: str
    rows_total: int
    elapsed_ms_total: int


class ModelStatsContext(TypedDict):
    """
    Per-model stats as reported by the executor (if available).
    """

    rows: int
    bytes_scanned: int
    query_duration_ms: int


class HookContext(TypedDict, total=False):
    """
    Context passed to all Python hooks.

    Keys are the same for all `when` phases, but some may be absent
    depending on what is happening (e.g., `model` is None for run-level
    hooks).
    """

    when: str  # 'on_run_start', 'on_run_end', 'before_model', 'after_model', ...

    run: RunContext
    model: ModelContext | None

    # env vars (usually FF_* / FFT_* etc.). You decide what to put in.
    env: dict[str, str]

    # Optional aggregate stats for on_run_end
    run_stats: NotRequired[RunStatsContext]

    # Optional per-model stats for before_model/after_model hooks
    model_stats: NotRequired[ModelStatsContext]

    params: NotRequired[dict[str, Any]]
