from __future__ import annotations

from typing import Any

from fastflowtransform.hooks.registry import fft_hook
from fastflowtransform.hooks.types import HookContext, RunContext, ModelContext


def _fmt_env(env: dict[str, Any]) -> str:
    """Small helper to format env for logs (defensive against missing keys)."""
    parts = []
    for key in ("FFT_ACTIVE_ENV", "FF_ENGINE", "FF_ENGINE_VARIANT"):
        if key in env:
            parts.append(f"{key}={env[key]}")
    return ", ".join(parts) if parts else "<no FFT_* env>"


@fft_hook(name="python_banner")
def on_run_start(context: HookContext) -> None:
    """
    Example Python run-start hook.

    Context shape (simplified):

        context["when"] -> "on_run_start"
        context["run"]  -> RunContext
        context["env"]  -> dict of env vars
    """
    run: RunContext = context.get("run", {})  # type: ignore[assignment]
    env = context.get("env", {}) or {}

    run_id = run.get("run_id")
    env_name = run.get("env_name")
    engine_name = run.get("engine_name")

    info = _fmt_env(env if isinstance(env, dict) else {})
    print(
        f"[hooks_demo] on_run_start: run_id={run_id} "
        f"(env_name={env_name}, engine={engine_name}; {info})"
    )


@fft_hook(name="python_summary", when="on_run_end")
def on_run_end(context: HookContext) -> None:
    """
    Example Python run-end hook.

    Context shape:

        context["when"]  -> "on_run_end"
        context["run"]   -> RunContext (with status/error)
        context["stats"] -> optional RunStatsContext (if you populate it)
    """
    run: RunContext = context.get("run", {})  # type: ignore[assignment]
    stats = context.get("stats") or {}

    run_id = run.get("run_id")
    status = run.get("status")

    built = stats.get("models_built")
    skipped = stats.get("models_skipped")
    failed = stats.get("models_failed")

    print(
        "[FFT] [hooks_demo] on_run_end: run_id=%s status=%s (built=%s, skipped=%s, failed=%s)"
        % (run_id, status, built, skipped, failed)
    )


@fft_hook(name="model_end_log_python", when="after_model")
def on_model_end(context: HookContext) -> None:
    """
    Example Python model-level hook (called after a model finishes).

    Context shape:

        context["when"]  -> "after_model"
        context["run"]   -> RunContext
        context["model"] -> ModelContext (name, status, rows_affected, ...)
    """
    run: RunContext = context.get("run", {})  # type: ignore[assignment]
    model: ModelContext | None = context.get("model")  # type: ignore[assignment]

    run_id = run.get("run_id")
    model_name = model.get("name") if model else None
    status = model.get("status") if model else None
    rows = model.get("rows_affected") if model else None
    elapsed_ms = model.get("elapsed_ms") if model else None

    print(
        "[FFT] [hooks_demo] on_model_end: run_id=%s model=%s status=%s rows=%s elapsed_ms=%s"
        % (run_id, model_name, status, rows, elapsed_ms)
    )
