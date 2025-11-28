from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from time import perf_counter
from typing import Any

from fastflowtransform.executors.budget import BudgetGuard
from fastflowtransform.executors.query_stats import QueryStats


def run_sql_with_budget(
    executor: Any,
    sql: str,
    *,
    guard: BudgetGuard,
    exec_fn: Callable[[], Any],
    rowcount_extractor: Callable[[Any], int | None] | None = None,
    extra_stats: Callable[[Any], QueryStats | None] | None = None,
    estimate_fn: Callable[[str], int | None] | None = None,
    post_estimate_fn: Callable[[str, Any], int | None] | None = None,
    record_stats: bool = True,
) -> Any:
    """
    Shared helper for guarded SQL execution with timing + stats recording.

    executor      object exposing _apply_budget_guard, _is_budget_guard_active, _record_query_stats
    sql           statement (used for guard + optional estimator)
    exec_fn       callable that executes the statement and returns a result/job handle
    rowcount_extractor(result) -> int|None    best-effort row count (non-negative only)
    extra_stats(result) -> QueryStats|None    allows engines to override/extend stats post-exec
    estimate_fn(sql) -> int|None              optional best-effort bytes estimate when guard
                                              inactive
    post_estimate_fn(sql, result) -> int|None optional post-exec fallback when bytes are still None
    record_stats  set False to skip immediate stats (e.g., when a job handle records on .result())
    """
    estimated_bytes = executor._apply_budget_guard(guard, sql)
    if (
        estimated_bytes is None
        and not executor._is_budget_guard_active()
        and estimate_fn is not None
    ):
        with suppress(Exception):
            estimated_bytes = estimate_fn(sql)

    # If stats should be deferred (BigQuery job handles), just run and return.
    if not record_stats:
        return exec_fn()

    started = perf_counter()
    result = exec_fn()
    duration_ms = int((perf_counter() - started) * 1000)

    rows: int | None = None
    if rowcount_extractor is not None:
        with suppress(Exception):
            rows = rowcount_extractor(result)

    stats = QueryStats(bytes_processed=estimated_bytes, rows=rows, duration_ms=duration_ms)

    if stats.bytes_processed is None and post_estimate_fn is not None:
        with suppress(Exception):
            post_estimate = post_estimate_fn(sql, result)
            if post_estimate is not None:
                stats = QueryStats(
                    bytes_processed=post_estimate,
                    rows=stats.rows,
                    duration_ms=stats.duration_ms,
                )

    if extra_stats is not None:
        with suppress(Exception):
            extra = extra_stats(result)
            if extra:
                stats = QueryStats(
                    bytes_processed=extra.bytes_processed
                    if extra.bytes_processed is not None
                    else stats.bytes_processed,
                    rows=extra.rows if extra.rows is not None else stats.rows,
                    duration_ms=extra.duration_ms
                    if extra.duration_ms is not None
                    else stats.duration_ms,
                )

    executor._record_query_stats(stats)
    return result
