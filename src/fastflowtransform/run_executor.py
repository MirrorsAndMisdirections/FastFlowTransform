# src/fastflowtransform/run_executor.py
from __future__ import annotations

import os
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import suppress
from dataclasses import dataclass
from time import perf_counter
from typing import Literal

from fastflowtransform.log_queue import LogQueue

FailPolicy = Literal["fail_fast", "keep_going"]


@dataclass
class ScheduleResult:
    per_node_s: dict[str, float]
    total_s: float
    failed: dict[str, BaseException]


class _NodeFailed(Exception):
    """Wrapper to propagate which node inside a batch failed."""

    def __init__(self, name: str, error: BaseException) -> None:
        super().__init__(str(error))
        self.name = name
        self.error = error


# ----------------- Helpers (außerhalb von `schedule`) -----------------


def _short(name: str, width: int) -> str:
    w = max(5, int(width))
    if len(name) <= w:
        return name
    head = (w - 1) // 2
    tail = w - 1 - head
    return f"{name[:head]}…{name[-tail:]}"


def _log(queue: LogQueue | None, msg: str) -> None:
    if queue:
        queue.put(msg)


def _log_start(
    queue: LogQueue | None, lvl_idx: int, engine_abbr: str, name: str, width: int
) -> None:
    _log(queue, f"▶ L{lvl_idx:02d} [{engine_abbr}] {_short(name, width)}")


def _log_end(
    queue: LogQueue | None, lvl_idx: int, engine_abbr: str, name: str, ok: bool, ms: int, width: int
) -> None:
    mark = "✓" if ok else "✖"
    _log(queue, f"{mark} L{lvl_idx:02d} [{engine_abbr}] {_short(name, width)}  {ms} ms")


def _log_level_summary(queue: LogQueue | None, lvl_idx: int, ok: int, fail: int, ms: int) -> None:
    _log(queue, f"— L{lvl_idx:02d} summary: ok={ok} failed={fail}  {ms} ms")


def _call_before(before_cb: Callable[..., None] | None, name: str, lvl_idx: int) -> None:
    if before_cb is None:
        return
    with suppress(Exception):
        try:
            before_cb(name, lvl_idx)  # neue Arity
        except TypeError:
            before_cb(name)  # Legacy: nur (name)


def _resolve_jobs(
    jobs: int | str,
    level_size: int,
    engine_abbr: str,
) -> int:
    """
    Turn the CLI `jobs` param (int or 'auto') into an effective max_workers
    for a given level.

    Rules:
      - If level is empty → 0 (no executor).
      - If jobs is an int:
          * <= 0 → treated as 1
          * > 0 → min(jobs, level_size)
      - If jobs is 'auto':
          * Base concurrency depends on engine + CPU count.
          * Never exceed level_size.
      - If jobs is an invalid string:
          * Fallback to 1.
    """
    if level_size <= 0:
        return 0

    # numeric → cap at level_size, enforce >=1
    if isinstance(jobs, int):
        if jobs <= 0:
            return 1
        return min(jobs, level_size)

    # string → allow 'auto' or numeric fallback
    text = jobs.strip().lower()
    if text != "auto":
        # try to interpret as integer anyway
        try:
            val = int(text)
        except ValueError:
            return 1
        return _resolve_jobs(val, level_size, engine_abbr)

    # 'auto' mode
    cpu = os.cpu_count() or 4

    # very rough heuristics per engine:
    # - BQ: more conservative; remote service with quotas
    # - SPK: allow a bit higher, it parallelizes internally
    # - others: moderate parallelism
    if engine_abbr == "BQ":
        base = min(cpu, 4)
    elif engine_abbr == "SPK":
        base = min(cpu * 2, 16)
    elif engine_abbr in {"DUCK", "PG", "SNOW"}:
        base = min(cpu, 8)
    else:
        base = cpu

    # never exceed level size, always at least 1
    return max(1, min(base, level_size))


def _partition_groups(
    names: list[str],
    max_workers: int,
    durations_s: dict[str, float] | None,
) -> list[list[str]]:
    """
    Partition `names` into <= max_workers batches.

    If durations are known, use a greedy bin-packing to balance total
    estimated time per batch. Otherwise, fall back to round-robin.
    """
    n = len(names)
    if n == 0:
        return []
    if max_workers <= 1 or n == 1:
        return [names]

    k = min(max_workers, n)

    if not durations_s:
        groups: list[list[str]] = [[] for _ in range(k)]
        for idx, name in enumerate(names):
            groups[idx % k].append(name)
        return groups

    # Greedy by descending duration
    sorted_names = sorted(names, key=lambda nm: durations_s.get(nm, 0.0), reverse=True)
    groups = [[] for _ in range(k)]
    loads: list[float] = [0.0] * k

    for nm in sorted_names:
        j = loads.index(min(loads))
        groups[j].append(nm)
        loads[j] += durations_s.get(nm, 0.0)

    return groups


def _make_task(
    lvl_idx: int,
    before_cb: Callable[..., None] | None,
    run_node: Callable[[str], None],
    per_node: dict[str, float],
    per_node_lock: threading.Lock,
) -> Callable[[str], None]:
    def _task(name: str, _lvl: int = lvl_idx) -> None:
        if before_cb is not None:
            _call_before(before_cb, name, _lvl)
        t0 = perf_counter()
        try:
            run_node(name)
        finally:
            dt = perf_counter() - t0
            with per_node_lock:
                per_node[name] = dt

    return _task


def _run_level(
    lvl_idx: int,
    names: list[str],
    jobs: int | str,
    fail_policy: FailPolicy,
    before_cb: Callable[..., None] | None,
    run_node: Callable[[str], None],
    per_node: dict[str, float],
    per_node_lock: threading.Lock,
    failed: dict[str, BaseException],
    logger: LogQueue | None,
    engine_abbr: str,
    name_width: int,
    name_formatter: Callable[[str], str] | None,
    durations_s: dict[str, float] | None,
) -> tuple[bool, int, int, int]:
    """Executes one level and logs. Returns: (had_error, ok_count, fail_count, lvl_ms)."""
    if not names:
        return False, 0, 0, 0

    # Adaptive per-level worker count (supports '--jobs auto')
    max_workers = _resolve_jobs(jobs, len(names), engine_abbr)
    if max_workers <= 0:
        # no work or something odd → treat as no-op
        return False, 0, 0, 0

    groups = _partition_groups(names, max_workers, durations_s)

    lvl_t0 = perf_counter()
    level_had_error = False

    prev_nodes = set(per_node.keys())
    prev_failed = set(failed.keys())

    def _group_task(group_names: list[str]) -> None:
        for nm in group_names:
            label = name_formatter(nm) if name_formatter else nm
            _log_start(logger, lvl_idx, engine_abbr, label, name_width)
            t0_node = perf_counter()
            try:
                if before_cb is not None:
                    _call_before(before_cb, nm, lvl_idx)
                run_node(nm)
            except BaseException as e:
                dt = perf_counter() - t0_node
                with per_node_lock:
                    per_node[nm] = dt
                _log_end(
                    logger,
                    lvl_idx,
                    engine_abbr,
                    label,
                    False,
                    int(dt * 1000),
                    name_width,
                )
                # propagate which node failed inside the batch
                raise _NodeFailed(nm, e) from e
            else:
                dt = perf_counter() - t0_node
                with per_node_lock:
                    per_node[nm] = dt
                _log_end(
                    logger,
                    lvl_idx,
                    engine_abbr,
                    label,
                    True,
                    int(dt * 1000),
                    name_width,
                )

    with ThreadPoolExecutor(
        max_workers=len(groups),
        thread_name_prefix="ff-worker",
    ) as pool:
        futures: dict[Future[None], list[str]] = {}
        for grp in groups:
            futures[pool.submit(_group_task, grp)] = grp

        for fut in as_completed(futures):
            try:
                fut.result()
            except _NodeFailed as e:
                level_had_error = True
                failed[e.name] = e.error
                if fail_policy == "fail_fast":
                    for f in futures:
                        if not f.done():
                            f.cancel()
            except BaseException as e:
                level_had_error = True
                # No specific node known; attach under a synthetic key
                failed[f"<level-{lvl_idx}>"] = e
                if fail_policy == "fail_fast":
                    for f in futures:
                        if not f.done():
                            f.cancel()

    lvl_ms = int((perf_counter() - lvl_t0) * 1000)
    new_nodes = set(per_node.keys()) - prev_nodes
    new_failed = set(failed.keys()) - prev_failed
    ok_in_level = len(new_nodes - new_failed)
    fail_in_level = len(new_failed)

    _log_level_summary(logger, lvl_idx, ok_in_level, fail_in_level, lvl_ms)
    return level_had_error, ok_in_level, fail_in_level, lvl_ms


# ----------------- Kompakte Orchestrierung -----------------


def schedule(
    levels: list[list[str]],
    jobs: int | str,
    fail_policy: FailPolicy,
    run_node: Callable[[str], None],
    before: Callable[..., None] | None = None,
    on_error: Callable[[str, BaseException], None] | None = None,
    logger: LogQueue | None = None,
    engine_abbr: str = "",
    name_width: int = 28,
    name_formatter: Callable[[str], str] | None = None,
    durations_s: dict[str, float] | None = None,
) -> ScheduleResult:
    """Run levels sequentially; within a level run up to `jobs` nodes in parallel."""
    per_node: dict[str, float] = {}
    failed: dict[str, BaseException] = {}
    per_node_lock = threading.Lock()
    t_total0 = perf_counter()

    for lvl_idx, lvl in enumerate(levels, start=1):
        had_error, _, _, _ = _run_level(
            lvl_idx=lvl_idx,
            names=lvl,
            jobs=jobs,
            fail_policy=fail_policy,
            before_cb=before,
            run_node=run_node,
            per_node=per_node,
            per_node_lock=per_node_lock,
            failed=failed,
            logger=logger,
            engine_abbr=engine_abbr,
            name_width=name_width,
            name_formatter=name_formatter,
            durations_s=durations_s,
        )
        if had_error:
            if on_error:
                # bereits pro Node best-effort gemeldet; keine Sammelmeldung hier
                pass
            break

    total = perf_counter() - t_total0
    return ScheduleResult(per_node_s=per_node, total_s=total, failed=failed)
