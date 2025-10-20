# src/fastflowtransform/run_executor.py
from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import suppress
from dataclasses import dataclass
from time import perf_counter
from typing import Literal

from .log_queue import LogQueue

FailPolicy = Literal["fail_fast", "keep_going"]


@dataclass
class ScheduleResult:
    per_node_s: dict[str, float]
    total_s: float
    failed: dict[str, BaseException]


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


def _make_task(
    lvl_idx: int,
    before_cb: Callable[..., None] | None,
    run_node: Callable[[str], None],
    per_node: dict[str, float],
    per_node_lock: threading.Lock,
) -> Callable[[str], None]:
    def _task(name: str, _lvl: int = lvl_idx) -> None:  # bindet lvl_idx → kein B023
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
    jobs: int,
    fail_policy: FailPolicy,
    before_cb: Callable[..., None] | None,
    run_node: Callable[[str], None],
    per_node: dict[str, float],
    per_node_lock: threading.Lock,
    failed: dict[str, BaseException],
    logger: LogQueue | None,
    engine_abbr: str,
    name_width: int,
) -> tuple[bool, int, int, int]:
    """Führt eine Ebene aus und loggt. Rückgabe: (had_error, ok_count, fail_count, lvl_ms)."""
    if not names:
        return False, 0, 0, 0

    task = _make_task(lvl_idx, before_cb, run_node, per_node, per_node_lock)
    lvl_t0 = perf_counter()
    ok_in_level = 0
    fail_in_level = 0
    level_had_error = False

    with ThreadPoolExecutor(max_workers=max(1, int(jobs)), thread_name_prefix="ff-worker") as pool:
        futures: dict[Future[None], str] = {}
        for nm in names:
            _log_start(logger, lvl_idx, engine_abbr, nm, name_width)
            futures[pool.submit(task, nm)] = nm

        for fut in as_completed(futures):
            nm = futures[fut]
            try:
                fut.result()
                ok_in_level += 1
                _log_end(
                    logger, lvl_idx, engine_abbr, nm, True, int(per_node[nm] * 1000), name_width
                )
            except BaseException as e:
                level_had_error = True
                failed[nm] = e
                fail_in_level += 1
                ms = int((per_node.get(nm, perf_counter() - lvl_t0)) * 1000)
                _log_end(logger, lvl_idx, engine_abbr, nm, False, ms, name_width)
                if fail_policy == "fail_fast":
                    for f in futures:
                        if not f.done():
                            f.cancel()

    lvl_ms = int((perf_counter() - lvl_t0) * 1000)
    _log_level_summary(logger, lvl_idx, ok_in_level, fail_in_level, lvl_ms)
    return level_had_error, ok_in_level, fail_in_level, lvl_ms


# ----------------- Kompakte Orchestrierung -----------------


def schedule(
    levels: list[list[str]],
    jobs: int,
    fail_policy: FailPolicy,
    run_node: Callable[[str], None],
    before: Callable[..., None] | None = None,
    on_error: Callable[[str, BaseException], None] | None = None,
    logger: LogQueue | None = None,
    engine_abbr: str = "",
    name_width: int = 28,
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
        )
        if had_error:
            if on_error:
                # bereits pro Node best-effort gemeldet; keine Sammelmeldung hier
                pass
            break

    total = perf_counter() - t_total0
    return ScheduleResult(per_node_s=per_node, total_s=total, failed=failed)
