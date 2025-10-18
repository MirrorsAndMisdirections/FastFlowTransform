# src/flowforge/run_executor.py
from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import suppress
from dataclasses import dataclass
from time import perf_counter
from typing import Literal

FailPolicy = Literal["fail_fast", "keep_going"]


@dataclass
class ScheduleResult:
    per_node_s: dict[str, float]
    total_s: float
    failed: dict[str, BaseException]


def schedule(
    levels: list[list[str]],
    jobs: int,
    fail_policy: FailPolicy,
    run_node: Callable[[str], None],
    before: Callable[[str], None] | None = None,
    on_error: Callable[[str, BaseException], None] | None = None,
) -> ScheduleResult:
    """
    Execute the provided levels sequentially; within each level up to `jobs`
    nodes are started in parallel. If a level fails, subsequent levels are not
    scheduled.

    - fail_fast: after the first error, cancel any not-yet-started tasks in the
                 level when possible.
    - keep_going: let all already scheduled tasks within the level finish.

    Returns per-node durations, total runtime, and node-level exceptions.
    """
    jobs = max(1, int(jobs))
    per_node: dict[str, float] = {}
    failed: dict[str, BaseException] = {}
    per_node_lock = threading.Lock()

    t_total0 = perf_counter()

    for lvl in levels:
        if not lvl:
            continue

        # Separate pool per level to guarantee the per-level concurrency limit.
        with ThreadPoolExecutor(max_workers=jobs, thread_name_prefix="ff-worker") as pool:
            futures: dict[Future[None], str] = {}

            def _task(name: str) -> None:
                if before:
                    with suppress(Exception):
                        before(name)
                t0 = perf_counter()
                try:
                    run_node(name)
                finally:
                    # Always record duration, even when an exception is raised
                    dt = perf_counter() - t0
                    with per_node_lock:
                        per_node[name] = dt

            # Schedule tasks
            for name in lvl:
                fut = pool.submit(_task, name)
                futures[fut] = name

            # Evaluate futures; on first error optionally cancel the remainder
            level_had_error = False
            for fut in as_completed(list(futures.keys())):
                name = futures[fut]
                try:
                    fut.result()
                except BaseException as e:
                    level_had_error = True
                    failed[name] = e
                    if on_error:
                        with suppress(Exception):
                            on_error(name, e)
                    if fail_policy == "fail_fast":
                        # Try to cancel all futures that have not started yet
                        for f in futures:
                            if f is fut:
                                continue
                            f.cancel()

            # Only proceed to subsequent levels if the current one succeeded
            if level_had_error:
                break

    total = perf_counter() - t_total0
    return ScheduleResult(per_node_s=per_node, total_s=total, failed=failed)
