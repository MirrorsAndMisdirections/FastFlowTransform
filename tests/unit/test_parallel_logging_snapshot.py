from __future__ import annotations

import re
import time

from fastflowtransform.log_queue import LogQueue
from fastflowtransform.run_executor import ScheduleResult, schedule


def _normalize(lines: list[str]) -> list[str]:
    """Replace durations with <ms> to get stable snapshots."""
    out: list[str] = []
    for line in lines:
        s = re.sub(r"\b\d+(?:\.\d+)?\s*ms\b", "<ms>", line)
        out.append(s)
    return out


def test_logging_snapshot_single_level_order_and_summary():
    levels = [["a", "b"]]
    logq = LogQueue()

    def run_node(name: str) -> None:
        # b finishes first
        time.sleep(0.01 if name == "b" else 0.03)

    res: ScheduleResult = schedule(
        levels,
        jobs=2,
        fail_policy="keep_going",
        run_node=run_node,
        logger=logq,
        engine_abbr="DUCK",
        name_width=12,
    )
    assert not res.failed

    lines = _normalize(logq.drain())
    # Expect: starts for a,b (order of starts), then end for b, end for a, summary
    # Starts may be enqueued quickly; both should appear before any ✓
    # Concrete pattern check:
    assert lines[0].startswith("▶ L01 [DUCK] ")
    assert lines[1].startswith("▶ L01 [DUCK] ")
    assert lines[-1].startswith("— L01 summary: ok=2 failed=0  <ms>")
    # Ends contain ✓ and <ms>
    assert any(line.startswith("✓ L01 [DUCK] b") for line in lines)
    assert any(line.startswith("✓ L01 [DUCK] a") for line in lines)


def test_logging_snapshot_multi_level_and_long_names():
    levels = [["very_long_model_name_exceeding_width.ff"], ["next"]]
    logq = LogQueue()

    def run_node(name: str) -> None:
        time.sleep(0.002)

    res = schedule(
        levels,
        jobs=1,
        fail_policy="keep_going",
        run_node=run_node,
        logger=logq,
        engine_abbr="PG",
        name_width=16,  # force ellipsis
    )
    assert not res.failed
    lines = _normalize(logq.drain())
    # Ellipsis present
    assert "…" in lines[0]
    assert lines[0].startswith("▶ L01 [PG] ")
    assert lines[-1].startswith("— L02 summary: ok=1 failed=0  <ms>")
