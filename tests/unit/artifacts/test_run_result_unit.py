import json
from pathlib import Path

import pytest

from fastflowtransform.artifacts import RunNodeResult, write_run_results


@pytest.mark.unit
def test_run_results_written(tmp_path: Path):
    started = "2025-01-01T00:00:00+00:00"
    finished = "2025-01-01T00:01:00+00:00"
    results = [
        RunNodeResult(
            name="b.ff",
            status="success",
            started_at=started,
            finished_at=finished,
            duration_ms=1000,
        ),
        RunNodeResult(
            name="a.ff",
            status="skipped",
            started_at=started,
            finished_at=finished,
            duration_ms=0,
            message=None,
        ),
    ]
    p = write_run_results(tmp_path, started_at=started, finished_at=finished, node_results=results)
    data = json.loads(p.read_text(encoding="utf-8"))
    names = [r["name"] for r in data["results"]]
    assert names == ["a.ff", "b.ff"]  # sorted by name
