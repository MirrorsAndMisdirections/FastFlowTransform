from __future__ import annotations

import json
from pathlib import Path

import pytest

from fastflowtransform.ci.core import CiIssue, CiSummary
from fastflowtransform.ci.sarif import write_sarif


def _empty_summary(issues: list[CiIssue]) -> CiSummary:
    """
    Helper to construct CiSummary with the required fields used
    by the current implementation (issues, selected_nodes, all_nodes).
    """
    return CiSummary(
        issues=issues,
        selected_nodes=[],  # list[str], not set()
        all_nodes=[],  # list[str], not set()
    )


@pytest.mark.unit
def test_write_sarif_empty_summary(tmp_path: Path) -> None:
    """
    When the summary has no issues, SARIF should still be valid:
    - version 2.1.0
    - 1 run
    - correct tool name
    - empty results list
    """
    summary = _empty_summary([])
    out_path = tmp_path / "sarif" / "empty.sarif.json"

    write_sarif(summary, out_path, tool_name="FastFlowTransform CI", tool_version="1.2.3")

    assert out_path.exists()
    data = json.loads(out_path.read_text(encoding="utf-8"))

    assert data["version"] == "2.1.0"
    assert len(data["runs"]) == 1

    run = data["runs"][0]
    driver = run["tool"]["driver"]

    assert driver["name"] == "FastFlowTransform CI"
    assert driver["version"] == "1.2.3"
    assert run["results"] == []


@pytest.mark.unit
def test_write_sarif_single_error_issue_with_location(tmp_path: Path) -> None:
    """
    A single error issue with file/line/column should be mapped to:
    - level: 'error'
    - ruleId: issue.code
    - message.text: issue.message
    - locations[0].physicalLocation.artifactLocation.uri == file
    - region.startLine / startColumn
    """
    issue = CiIssue(
        code="MISSING_DEP",
        level="error",
        message="Model has missing dependency",
        obj_name="orders.ff",
        file="models/orders.ff.sql",
        line=10,
        column=3,
    )
    summary = _empty_summary([issue])
    out_path = tmp_path / "sarif" / "single_error.sarif.json"

    write_sarif(summary, out_path, tool_name="fft-ci", tool_version=None)

    data = json.loads(out_path.read_text(encoding="utf-8"))
    run = data["runs"][0]

    assert run["tool"]["driver"]["name"] == "fft-ci"
    # No version key when tool_version=None
    assert "version" not in run["tool"]["driver"]

    assert len(run["results"]) == 1
    res = run["results"][0]

    assert res["ruleId"] == "MISSING_DEP"
    assert res["level"] == "error"
    assert res["message"]["text"] == "Model has missing dependency"

    assert len(res["locations"]) == 1
    loc = res["locations"][0]
    phys = loc["physicalLocation"]

    assert phys["artifactLocation"]["uri"] == "models/orders.ff.sql"
    region = phys["region"]
    assert region["startLine"] == 10
    assert region["startColumn"] == 3


@pytest.mark.unit
def test_write_sarif_warn_issue_without_location(tmp_path: Path) -> None:
    """
    A non-error level should be normalized to SARIF 'warning', and
    issues without file/line/column should emit an empty 'locations' list.
    """
    issue = CiIssue(
        code="STYLE",
        level="warn",
        message="Style nit",
        obj_name="customers.ff",
        file=None,
        line=None,
        column=None,
    )
    summary = _empty_summary([issue])
    out_path = tmp_path / "sarif" / "warn_no_location.sarif.json"

    write_sarif(summary, out_path, tool_name="fft-ci")

    data = json.loads(out_path.read_text(encoding="utf-8"))
    run = data["runs"][0]
    res = run["results"][0]

    assert res["ruleId"] == "STYLE"
    # any non-"error" level becomes "warning" for SARIF
    assert res["level"] == "warning"
    assert res["message"]["text"] == "Style nit"
    assert res["locations"] == []
