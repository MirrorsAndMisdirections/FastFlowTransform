# fastflowtransform/ci/sarif.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastflowtransform.ci.core import CiIssue, CiSummary


def _issue_to_sarif_result(issue: CiIssue) -> dict[str, Any]:
    """
    Map a CiIssue into a minimal SARIF result record.
    """
    level = "error" if issue.level.lower() == "error" else "warning"

    locations: list[dict[str, Any]] = []
    if issue.file:
        region: dict[str, Any] = {}
        if issue.line is not None:
            region["startLine"] = issue.line
        if issue.column is not None:
            region["startColumn"] = issue.column

        loc: dict[str, Any] = {
            "physicalLocation": {
                "artifactLocation": {"uri": issue.file},
            }
        }
        if region:
            loc["physicalLocation"]["region"] = region
        locations.append(loc)

    return {
        "ruleId": issue.code,
        "level": level,
        "message": {"text": issue.message},
        "locations": locations,
    }


def write_sarif(
    summary: CiSummary,
    path: Path,
    *,
    tool_name: str = "FastFlowTransform CI",
    tool_version: str | None = None,
) -> None:
    """
    Serialize a CiSummary into a SARIF file consumable by GitHub code scanning
    or other tools.

    This is intentionally minimal but standards-compliant enough for basic use.
    """
    results = [_issue_to_sarif_result(issue) for issue in summary.issues]

    driver: dict[str, Any] = {"name": tool_name}
    if tool_version:
        driver["version"] = tool_version

    sarif = {
        "version": "2.1.0",
        "runs": [
            {
                "tool": {"driver": driver},
                "results": results,
            }
        ],
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sarif, indent=2), encoding="utf-8")
