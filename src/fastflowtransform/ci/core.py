# fastflowtransform/ci/core.py
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from fastflowtransform.cli.selectors import _compile_selector, _parse_select
from fastflowtransform.core import REGISTRY
from fastflowtransform.dag import topo_sort
from fastflowtransform.errors import DependencyNotFoundError, ModelCycleError
from fastflowtransform.logging import get_logger

logger = get_logger("ci")


@dataclass
class CiIssue:
    """
    A single CI issue (error or warning) discovered during static checks.

    This is intentionally generic enough to be mapped to SARIF, GitHub
    annotations, or plain log lines.
    """

    level: str  # "error" | "warning"
    code: str  # short machine-friendly code, e.g. "GRAPH_CYCLE"
    message: str  # human-readable
    file: str | None = None
    line: int | None = None
    column: int | None = None
    obj_name: str | None = None  # model/source/test name, if applicable


@dataclass
class CiSummary:
    """
    Overall result of a CI check run.
    """

    issues: list[CiIssue]
    selected_nodes: list[str]
    all_nodes: list[str]


def _graph_issues() -> list[CiIssue]:
    """
    Run the same validation logic as your DAG / registry:

    - Missing dependencies → DependencyNotFoundError
    - Cycles → ModelCycleError

    and convert these into CiIssue entries.

    This uses:
      * REGISTRY.nodes
      * fastflowtransform.dag.topo_sort
    """
    issues: list[CiIssue] = []

    nodes = getattr(REGISTRY, "nodes", {}) or {}
    if not nodes:
        # Nothing to validate here
        return issues

    try:
        # topo_sort will:
        #   - raise DependencyNotFoundError for missing deps
        #   - raise ModelCycleError for cycles
        #   - otherwise return a valid ordering
        topo_sort(nodes)
        return issues
    except DependencyNotFoundError as exc:
        # topo_sort raises DependencyNotFoundError(missing_map)
        payload = getattr(exc, "missing", None) or (exc.args[0] if exc.args else None)

        if isinstance(payload, dict):
            for node_name, missing in payload.items():
                if not missing:
                    continue
                msg = f"Missing dependencies for '{node_name}': {', '.join(sorted(missing))}"
                issues.append(
                    CiIssue(
                        level="error",
                        code="MISSING_DEP",
                        message=msg,
                        obj_name=node_name,
                    )
                )
        else:
            # Fallback: just surface the exception message
            issues.append(
                CiIssue(
                    level="error",
                    code="MISSING_DEP",
                    message=str(exc),
                )
            )
        return issues
    except ModelCycleError as exc:
        # topo_sort raises ModelCycleError("Cycle detected among nodes: a, b, c")
        msg = str(exc)
        node_list: list[str] = []
        # Best-effort parse of the node names from the message
        parts = msg.split(":", 1)
        if len(parts) == 2:
            node_list = [p.strip() for p in parts[1].split(",") if p.strip()]

        issues.append(
            CiIssue(
                level="error",
                code="GRAPH_CYCLE",
                message=msg,
                obj_name=",".join(node_list) if node_list else None,
            )
        )
        return issues


def _resolve_selection(
    all_nodes: Sequence[str],
    patterns: Sequence[str] | None,
) -> list[str]:
    """
    Best-effort selection resolution.

    If the CLI selector engine is available, we reuse it so that CI behaves
    like `fft run` (supports tag:, state:, etc.). If that fails, we fall back
    to a simple substring-based filter:

        pattern "foo" matches any node name containing "foo".

    If no patterns are provided, all nodes are considered "selected".
    """
    if not all_nodes:
        return []

    if not patterns:
        return list(all_nodes)

    # --- Prefer the real selector engine used by `fft run` ------------------
    try:
        tokens = _parse_select(list(patterns))
        _, pred = _compile_selector(tokens)

        selected: list[str] = []
        for name in all_nodes:
            node = REGISTRY.nodes.get(name)
            if node is not None and pred(node):
                selected.append(name)

        # Deduplicate while preserving order
        seen: set[str] = set()
        out: list[str] = []
        for n in selected:
            if n not in seen:
                seen.add(n)
                out.append(n)
        return out
    except Exception as exc:  # pragma: no cover - defensive fallback
        logger.warning("CI selector resolution via cli.selectors failed: %s", exc)

    # --- Fallback: simple substring match on node names ----------------------
    selected_simple: list[str] = []
    for name in all_nodes:
        if any(pat in name for pat in patterns):
            selected_simple.append(name)

    seen = set()
    out = []
    for n in selected_simple:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def run_ci_check(
    *,
    select: Sequence[str] | None = None,
) -> CiSummary:
    """
    Run static CI checks against the currently-loaded project.

    Assumptions:
      - REGISTRY.load_project(...) has already been called (via _prepare_context).
      - No database connections are required (purely static).

    Returns:
        CiSummary with issues and selection info.

    Exit codes are handled by the CLI wrapper, based on the returned issues.
    """
    issues: list[CiIssue] = []

    # --- Gather all nodes (models) known to the registry --------------------
    nodes = getattr(REGISTRY, "nodes", {}) or {}
    all_nodes = sorted(nodes.keys())

    if not all_nodes:
        issues.append(
            CiIssue(
                level="warning",
                code="NO_MODELS",
                message="Registry contains no models for this project.",
            )
        )

    # --- Graph sanity checks (missing deps + cycles) ------------------------
    issues.extend(_graph_issues())

    # --- Selection preview ---------------------------------------------------
    selected_nodes = _resolve_selection(all_nodes, select)

    if all_nodes and not selected_nodes:
        issues.append(
            CiIssue(
                level="warning",
                code="SELECTION_EMPTY",
                message="Selection is empty: no nodes would run with the given --select.",
            )
        )

    return CiSummary(
        issues=issues,
        selected_nodes=selected_nodes,
        all_nodes=all_nodes,
    )
