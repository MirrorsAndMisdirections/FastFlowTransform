# fastflowtransform/ci/changed_since.py
from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping
from pathlib import Path

from fastflowtransform.core import REGISTRY, Node
from fastflowtransform.logging import get_logger

logger = get_logger("ci.changed_since")


def get_changed_models(project_dir: Path, git_ref: str) -> set[str]:
    """
    Return the set of model names (REGISTRY.nodes keys) whose model files
    changed since `git_ref`.

    We look at:
      - models/**/*.ff.sql
      - models/**/*.ff.py

    Anything else is currently ignored. If git is unavailable or the repo is
    not initialized, we log a warning and return an empty set.

    For demos/CI you can override detection via the FF_CI_CHANGED_MODELS
    environment variable:

        FF_CI_CHANGED_MODELS="models/a.ff.sql,models/b.ff.sql"  # paths or names

    In that case we treat the provided comma-separated tokens as changed
    *model names* and skip git entirely.
    """
    # --- Demo/CI override -----------------------------------------------
    override = os.getenv("FF_CI_CHANGED_MODELS")
    if override:
        # Allow either bare model names ("a.ff") or file-ish tokens;
        # we just strip whitespace and ignore empties.
        tokens = [tok.strip() for tok in override.split(",")]
        return {t for t in tokens if t}

    # --- Normal git-based detection -------------------------------------
    project_dir = project_dir.resolve()

    try:
        proc = subprocess.run(
            ["git", "diff", "--name-only", git_ref, "HEAD", "--", "."],
            cwd=project_dir,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:  # pragma: no cover (env-specific)
        logger.warning("Unable to run 'git diff' for --changed-since=%s: %s", git_ref, exc)
        return set()

    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        if stderr:
            logger.warning(
                "git diff --name-only %s...HEAD failed (exit %s): %s",
                git_ref,
                proc.returncode,
                stderr,
            )
        else:
            logger.warning(
                "git diff --name-only %s...HEAD failed (exit %s)",
                git_ref,
                proc.returncode,
            )
        return set()

    node_names = set(REGISTRY.nodes.keys())
    changed_models: set[str] = set()

    for line in proc.stdout.splitlines():
        rel = line.strip()
        if not rel:
            continue

        # Normalize to POSIX-style paths for prefix checks
        rel_posix = Path(rel).as_posix()

        # Only treat model files as model changes
        if not rel_posix.startswith("models/"):
            continue

        if not (rel_posix.endswith(".ff.sql") or rel_posix.endswith(".ff.py")):
            continue

        stem = Path(rel_posix).stem  # e.g. "customers.ff"
        if stem in node_names:
            changed_models.add(stem)

    if not changed_models:
        logger.info(
            "No model files under 'models/' changed since %s (based on git diff).",
            git_ref,
        )

    return changed_models


def compute_affected_models(
    changed: set[str],
    nodes: Mapping[str, Node],
) -> set[str]:
    """
    Given a set of changed model names, return the transitive closure of all
    affected models:

      - all changed models
      - all their upstream dependencies
      - all their downstream dependents

    Only model dependencies (REGISTRY.nodes edges) are considered; sources are
    ignored for this calculation.
    """
    if not changed:
        return set()

    # Build adjacency maps
    upstream: dict[str, set[str]] = {}
    downstream: dict[str, set[str]] = {name: set() for name in nodes}

    for name, node in nodes.items():
        deps = {d for d in (node.deps or []) if d in nodes}
        upstream[name] = deps
        for dep in deps:
            downstream.setdefault(dep, set()).add(name)

    affected: set[str] = set(changed)

    # Upstream (ancestors)
    stack = list(changed)
    while stack:
        cur = stack.pop()
        for parent in upstream.get(cur, ()):
            if parent not in affected:
                affected.add(parent)
                stack.append(parent)

    # Downstream (descendants)
    stack = list(changed)
    while stack:
        cur = stack.pop()
        for child in downstream.get(cur, ()):
            if child not in affected:
                affected.add(child)
                stack.append(child)

    return affected
