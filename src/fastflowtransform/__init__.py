# src/fastflowtransform/__init__.py
"""
FastFlowTransform package entry point.

Expose the package version and a few commonly-used types/APIs for convenience.
Importing here is intentionally lightweight to avoid circular imports with CLI code.
"""

from __future__ import annotations

from ._version import __version__  # re-export for `from fastflowtransform import __version__`

# Optional convenience re-exports (safe, low-risk imports).
# If you prefer a minimal surface, you can remove the block below.
try:
    from fastflowtransform.core import REGISTRY, Node, relation_for
    from fastflowtransform.dag import levels, mermaid, topo_sort
    from fastflowtransform.decorators import dq_test, engine_model, model
    from fastflowtransform.fingerprint import (
        EnvCtx,
        build_env_ctx,
        fingerprint_py,
        fingerprint_sql,
        get_function_source,
        normalized_sources_blob,
    )
except Exception:
    # Keep import-time robustness; the CLI only needs __version__ at import time.
    # Other symbols remain available when modules are importable.
    pass

__all__ = [
    "REGISTRY",
    "EnvCtx",
    "Node",
    "__version__",
    "build_env_ctx",
    "dq_test",
    "engine_model",
    "fingerprint_py",
    "fingerprint_sql",
    "get_function_source",
    "levels",
    "mermaid",
    "model",
    "normalized_sources_blob",
    "relation_for",
    "topo_sort",
]
