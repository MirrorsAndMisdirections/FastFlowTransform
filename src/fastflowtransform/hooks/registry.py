# fastflowtransform/hooks/registry.py

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from fastflowtransform.hooks.types import HookContext

# Registry structure:
#   { (when, name) -> callable }
# where `when` can be a specific phase ("on_run_start") or "*" (wildcard)
_HOOKS: dict[tuple[str, str], Callable[[HookContext], Any]] = {}


def fft_hook(name: str | None = None, when: str | None = None) -> Callable:
    """
    Decorator to register a Python hook.

    Usage:

        from fastflowtransform.hooks.registry import fft_hook

        @fft_hook(name="python_banner")               # no 'when' -> wildcard
        def on_run_start(ctx: dict[str, Any]):
            ...

        @fft_hook(name="python_banner", when="on_run_start")
        def banner_for_run_start(ctx: dict[str, Any]):
            ...

    - `name`: logical hook name (matches project.yml `hooks: ... name:`).
              If omitted, defaults to the function name.
    - `when`: lifecycle event ("on_run_start", "on_run_end",
              "before_model", "after_model", etc.).
              If omitted, the hook is registered for the wildcard phase "*".
    """

    def decorator(fn: Callable[[HookContext], Any]) -> Callable[[HookContext], Any]:
        hook_name = name or fn.__name__
        phase = when or "*"  # wildcard by default

        key = (phase, hook_name)
        if key in _HOOKS:
            raise ValueError(f"Hook already registered for {key!r}")

        _HOOKS[key] = fn
        return fn

    return decorator


def resolve_hook(when: str, name: str) -> Callable[[HookContext], Any]:
    """
    Retrieve a previously-registered hook function.

    Resolution order:
      1. Exact match:   (when, name)
      2. Wildcard match: ('*', name)

    Raises KeyError if not found.
    """
    key = (when, name)
    if key in _HOOKS:
        return _HOOKS[key]

    wildcard_key = ("*", name)
    if wildcard_key in _HOOKS:
        return _HOOKS[wildcard_key]

    raise KeyError(f"No hook registered for when={when!r}, name={name!r}")


def load_project_hooks(project_dir: str | Path) -> None:
    """
    Load all Python files under `<project_dir>/hooks/**.py`.

    This executes the modules (without requiring them to be proper
    Python packages), so any `@fft_hook(...)` calls will populate the
    registry.

    This is intentionally import-path agnostic: we don't require
    `project_dir` to be on sys.path and we don't care about the
    module name outside of this function.
    """
    base = Path(project_dir)
    hooks_dir = base / "hooks"

    if not hooks_dir.is_dir():
        return

    for path in hooks_dir.rglob("*.py"):
        # Build a synthetic, flat module name so we don't rely on package structure
        rel = path.relative_to(base)
        stem_parts = rel.with_suffix("").parts  # e.g. ("hooks", "notify")
        module_name = "_fft_project_hooks_" + "_".join(stem_parts)

        # Skip if already loaded (avoid double-execution)
        if module_name in sys.modules:
            continue

        spec = importlib.util.spec_from_file_location(module_name, path)
        if not spec or not spec.loader:
            continue

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
