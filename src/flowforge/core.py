# flowforge/core.py
from __future__ import annotations

import importlib.util
import types
import jinja2.runtime
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib.machinery import ModuleSpec
from pathlib import Path
from typing import Any, cast

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from .errors import DependencyNotFoundError, ModuleLoadError


@dataclass
class Node:
    name: str
    kind: str  # "sql" | "python"
    path: Path
    deps: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)


class Registry:
    def __init__(self):
        self.nodes: dict[str, Node] = {}
        self.py_funcs: dict[str, Callable] = {}
        self.project_dir: Path | None = None
        self.env = None
        self.sources: dict[str, dict[str, Any]] = {}
        self.py_requires: dict[str, dict[str, set[str]]] = {}
        self.macros: dict[str, Path] = {}  # macro_name -> file path
        self.project_vars: dict[str, Any] = {}   # project.yml: vars
        self.cli_vars: dict[str, Any] = {}       # CLI --vars overrides

    def get_project_dir(self) -> Path:
        """Return the project directory after load_project(), or raise if not set."""
        if self.project_dir is None:
            raise RuntimeError("Project directory not initialized. Call load_project() first.")
        return self.project_dir
    
    def get_env(self) -> Environment:
        """Return the initialized Jinja Environment, or raise if not loaded."""
        if self.env is None:
            raise RuntimeError("Jinja environment not initialized. Call load_project() first.")
        return self.env
    
    def get_node(self, name: str) -> Node:
        # exact match
        n = self.nodes.get(name)
        if n:
            return n
        # common aliases
        if name.endswith(".ff") and name in self.nodes:
            return self.nodes[name]
        alt = f"{name}.ff"
        n = self.nodes.get(alt)
        if n:
            return n
        raise KeyError(name)
    
    def set_cli_vars(self, overrides: dict[str, Any]) -> None:
        """Set CLI --vars overrides (highest precedence)."""
        self.cli_vars = dict(overrides or {})

    def load_project(self, project_dir: Path):
        self.nodes.clear()
        self.py_funcs.clear()
        self.py_requires.clear()
        self.sources = {}
        self.project_vars = {}
        self.cli_vars = {}
        self.macros.clear()

        self.project_dir = project_dir
        models_dir = project_dir / "models"
        self.env = Environment(
            loader=FileSystemLoader(str(models_dir)),
            undefined=StrictUndefined,
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Make sure macros are available to all templates before model discovery.
        self._load_macros(models_dir)

        # load sources
        src_path = project_dir / "sources.yml"
        self.sources = (
            yaml.safe_load(src_path.read_text(encoding="utf-8")) if src_path.exists() else {}
        )
        self.sources = self.sources or {}

        # load project.yml (vars)
        proj_path = project_dir / "project.yml"
        if proj_path.exists():
            proj_cfg = yaml.safe_load(proj_path.read_text(encoding="utf-8")) or {}
            self.project_vars = dict(proj_cfg.get("vars", {}) or {})

        # discover models
        for p in models_dir.rglob("*.ff.sql"):
            name = p.stem
            deps = self._scan_sql_deps(p)
            meta = self._parse_model_config(p)
            self._add_node_or_fail(name, "sql", p, deps, meta=meta)
            
        for p in models_dir.rglob("*.ff.py"):
            mod = self._load_py_module(p)
            for _, func in list(self.py_funcs.items()):
                func_path = Path(getattr(func, "__ff_path__", "")).resolve()
                if func_path == p.resolve():
                    name = func.__ff_name__
                    deps = getattr(func, "__ff_deps__", [])
                    self._add_node_or_fail(name, "python", p, deps, meta={})

                    req = getattr(func, "__ff_require__", None)
                    if req:
                        self.py_requires[name] = req

        # ---- Dependency validation (early and clear)
        self._validate_dependencies()

    # --- Macros ---------------------------------------------------------
    def _load_macros(self, models_dir: Path) -> None:
        """
        Load all Jinja macros from 'models/macros/**/*.sql' and register them
        into env.globals so they can be called directly as {{ my_macro(...) }}.
        """
        env = self.get_env()
        macros_dir = models_dir / "macros"
        if not macros_dir.exists():
            return
        for p in macros_dir.rglob("*.sql"):
            rel = p.relative_to(models_dir).as_posix()  # e.g., "macros/utils.sql"
            tmpl = self.env.get_template(rel)  # type: ignore[arg-type]
            module = tmpl.make_module({})  # render context not needed for macro extraction
            for name, obj in module.__dict__.items():
                # Jinja stores macro callables as jinja2.runtime.Macro
                if getattr(jinja2.runtime, "Macro", None) and isinstance(obj, jinja2.runtime.Macro):  # type: ignore[arg-type]
                    if name in self.macros:
                        # Last one wins; could be changed to raise if you prefer strictness.
                        pass
                    env.globals[name] = obj  # publish globally
                    self.macros[name] = p

    def _load_py_module(self, path: Path) -> types.ModuleType:
        """
        Load a Python module from filesystem path in a typing-safe way.
        Ensures both spec and spec.loader are non-None, otherwise raises.
        """
        # Important: use absolute paths so later comparisons work
        path = path.resolve()

        spec = importlib.util.spec_from_file_location(path.stem, path)
        if spec is None:
            raise ModuleLoadError(f"Unable to create module spec for {path}")

        if spec.loader is None:
            raise ModuleLoadError(f"Module spec has no loader for {path}")

        # At this point spec is a ModuleSpec and loader is guaranteed to exist
        spec = cast(ModuleSpec, spec)
        mod = importlib.util.module_from_spec(spec)
        # exec_module is part of the loader protocol; Pylance now knows the type
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod

    def _add_node_or_fail(self, name: str, kind: str, path: Path, deps: list[str], *, meta: dict[str, Any]):
        if name in self.nodes:
            other = self.nodes[name].path
            raise ModuleLoadError(
                "Doppelter Modellname erkannt:\n"
                f"• bereits registriert: {other}\n"
                f"• weiterer Fund:        {path}\n"
                "Tipp: Benenne eines der Modelle um (Dateistamm = Node-Name) "
                "oder nutze @model(name='…') für Python."
            )
        self.nodes[name] = Node(name=name, kind=kind, path=path, deps=deps, meta=meta)

    def _scan_sql_deps(self, path: Path) -> list[str]:
        txt = path.read_text(encoding="utf-8")
        # very simple ref() parser
        import re

        pattern = re.compile(r"ref\s*\(\s*['\"]([a-zA-Z0-9_\-]+)['\"]\s*\)")
        return pattern.findall(txt)
    
    # -------- {{ config(...) }} Head-Parser --------
    def _parse_model_config(self, path: Path) -> dict[str, Any]:
        """
        Reads the leading line {{ config(materialized='view', key=1) }}.
        Safely parses via ast.literal_eval for keyword arguments. Errors → {}.
        """
        import re, ast
        try:
            head = path.read_text(encoding="utf-8", errors="ignore")[:2000]
        except Exception:
            return {}
        m = re.search(r"^\s*\{\{\s*config\s*\((?P<args>.*?)\)\s*\}\}", head, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            return {}
        args = m.group("args").strip()
        if not args:
            return {}
        try:
            # parse "a=1, b='x'" as a Call and extract keywords
            node = ast.parse(f"__CFG__({args})", mode="eval")
            if not isinstance(node.body, ast.Call):
                return {}
            cfg: dict[str, Any] = {}
            for kw in node.body.keywords:
                if kw.arg is None:
                    # **kwargs werden (noch) ignoriert
                    continue
                cfg[kw.arg] = ast.literal_eval(kw.value)
            return cfg
        except Exception:
            # Robust: keine Hard-Fails beim Laden
            return {}

    def _validate_dependencies(self) -> None:
        """
        Collect all missing dependencies across nodes and raise
        DependencyNotFoundError with a precise list and hints.
        """
        missing_map: dict[str, list[str]] = {}
        known = set(self.nodes.keys())
        for node in self.nodes.values():
            # Only validate actual model refs – source() targets are not nodes
            missing = [dep for dep in (node.deps or []) if dep not in known]
            if missing:
                missing_map[node.name] = missing

        if missing_map:
            raise DependencyNotFoundError(missing_map)


REGISTRY = Registry()

# ---- DSL helpers ----


def relation_for(node_name: str) -> str:
    """
    Map a logical node name to the physical relation (table/view name).
    Convention:
      - if the name ends with '.ff' → strip the suffix (e.g. 'users.ff' → 'users')
      - otherwise: return unchanged
    """
    return node_name[:-3] if node_name.endswith(".ff") else node_name
