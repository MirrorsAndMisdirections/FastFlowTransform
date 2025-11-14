# fastflowtransform/core.py
from __future__ import annotations

import ast
import importlib.util
import inspect
import os
import re
import types
from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import jinja2.runtime
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pydantic import ValidationError

from fastflowtransform import storage
from fastflowtransform.config.models import validate_model_meta_strict
from fastflowtransform.config.project import parse_project_yaml_config
from fastflowtransform.config.sources import load_sources_config
from fastflowtransform.errors import (
    DependencyNotFoundError,
    ModelConfigError,
    ModuleLoadError,
)
from fastflowtransform.logging import get_logger


def _validate_py_model_signature(func: Callable, deps: list[str], *, path: Path, name: str) -> None:
    """
    Validate that a Python model function can accept the declared deps.

    Rules:
      - If no deps are declared:
          - Functions with 0 positional params are OK.
          - Functions with *args/**kwargs are OK.
          - Otherwise: error.
      - If N deps are declared:
          - Functions with at least N positional params are OK.
          - Functions with *args are OK regardless of arity.
          - Otherwise: error.
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    # Count positional params (pos-only or pos-or-kw)
    pos_params = [p for p in params if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    has_varargs = any(p.kind == p.VAR_POSITIONAL for p in params)  # *args
    has_varkw = any(p.kind == p.VAR_KEYWORD for p in params)  # **kwargs

    dep_count = len(deps or [])

    # Zero deps case
    if dep_count == 0:
        if len(pos_params) == 0:
            return  # perfect match
        if has_varargs or has_varkw:
            return  # flexible function, OK
        # Too many required positional params, no *args/**kwargs
        raise ModuleLoadError(
            f"{path}: @model(name='{name}') declares no deps but the function defines "
            f"{len(pos_params)} positional parameter(s).",
            hint=(
                "Strict mode is enabled: zero-dep models must not define positional parameters.\n"
                "Fix one of the following:\n"
                "  • Remove parameters:        def build(): …\n"
                "  • Accept varargs:           def build(*_): …\n"
                "  • Declare explicit deps:    @model(..., deps=['upstream']) "
                "and def build(upstream): …"
            ),
            code="PY_SIG_STRICT",
        )

    # N deps case
    if len(pos_params) >= dep_count or has_varargs:
        return  # OK (enough positional slots, or *args present)

    # Not enough positional capacity, and no *args
    expected = ", ".join(deps)
    raise ModuleLoadError(
        f"{path}: @model(name='{name}') declares {dep_count} dep(s) but the function "
        f"accepts only {len(pos_params)} positional parameter(s) and no *args.",
        hint=(
            "Match parameter count/order to your deps or accept varargs.\n"
            f"Example: def {func.__name__}({expected}): …\n"
            f"Or:      def {func.__name__}(*deps): …"
        ),
    )


def _merge_source_configs(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if key == "options":
            opts = dict(merged.get("options") or {})
            opts.update(value or {})
            merged["options"] = opts
        else:
            merged[key] = value
    if "options" not in merged or merged["options"] is None:
        merged["options"] = {}
    return merged


def resolve_source_entry(
    entry: Mapping[str, Any], engine: str | None, *, default_identifier: str | None = None
) -> dict[str, Any]:
    base = entry.get("base") if isinstance(entry, Mapping) else None
    if not isinstance(base, Mapping):
        base = {}

    cfg = dict(base)
    cfg.setdefault("identifier", None)
    cfg.setdefault("schema", None)
    cfg.setdefault("database", None)
    cfg.setdefault("catalog", None)
    cfg.setdefault("project", None)
    cfg.setdefault("dataset", None)
    cfg.setdefault("location", None)
    cfg.setdefault("format", None)
    cfg.setdefault("options", {})

    overrides = entry.get("overrides") if isinstance(entry, Mapping) else None
    if isinstance(overrides, Mapping):
        for wildcard_key in ("*", "default", "any"):
            if wildcard_key in overrides:
                cfg = _merge_source_configs(cfg, overrides[wildcard_key])
        if engine and engine in overrides:
            cfg = _merge_source_configs(cfg, overrides[engine])

    ident = cfg.get("identifier")
    if (ident is None or ident == "") and not cfg.get("location"):
        if default_identifier:
            cfg["identifier"] = default_identifier
        else:
            raise KeyError("Source configuration missing identifier or location")

    return cfg


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
        self.project_vars: dict[str, Any] = {}  # project.yml: vars
        self.cli_vars: dict[str, Any] = {}  # CLI --vars overrides
        self.active_engine: str | None = None
        self.incremental_models: dict[str, dict[str, Any]] = {}

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

    def set_active_engine(self, engine: str | None) -> None:
        """Store active engine hint (case-insensitive) for conditional loading."""
        self.active_engine = engine.lower().strip() if isinstance(engine, str) else None

    def _lookup_storage_meta(self, node_name: str) -> dict[str, Any]:
        """
        Return storage metadata for a given node (if configured in project.yml).
        Accepts names with or without trailing '.ff'.
        """
        return storage.get_model_storage(node_name)

    def _lookup_incremental_meta(self, node_name: str) -> dict[str, Any]:
        """
        Return incremental metadata for a given node (from project.yml → models.incremental).

        Accepts names with or without trailing '.ff' — we try both variants and
        return the first match.
        """
        candidates: list[str]
        if node_name.endswith(".ff"):
            # e.g. "users.ff" → try "users.ff", then "users"
            candidates = [node_name, node_name[:-3]]
        else:
            # e.g. "users" → try "users", then "users.ff"
            candidates = [node_name, f"{node_name}.ff"]

        for key in candidates:
            cfg = self.incremental_models.get(key)
            if cfg:
                return dict(cfg)

        return {}

    def _current_engine(self) -> str | None:
        """
        Determine the active engine in precedence order:
        1) Explicit hint via set_active_engine()
        2) Environment variable FF_ENGINE
        3) project.yml vars → engine
        4) CLI --vars {engine: ...}
        """
        if self.active_engine:
            return self.active_engine

        env_engine = os.getenv("FF_ENGINE")
        if isinstance(env_engine, str) and env_engine.strip():
            return env_engine.strip().lower()

        proj_engine = self.project_vars.get("engine")
        if isinstance(proj_engine, str) and proj_engine.strip():
            return proj_engine.strip().lower()

        cli_engine = self.cli_vars.get("engine")
        if isinstance(cli_engine, str) and cli_engine.strip():
            return cli_engine.strip().lower()

        return None

    def _should_register_for_engine(self, meta: Mapping[str, Any], *, path: Path) -> bool:
        """
        SQL models may declare config(engines=[...]) to limit registration.
        Returns True when the current engine matches (or no restriction given).
        """
        raw = meta.get("engines")
        if raw is None:
            return True

        tokens: Iterable[Any]
        if isinstance(raw, str):
            tokens = [raw]
        elif isinstance(raw, Iterable) and not isinstance(raw, (str, Mapping)):
            tokens = raw
        else:
            raise ModuleLoadError(
                f"{path}: config(engines=...) must be a string or iterable of strings."
            )

        allowed: set[str] = set()
        for tok in tokens:
            if not isinstance(tok, (str, bytes)):
                raise ModuleLoadError(
                    f"{path}: config(engines=...) expects strings, got {type(tok).__name__}."
                )
            text = str(tok).strip()
            if text:
                allowed.add(text.lower())

        if not allowed:
            return True

        current = self._current_engine()
        if current is None:
            raise ModuleLoadError(
                f"{path}: config(engines=...) requires an active engine.\n"
                "Hint: Export FF_ENGINE or call REGISTRY.set_active_engine('duckdb'|...)."
            )
        return current in allowed

    def load_project(self, project_dir: Path) -> None:
        """Load a FastFlowTransform project from the given directory."""
        self._reset_registry_state()
        self.project_dir = project_dir

        models_dir = project_dir / "models"
        self._init_jinja_env(models_dir)

        # macros first, because models may use them
        self._load_macros(models_dir)
        self._load_py_macros(models_dir)

        self._load_sources_yaml(project_dir)
        self._load_project_yaml(project_dir)

        # discover models
        self._discover_sql_models(models_dir)
        self._discover_python_models(models_dir)

        # final validation
        self._validate_dependencies()

    def _reset_registry_state(self) -> None:
        """Reset in-memory registry structures to a clean state."""
        self.nodes.clear()
        self.py_funcs.clear()
        self.py_requires.clear()
        self.sources = {}
        self.project_vars = {}
        self.cli_vars = {}
        self.macros.clear()
        self.incremental_models = {}
        # reset storage maps
        storage.set_model_storage({})
        storage.set_seed_storage({})

    def _init_jinja_env(self, models_dir: Path) -> None:
        """Initialize the Jinja environment for this project."""
        self.env = Environment(
            loader=FileSystemLoader(str(models_dir)),
            undefined=StrictUndefined,
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # ---- Make project vars & helpers available in Jinja ----
        # Note: these callables close over `self`, so they always read the
        # latest self.cli_vars / self.project_vars even after project.yml loads.
        def _var(key: str, default: Any | None = None) -> Any:
            # CLI --vars override project vars
            if isinstance(self.cli_vars, dict) and key in self.cli_vars:
                return self.cli_vars[key]
            if isinstance(self.project_vars, dict) and key in self.project_vars:
                return self.project_vars[key]
            return default

        def _engine(default: str | None = None) -> str | None:
            # Current active engine (duckdb|postgres|databricks_spark|…)
            return self._current_engine() or default

        # Simple env reader for templates/macros: {{ env("NAME", "fallback") }}
        def _env(name: str, default: Any | None = None) -> Any:
            return os.environ.get(name, default)

        # Expose helpers to Jinja
        self.env.globals["var"] = _var
        self.env.globals["engine"] = _engine
        self.env.globals["env"] = _env

        self.env.filters["var"] = _var
        self.env.filters["env"] = _env

    def _load_sources_yaml(self, project_dir: Path) -> None:
        """Load sources.yml (version 2) if present."""
        src_path = project_dir / "sources.yml"
        if not src_path.exists():
            self.sources = {}
            return

        try:
            self.sources = load_sources_config(project_dir)
        except Exception as exc:
            # pydantic.ValidationError, ValueError, etc.
            raise ValueError(f"Failed to parse sources.yml: {exc}") from exc

    def _load_project_yaml(self, project_dir: Path) -> None:
        """Load and validate project.yml (vars, storage, incremental overlays)."""
        proj_path = project_dir / "project.yml"
        if not proj_path.exists():
            return

        try:
            proj_cfg = parse_project_yaml_config(project_dir)
        except Exception as exc:
            # Surface a clear error when project.yml is invalid
            raise ValueError(f"Failed to parse project.yml: {exc}") from exc

        # Vars → available in Jinja via var("key")
        self.project_vars = dict(proj_cfg.vars or {})

        # Incremental overlays (per model) from project.yml → models.incremental
        # Stored as plain dicts so the rest of the registry can treat them as before.
        self.incremental_models = {
            name: cfg.model_dump(exclude_none=True)
            for name, cfg in proj_cfg.models.incremental.items()
        }

        # models.storage → storage.set_model_storage(...)
        model_storage_raw: dict[str, dict[str, Any]] = {
            name: s.model_dump(exclude_none=True) for name, s in proj_cfg.models.storage.items()
        }
        storage.set_model_storage(
            storage.normalize_storage_map(model_storage_raw, project_dir=project_dir)
        )

        # seeds.storage → storage.set_seed_storage(...)
        seed_storage_raw: dict[str, dict[str, Any]] = {
            name: s.model_dump(exclude_none=True) for name, s in proj_cfg.seeds.storage.items()
        }
        storage.set_seed_storage(
            storage.normalize_storage_map(seed_storage_raw, project_dir=project_dir)
        )

    def _discover_sql_models(self, models_dir: Path) -> None:
        """Scan *.ff.sql files, parse config, validate meta, and register nodes."""
        for path in models_dir.rglob("*.ff.sql"):
            name = path.stem
            deps = self._scan_sql_deps(path)

            # Raw config from leading {{ config(...) }} in the SQL file
            raw_meta = dict(self._parse_model_config(path))

            # Merge project-level storage override (project.yml → models.storage)
            storage_meta = self._lookup_storage_meta(name)
            if storage_meta:
                existing = dict(raw_meta.get("storage") or {})
                existing.update(storage_meta)
                raw_meta["storage"] = existing

            # Merge project-level incremental overlay (project.yml → models.incremental)
            incr_meta = self._lookup_incremental_meta(name)
            if incr_meta:
                merged = dict(incr_meta)
                merged.update(raw_meta or {})
                raw_meta = merged

            # Pydantic validation: hard fail on unknown keys / wrong types
            try:
                cfg = validate_model_meta_strict(raw_meta)
            except ValidationError as exc:
                # Reformat Pydantic errors into a compact, user-friendly message.
                lines = []
                for err in exc.errors():
                    loc = ".".join(str(p) for p in err.get("loc", ()) if p != "__root__")
                    msg = err.get("msg", "invalid value")
                    if loc:
                        lines.append(f"• {loc}: {msg}")
                    else:
                        lines.append(f"• {msg}")
                details = "\n".join(lines) if lines else str(exc)
                raise ModelConfigError(
                    f"schema validation failed:\n{details}",
                    path=str(path),
                    hint="Fix the fields listed above. Unknown keys are rejected (extra='forbid').",
                    code="CFG_SCHEMA",
                ) from exc

            # Backwards-compatible default: incremental → materialized='incremental'
            if cfg.is_incremental_enabled() and cfg.materialized is None:
                cfg.materialized = "incremental"

            # Node.meta is kept as a plain dict
            meta = cfg.model_dump(exclude_none=True)

            # Engine-filtering still works on the dict (config(engines=[...]))
            if not self._should_register_for_engine(meta, path=path):
                continue

            self._add_node_or_fail(name, "sql", path, deps, meta=meta)

    def _discover_python_models(self, models_dir: Path) -> None:
        """Scan *.ff.py files, import them, validate meta, and register decorated callables."""
        for path in models_dir.rglob("*.ff.py"):
            # Import the module so decorators can register functions
            self._load_py_module(path)

            # We may have loaded several functions; filter by file path
            for _, func in list(self.py_funcs.items()):
                func_path = Path(getattr(func, "__ff_path__", "")).resolve()
                if func_path != path.resolve():
                    continue

                name = getattr(func, "__ff_name__", func.__name__)
                deps = getattr(func, "__ff_deps__", [])
                kind = getattr(func, "__ff_kind__", "python") or "python"

                # Validate function signature vs declared deps (fail fast)
                _validate_py_model_signature(func, deps or [], path=path, name=name)

                # Raw meta attached by @model(..., meta={...})
                raw_meta = dict(getattr(func, "__ff_meta__", {}) or {})

                # Merge storage override from project.yml (models.storage)
                storage_meta = self._lookup_storage_meta(name)
                if storage_meta:
                    existing = dict(raw_meta.get("storage") or {})
                    existing.update(storage_meta)
                    raw_meta["storage"] = existing

                # Merge incremental overlay from project.yml (models.incremental)
                incr_meta = self._lookup_incremental_meta(name)
                if incr_meta:
                    merged = dict(incr_meta)
                    merged.update(raw_meta or {})
                    raw_meta = merged

                # Merge tags from decorator into meta.tags
                tags = list(getattr(func, "__ff_tags__", []) or [])
                if tags:
                    existing_tags = raw_meta.get("tags")
                    if isinstance(existing_tags, list):
                        base = existing_tags
                    elif existing_tags is None:
                        base = []
                    else:
                        base = [existing_tags]
                    merged_tags = base + [t for t in tags if t not in base]
                    raw_meta["tags"] = merged_tags

                # Store kind in meta for selectors / docs (optional but handy)
                raw_meta.setdefault("kind", kind)

                # Validate via Pydantic
                try:
                    cfg = validate_model_meta_strict(raw_meta)
                except ValidationError as exc:
                    lines = []
                    for err in exc.errors():
                        loc = ".".join(str(p) for p in err.get("loc", ()) if p != "__root__")
                        msg = err.get("msg", "invalid value")
                        if loc:
                            lines.append(f"• {loc}: {msg}")
                        else:
                            lines.append(f"• {msg}")
                    details = "\n".join(lines) if lines else str(exc)
                    raise ModelConfigError(
                        f"schema validation failed:\n{details}",
                        path=str(path),
                        hint="Check your @model(meta=...) dictionary.",
                        code="CFG_SCHEMA",
                    ) from exc

                # Default incremental materialization if enabled and not set explicitly
                if cfg.is_incremental_enabled() and cfg.materialized is None:
                    cfg.materialized = "incremental"

                meta = cfg.model_dump(exclude_none=True)

                # Register node
                self._add_node_or_fail(name, kind, path, deps, meta=meta)

                # Required-columns spec (for executors) stays as before
                req = getattr(func, "__ff_require__", None)
                if req:
                    self.py_requires[name] = req

    # --- Macros ---------------------------------------------------------
    def _load_macros(self, models_dir: Path) -> None:
        """
        Load all Jinja macros from 'models/macros/**/*.(sql|sql.j2)' and register them
        into env.globals so they can be called directly as {{ my_macro(...) }}.
        """
        env = self.get_env()
        macros_dir = models_dir / "macros"
        if not macros_dir.exists():
            return

        files = _collect_macro_files(macros_dir)
        if not files:
            return

        for path in files:
            rel = _relative_name(path, models_dir)
            tmpl = _get_or_build_template(env, path, rel)
            mod = _template_module_or_none(tmpl)
            if mod is None:
                continue

            for name, obj in _iter_public_attrs(mod):
                if _is_jinja_macro(obj):
                    env.globals[name] = obj  # last-one-wins ok
                    self.macros[name] = path

    def _load_py_macros(self, models_dir: Path) -> None:
        """
        Load Python helpers from 'models/macros_py/**/*.py' and register all public
        callables as Jinja globals & filters.
        """
        env = self.get_env()
        py_dir = models_dir / "macros_py"
        if not py_dir.exists():
            return

        for p in sorted(py_dir.rglob("*.py")):
            # unique module name to avoid caching collisions across tests/runs
            mod_name = f"ff_macros_{p.stem}_{abs(hash(str(p.resolve()))):x}"

            spec = importlib.util.spec_from_file_location(mod_name, p)
            if not spec or not spec.loader:
                continue

            mod = importlib.util.module_from_spec(spec)
            try:
                spec.loader.exec_module(mod)  # executes user code
            except Exception as e:
                # In Tests willst du das sehen; wenn du es leise ignorieren willst -> 'continue'
                raise RuntimeError(f"Failed to import macro helper {p}: {e}") from e

            for name, obj in vars(mod).items():
                if name.startswith("_") or not callable(obj):
                    continue
                env.globals[name] = obj
                with suppress(Exception):
                    env.filters[name] = obj
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

        mod = importlib.util.module_from_spec(spec)
        # exec_module is part of the loader protocol; Pylance now knows the type
        spec.loader.exec_module(mod)
        return mod

    def _add_node_or_fail(
        self, name: str, kind: str, path: Path, deps: list[str], *, meta: dict[str, Any]
    ) -> None:
        if name in self.nodes:
            other = self.nodes[name].path
            raise ModuleLoadError(
                "Duplicate model name detected:\n"
                f"• alredy registered: {other}\n"
                f"• new model:        {path}\n"
                "Hint: Rename one of the models (file name = node name)"
                "or use @model(name='…') for Python."
            )
        self.nodes[name] = Node(name=name, kind=kind, path=path, deps=deps, meta=meta)

    def _scan_sql_deps(self, path: Path) -> list[str]:
        txt = path.read_text(encoding="utf-8")
        literal = re.compile(r"ref\s*\(\s*['\"]([A-Za-z0-9_.\-]+)['\"]\s*\)")
        dynamic = re.compile(r"ref\s*\(\s*([^)]+)\)")

        deps = literal.findall(txt)

        for expr in dynamic.findall(txt):
            expr_stripped = expr.strip()
            if not (
                (expr_stripped.startswith("'") and expr_stripped.endswith("'"))
                or (expr_stripped.startswith('"') and expr_stripped.endswith('"'))
            ):
                logger = get_logger("registry")
                logger.warning(
                    "%s: ref(%s) cannot be statically resolved; DAG may miss this dependency. "
                    "Wrap options in a mapping of literal ref('...') calls and pick from that map.",
                    path,
                    expr_stripped,
                )

        return deps

    # -------- {{ config(...) }} Head-Parser --------
    def _parse_model_config(self, path: Path) -> dict[str, Any]:
        """
        Read the leading `{{ config(...) }}` header and parse keyword arguments.
        Behavior:
        - If no `config(...)` block is found → return {}.
        - If a `config(...)` block is found but parsing fails → RAISE ModuleLoadError.
        This ensures misconfigured headers fail loudly instead of being silently ignored.
        """
        try:
            head = path.read_text(encoding="utf-8", errors="ignore")[:2000]
        except Exception:
            return {}
        m = re.search(
            r"^\s*\{\{\s*config\s*\((?P<args>.*?)\)\s*\}\}", head, flags=re.IGNORECASE | re.DOTALL
        )
        if not m:
            return {}
        args = m.group("args").strip()
        if not args:
            return {}
        src = f"__CFG__({args})"
        try:
            node = ast.parse(src, mode="eval")
            if not isinstance(node.body, ast.Call):
                # Not a function-call AST; treat as empty to avoid false positives
                return {}
        except Exception as e:
            raise ModelConfigError(
                f"invalid syntax: {e}",
                path=str(path),
                field=None,
                hint="Ensure {{ config(...) }} contains comma-separated key=value literals.",
            ) from e

        cfg: dict[str, Any] = {}
        for kw in node.body.keywords:
            # Disallow **kwargs explicitly with a crisp message
            if kw.arg is None:
                val_src = ast.get_source_segment(src, kw.value) or "<expr>"
                raise ModelConfigError(
                    f"unsupported **kwargs (got {val_src})",
                    path=str(path),
                    field="**kwargs",
                    hint="Use explicit key=value pairs; expressions are not allowed.",
                )
            field = kw.arg
            try:
                cfg[field] = ast.literal_eval(kw.value)
            except Exception as err:
                val_src = ast.get_source_segment(src, kw.value) or "<expr>"
                raise ModelConfigError(
                    f"invalid literal (quote strings, no expressions): {val_src}",
                    path=str(path),
                    field=field,
                    hint="All values must be JSON/Python literals (e.g. 'view', ['tag']).",
                ) from err
        return cfg

    def _validate_dependencies(self) -> None:
        """
        Collect all missing dependencies across nodes and raise
        DependencyNotFoundError with a precise list and hints.
        """
        missing_map: dict[str, list[str]] = {}
        known = set(self.nodes.keys())
        for node in self.nodes.values():
            # Only validate actual model refs - source() targets are not nodes
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


# ----------------- Helper -----------------


def _collect_macro_files(macros_dir: Path) -> list[Path]:
    files = list(macros_dir.rglob("*.sql"))
    files += list(macros_dir.rglob("*.sql.j2"))
    return sorted(files)


def _relative_name(path: Path, models_dir: Path) -> str:
    with suppress(Exception):
        return path.relative_to(models_dir).as_posix()
    return path.name


def _get_or_build_template(env: jinja2.Environment, path: Path, rel: str) -> jinja2.Template:
    with suppress(Exception):
        return env.get_template(rel)
    src = path.read_text(encoding="utf-8")
    tmpl = env.from_string(src)
    tmpl.name = rel
    return tmpl


def _template_module_or_none(tmpl: jinja2.Template) -> Any:
    with suppress(Exception):
        return tmpl.module
    return None


def _iter_public_attrs(obj: object) -> Iterable[tuple[str, object]]:
    for name in dir(obj):
        if not name.startswith("_"):
            yield name, getattr(obj, name, None)


def _is_jinja_macro(obj: object) -> bool:
    if obj is None:
        return False
    # 1) Klassenname-Match (funktioniert ohne direkten Import)
    cls = getattr(obj, "__class__", None)
    if getattr(cls, "__name__", "") == "Macro":
        return True
    # 2) isinstance gegen jinja2.runtime.Macro (falls vorhanden)
    MacroClass = getattr(jinja2.runtime, "Macro", None)
    if MacroClass is not None:
        with suppress(Exception, TypeError):
            return isinstance(obj, MacroClass)
    return False
