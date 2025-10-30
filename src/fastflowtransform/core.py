# fastflowtransform/core.py
from __future__ import annotations

import ast
import importlib.util
import os
import re
import types
from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import jinja2.runtime
import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from fastflowtransform import storage
from fastflowtransform.errors import DependencyNotFoundError, ModuleLoadError
from fastflowtransform.logging import get_logger

_SOURCE_CFG_FIELDS = {
    "identifier",
    "schema",
    "database",
    "catalog",
    "project",
    "dataset",
    "location",
    "format",
    "options",
}


def _compact_cfg(cfg: Mapping[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in cfg.items():
        if key == "options":
            if value:
                cleaned[key] = dict(value)
            continue
        if value is not None:
            cleaned[key] = value
    return cleaned


def _normalize_options(value: Any, *, field_path: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return {str(k): v for k, v in value.items()}
    raise ValueError(f"sources.yml → {field_path}: expected mapping, got {type(value).__name__}")


def _pick_source_fields(
    data: Mapping[str, Any] | None,
    base: Mapping[str, Any] | None,
    *,
    field_path: str,
) -> dict[str, Any]:
    """Return a dict limited to the supported source configuration fields."""

    data = data or {}
    base = base or {}
    out: dict[str, Any] = {k: base.get(k) for k in _SOURCE_CFG_FIELDS}
    for key, value in data.items():
        if key not in _SOURCE_CFG_FIELDS:
            continue
        if key == "options":
            base_opts = out.get("options") or {}
            incoming = _normalize_options(value, field_path=f"{field_path}.options")
            merged = dict(base_opts)
            merged.update(incoming)
            out["options"] = merged
        else:
            out[key] = value

    if "options" not in out or out["options"] is None:
        out["options"] = {}
    return out


def _normalize_engine_overrides(
    overrides: Mapping[str, Any] | None,
    *,
    field_path: str,
) -> dict[str, dict[str, Any]]:
    if overrides is None:
        return {}
    if not isinstance(overrides, Mapping):
        raise ValueError(
            f"sources.yml → {field_path}: overrides must be a mapping of engine -> config"
        )

    normalized: dict[str, dict[str, Any]] = {}
    for engine, cfg in overrides.items():
        if cfg is None:
            normalized[str(engine)] = {}
            continue
        if not isinstance(cfg, Mapping):
            raise ValueError(
                f"sources.yml → {field_path}[{engine!r}]: "
                "expected mapping, got {type(cfg).__name__}"
            )
        picked = _pick_source_fields(cfg, None, field_path=f"{field_path}[{engine!r}]")
        normalized[str(engine)] = _compact_cfg(picked)
    return normalized


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


def _combine_engine_overrides(
    source_overrides: Mapping[str, dict[str, Any]],
    table_overrides: Mapping[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    engines = set(source_overrides) | set(table_overrides)
    combined: dict[str, dict[str, Any]] = {}
    for engine in engines:
        combined[engine] = _merge_source_configs(
            source_overrides.get(engine, {}),
            table_overrides.get(engine, {}),
        )
    return combined


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


def _parse_sources_yaml(raw: Any) -> dict[str, dict[str, dict[str, Any]]]:
    if not raw:
        return {}
    if not isinstance(raw, Mapping):
        raise ValueError("sources.yml must be a mapping with keys 'version' and 'sources'.")

    version = raw.get("version")
    version_no = 2
    if version != version_no:
        raise ValueError("sources.yml → version: Only '2' is supported.")

    entries = raw.get("sources")
    if entries is None:
        return {}
    if not isinstance(entries, Iterable):
        raise ValueError("sources.yml → sources: expected a list of source declarations.")

    normalized: dict[str, dict[str, dict[str, Any]]] = {}
    for idx, entry in enumerate(entries):
        if not isinstance(entry, Mapping):
            raise ValueError(
                f"sources.yml → sources[{idx}]: expected mapping, got {type(entry).__name__}."
            )

        src_name = entry.get("name")
        if not src_name or not isinstance(src_name, str):
            raise ValueError(f"sources.yml → sources[{idx}]: missing 'name'.")

        if src_name in normalized:
            raise ValueError(f"sources.yml: duplicate source '{src_name}'.")

        src_defaults = _pick_source_fields(entry, None, field_path=f"sources[{idx}]")
        src_overrides = _normalize_engine_overrides(
            entry.get("overrides"), field_path=f"sources[{idx}].overrides"
        )

        tables = entry.get("tables")
        if tables is None:
            raise ValueError(f"sources.yml → sources[{idx}]: missing 'tables' list.")
        if not isinstance(tables, Iterable):
            raise ValueError(
                f"sources.yml → sources[{idx}].tables: expected list, got {type(tables).__name__}."
            )

        group: dict[str, dict[str, Any]] = {}
        for t_idx, table in enumerate(tables):
            if not isinstance(table, Mapping):
                raise ValueError(
                    f"sources.yml → sources[{idx}].tables[{t_idx}]: "
                    f"expected mapping, got {type(table).__name__}."
                )

            tbl_name = table.get("name")
            if not tbl_name or not isinstance(tbl_name, str):
                raise ValueError(f"sources.yml → sources[{idx}].tables[{t_idx}]: missing 'name'.")

            if tbl_name in group:
                raise ValueError(
                    f"sources.yml → source '{src_name}': duplicate table '{tbl_name}'."
                )

            base_cfg = _pick_source_fields(
                table, src_defaults, field_path=f"sources[{idx}].tables[{t_idx}]"
            )
            if not base_cfg.get("identifier") and not base_cfg.get("location"):
                base_cfg["identifier"] = tbl_name

            table_overrides = _normalize_engine_overrides(
                table.get("overrides"),
                field_path=f"sources[{idx}].tables[{t_idx}].overrides",
            )
            overrides = _combine_engine_overrides(src_overrides, table_overrides)

            entry_meta = {
                "description": table.get("description"),
                "columns": table.get("columns"),
                "meta": table.get("meta"),
            }

            group[tbl_name] = {
                "base": base_cfg,
                "overrides": overrides,
                **{k: v for k, v in entry_meta.items() if v is not None},
            }

        normalized[src_name] = group

    return normalized


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

    # def load_project(self, project_dir: Path) -> None:
    #     self.nodes.clear()
    #     self.py_funcs.clear()
    #     self.py_requires.clear()
    #     self.sources = {}
    #     self.project_vars = {}
    #     self.cli_vars = {}
    #     self.macros.clear()

    #     storage.set_model_storage({})
    #     storage.set_seed_storage({})

    #     self.project_dir = project_dir
    #     models_dir = project_dir / "models"
    #     self.env = Environment(
    #         loader=FileSystemLoader(str(models_dir)),
    #         undefined=StrictUndefined,
    #         autoescape=False,
    #         trim_blocks=True,
    #         lstrip_blocks=True,
    #     )

    #     # Make sure macros are available to all templates before model discovery.
    #     self._load_macros(models_dir)
    #     self._load_py_macros(models_dir)

    #     # load sources (version 2 schema)
    #     src_path = project_dir / "sources.yml"
    #     if src_path.exists():
    #         raw_sources = yaml.safe_load(src_path.read_text(encoding="utf-8"))
    #         try:
    #             self.sources = _parse_sources_yaml(raw_sources)
    #         except ValueError as exc:
    #             raise ValueError(f"Failed to parse sources.yml: {exc}") from exc
    #     else:
    #         self.sources = {}

    #     # load project.yml (vars)
    #     proj_path = project_dir / "project.yml"
    #     if proj_path.exists():
    #         proj_cfg = yaml.safe_load(proj_path.read_text(encoding="utf-8")) or {}
    #         self.project_vars = dict(proj_cfg.get("vars", {}) or {})

    #         models_cfg = proj_cfg.get("models") if isinstance(proj_cfg, Mapping) else None
    #         model_storage_raw = None
    #         if isinstance(models_cfg, Mapping):
    #             candidate = models_cfg.get("storage")
    #             if isinstance(candidate, Mapping):
    #                 model_storage_raw = candidate
    #         storage.set_model_storage(
    #             storage.normalize_storage_map(model_storage_raw, project_dir=project_dir)
    #         )

    #         seeds_cfg = proj_cfg.get("seeds") if isinstance(proj_cfg, Mapping) else None
    #         seed_storage_raw = None
    #         if isinstance(seeds_cfg, Mapping):
    #             candidate = seeds_cfg.get("storage")
    #             if isinstance(candidate, Mapping):
    #                 seed_storage_raw = candidate
    #         storage.set_seed_storage(
    #             storage.normalize_storage_map(seed_storage_raw, project_dir=project_dir)
    #         )

    #     # discover models
    #     for p in models_dir.rglob("*.ff.sql"):
    #         name = p.stem
    #         deps = self._scan_sql_deps(p)
    #         meta = dict(self._parse_model_config(p))
    #         storage_meta = self._lookup_storage_meta(name)
    #         if storage_meta:
    #             existing = dict(meta.get("storage") or {})
    #             existing.update(storage_meta)
    #             meta["storage"] = existing
    #         if not self._should_register_for_engine(meta, path=p):
    #             continue
    #         self._add_node_or_fail(name, "sql", p, deps, meta=meta)
    #     for p in models_dir.rglob("*.ff.py"):
    #         self._load_py_module(p)
    #         for _, func in list(self.py_funcs.items()):
    #             func_path = Path(getattr(func, "__ff_path__", "")).resolve()
    #             if func_path == p.resolve():
    #                 name = getattr(func, "__ff_name__", func.__name__)
    #                 deps = getattr(func, "__ff_deps__", [])
    #                 kind = getattr(func, "__ff_kind__", "python") or "python"

    #                 meta = dict(getattr(func, "__ff_meta__", {}) or {})
    #                 storage_meta = self._lookup_storage_meta(name)
    #                 if storage_meta:
    #                     existing = dict(meta.get("storage") or {})
    #                     existing.update(storage_meta)
    #                     meta["storage"] = existing
    #                 tags = list(getattr(func, "__ff_tags__", []) or [])
    #                 if tags:
    #                     existing_tags = meta.get("tags")
    #                     if isinstance(existing_tags, list):
    #                         merged = existing_tags + [t for t in tags if t not in existing_tags]
    #                         meta["tags"] = merged
    #                     elif existing_tags is None:
    #                         meta["tags"] = tags
    #                     else:
    #                         # Normalize non-list tags into a list while preserving the value
    #                         meta["tags"] = [existing_tags, *tags]

    #                 self._add_node_or_fail(name, kind, p, deps, meta=meta)

    #                 req = getattr(func, "__ff_require__", None)
    #                 if req:
    #                     self.py_requires[name] = req

    #     # ---- Dependency validation (early and clear)
    #     self._validate_dependencies()

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

    def _load_sources_yaml(self, project_dir: Path) -> None:
        """Load sources.yml (version 2) if present."""
        src_path = project_dir / "sources.yml"
        if not src_path.exists():
            self.sources = {}
            return

        raw_sources = yaml.safe_load(src_path.read_text(encoding="utf-8"))
        try:
            self.sources = _parse_sources_yaml(raw_sources)
        except ValueError as exc:
            raise ValueError(f"Failed to parse sources.yml: {exc}") from exc

    def _load_project_yaml(self, project_dir: Path) -> None:
        """Load project.yml (vars, storage blocks) if present."""
        proj_path = project_dir / "project.yml"
        if not proj_path.exists():
            return

        proj_cfg = yaml.safe_load(proj_path.read_text(encoding="utf-8")) or {}
        self.project_vars = dict(proj_cfg.get("vars", {}) or {})

        # models.storage
        models_cfg = proj_cfg.get("models") if isinstance(proj_cfg, Mapping) else None
        model_storage_raw = None
        if isinstance(models_cfg, Mapping):
            candidate = models_cfg.get("storage")
            if isinstance(candidate, Mapping):
                model_storage_raw = candidate
        storage.set_model_storage(
            storage.normalize_storage_map(model_storage_raw, project_dir=project_dir)
        )

        # seeds.storage
        seeds_cfg = proj_cfg.get("seeds") if isinstance(proj_cfg, Mapping) else None
        seed_storage_raw = None
        if isinstance(seeds_cfg, Mapping):
            candidate = seeds_cfg.get("storage")
            if isinstance(candidate, Mapping):
                seed_storage_raw = candidate
        storage.set_seed_storage(
            storage.normalize_storage_map(seed_storage_raw, project_dir=project_dir)
        )

    def _discover_sql_models(self, models_dir: Path) -> None:
        """Scan *.ff.sql files, parse deps, and register nodes."""
        for path in models_dir.rglob("*.ff.sql"):
            name = path.stem
            deps = self._scan_sql_deps(path)
            meta = dict(self._parse_model_config(path))
            storage_meta = self._lookup_storage_meta(name)
            if storage_meta:
                existing = dict(meta.get("storage") or {})
                existing.update(storage_meta)
                meta["storage"] = existing
            if not self._should_register_for_engine(meta, path=path):
                continue
            self._add_node_or_fail(name, "sql", path, deps, meta=meta)

    def _discover_python_models(self, models_dir: Path) -> None:
        """Scan *.ff.py files, import them, and register decorated callables."""
        for path in models_dir.rglob("*.ff.py"):
            self._load_py_module(path)

            # we might have loaded several functions; filter by file path
            for _, func in list(self.py_funcs.items()):
                func_path = Path(getattr(func, "__ff_path__", "")).resolve()
                if func_path != path.resolve():
                    continue

                name = getattr(func, "__ff_name__", func.__name__)
                deps = getattr(func, "__ff_deps__", [])
                kind = getattr(func, "__ff_kind__", "python") or "python"

                meta = dict(getattr(func, "__ff_meta__", {}) or {})
                storage_meta = self._lookup_storage_meta(name)
                if storage_meta:
                    existing = dict(meta.get("storage") or {})
                    existing.update(storage_meta)
                    meta["storage"] = existing

                # merge tags from decorator into model meta.tags
                tags = list(getattr(func, "__ff_tags__", []) or [])
                if tags:
                    existing_tags = meta.get("tags")
                    if isinstance(existing_tags, list):
                        merged = existing_tags + [t for t in tags if t not in existing_tags]
                        meta["tags"] = merged
                    elif existing_tags is None:
                        meta["tags"] = tags
                    else:
                        meta["tags"] = [existing_tags, *tags]

                self._add_node_or_fail(name, kind, path, deps, meta=meta)

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
        Reads the leading line {{ config(materialized='view', key=1) }}.
        Safely parses via ast.literal_eval for keyword arguments. Errors → {}.
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
