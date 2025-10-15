# flowforge/executors/base.py
from __future__ import annotations

import contextvars, os, re
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Generic, TypeVar

import logging
from jinja2 import Environment

from flowforge.core import REGISTRY, Node, relation_for

# Frame type (pandas.DataFrame, pyspark.sql.DataFrame, snowflake.snowpark.DataFrame, ...)
TFrame = TypeVar("TFrame")


class BaseExecutor(Generic[TFrame], ABC):
    """
    Shared workflow for SQL rendering and Python models.
    I/O is frame-agnostic; subclasses provide frame-specific hooks:
      - _read_relation
      - _materialize_relation
      - _validate_required
      - _columns_of
      - _is_frame
      - (optional) _frame_name
    """

    # ---------- SQL ----------
    def render_sql(
        self,
        node: Node,
        env: Environment,
        ref_resolver: Callable[[str], str] | None = None,
        source_resolver: Callable[[str, str], str] | None = None,
    ) -> str:
        # ---- thread-/task-local config()-hook
        _RENDER_CFG: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
            "_RENDER_CFG", default={}
        )
        def _config_hook(**kwargs) -> str:
            d = _RENDER_CFG.get().copy()
            d.update(kwargs)
            _RENDER_CFG.set(d)
            return ""  # nichts in SQL emittieren
        if "config" not in env.globals:
            env.globals["config"] = _config_hook

        # ---- var() builtin: CLI overrides > project.yml vars > default
        if "var" not in env.globals:
            def _var(key: str, default: Any = None) -> Any:
                cli = getattr(REGISTRY, "cli_vars", {}) or {}
                if key in cli:
                    return cli[key]
                proj = getattr(REGISTRY, "project_vars", {}) or {}
                if key in proj:
                    return proj[key]
                return default
            env.globals["var"] = _var

        raw = Path(node.path).read_text(encoding="utf-8")
        tmpl = env.from_string(raw)

        def _default_ref(name: str) -> str:
            return relation_for(name)

        def _default_source(source_name: str, table_name: str) -> str:
            cfg = REGISTRY.sources.get(source_name, {}).get(table_name)
            if not cfg:
                raise KeyError(f"Unknown source {source_name}.{table_name}")
            identifier = cfg.get("identifier")
            if not identifier:
                raise KeyError(f"Source {source_name}.{table_name} missing identifier")
            return identifier

        _RENDER_CFG.set({})
        # expose 'this' to the template
        this_obj = {
            "name": relation_for(node.name),
            "materialized": (getattr(node, "meta", {}) or {}).get("materialized", "table"),
            # best-effort: these are engine-specific and may be None
            "schema": getattr(self, "schema", None) or getattr(self, "dataset", None),
            "database": getattr(self, "database", None) or getattr(self, "project", None),
        }

        sql = tmpl.render(
            ref=ref_resolver or _default_ref,
            source=source_resolver or _default_source,
            this=this_obj,
        )

        cfg = _RENDER_CFG.get()
        if cfg:
            for k, v in cfg.items():
                node.meta.setdefault(k, v)
        return sql

    def run_sql(self, node: Node, env: Environment) -> None:
        """
        Shared orchestration for SQL models. Subclasses implement quoting and DDL via hooks.

        Flow:
          1. Set up ref()/source() resolvers (with ephemeral inlining)
          2. Render the SQL template
          3. Strip leading config blocks
          4. Skip materialisation for ephemeral models
          5. Extract the SELECT body and let the executor materialise it
        """

        sql = self.render_sql(
            node,
            env,
            ref_resolver=lambda name: self._resolve_ref(name, env),
            source_resolver=self._resolve_source,
        )
        sql = self._strip_leading_config(sql).strip()

        materialization = (node.meta or {}).get("materialized", "table")
        if materialization == "ephemeral":
            return

        select_body = self._first_select_body(sql).rstrip(" ;\n\t")
        target_sql = self._format_relation_for_ref(node.name)
        self._apply_sql_materialization(node, target_sql, select_body, materialization)

    # --- Helpers for materialization & ephemeral inlining (instance methods) ---
    def _first_select_body(self, sql: str) -> str:
        """
        Extrahiert den 'SELECT …' Teil aus einem SQL-String, damit wir ihn in
        CREATE VIEW/TABLE … AS <body> einfügen können. Falls kein SELECT gefunden
        wird, geben wir den Original-String zurück.
        """
        m = re.search(r"\bselect\b", sql, flags=re.I | re.S)
        return sql[m.start():] if m else sql

    def _strip_leading_config(self, sql: str) -> str:
        """
        Entfernt eine führende Jinja-Zeile/Block {{ config(...) }} vom SQL,
        damit der Executor saubere DDL/DML bekommt.
        """
        return re.sub(
            r"^\s*\{\{\s*config\s*\(.*?\)\s*\}\}\s*",
            "",
            sql,
            flags=re.I | re.S,
        )

    def _render_ephemeral_sql(self, name: str, env: Environment) -> str:
        """
        Rendert das SQL für ein 'ephemeral' Modell und gibt eine Subquery
        als String '( ... )' zurück. Diese Methode respektiert rekursiv
        weitere 'ephemeral'-Refs.
        """
        # Node lookup (unterstützt optional REGISTRY.get_node aliasing)
        node = REGISTRY.get_node(name) if hasattr(REGISTRY, "get_node") else REGISTRY.nodes[name]

        raw = Path(node.path).read_text(encoding="utf-8")
        tmpl = env.from_string(raw)

        # ref() → bei 'ephemeral' rekursiv inline, sonst physischer Relationsname
        sql = tmpl.render(
            ref=lambda n: self._resolve_ref(n, env),
            source=self._resolve_source,
        )
        sql = self._strip_leading_config(sql).strip()
        body = self._first_select_body(sql).rstrip(" ;\n\t")
        return f"(\n{body}\n)"

    # ---------- Python models ----------
    def run_python(self, node: Node) -> None:
        func = REGISTRY.py_funcs[node.name]
        deps = REGISTRY.nodes[node.name].deps or []
        if not deps:
            raise ValueError(f"Python-Modell '{node.name}' hat keine Dependencies (erwarte ≥1).")

        # Load inputs
        if len(deps) == 1:
            rel = relation_for(deps[0])
            df_in: TFrame = self._read_relation(rel, node, deps)
            self._log_dep(node.name, rel, df_in)
            arg: Any = df_in  # TFrame
        else:
            frames: dict[str, TFrame] = {}
            for dep in deps:
                rel = relation_for(dep)
                f = self._read_relation(rel, node, deps)
                self._log_dep(node.name, rel, f)
                frames[rel] = f
            arg = frames  # dict[str, TFrame]

        # Validate required columns / structure (frame specific)
        requires = REGISTRY.py_requires.get(node.name, {})
        self._validate_required(node.name, arg, requires)

        # Execute the model
        out = func(arg)
        if not self._is_frame(out):
            raise TypeError(
                f"Python-Modell '{node.name}' muss {self._frame_name()} DataFrame zurückgeben."
            )

        # Materialize the result (table default; view supported)
        target = relation_for(node.name)
        mat = (getattr(node, "meta", {}) or {}).get("materialized", "table")
        if mat == "view":
            backing = self._py_view_backing_name(target)
            self._materialize_relation(backing, out, node)
            self._create_or_replace_view_from_table(target, backing, node)
        else:
            self._materialize_relation(target, out, node)

    # -------- Python model view helpers (shared) --------
    def _py_view_backing_name(self, relation: str) -> str:
        """
        Backing table name for Python models materialized as views.
        Must be a valid identifier for the target engine.
        """
        return f"__ff_py_{relation}"

    @abstractmethod
    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        """
        Create (or replace) a VIEW named `view_name` that selects from `backing_table`.
        Implement engine-specific DDL here.
        """
        ...

    # ---------- SQL hook contracts ----------
    @abstractmethod
    def _format_relation_for_ref(self, name: str) -> str:
        """
        Return the engine-specific SQL identifier used to reference a model's materialised relation.
        """
        ...

    @abstractmethod
    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        """
        Return the SQL identifier used to reference a configured source.
        """
        ...

    def _apply_sql_materialization(
        self, node: Node, target_sql: str, select_body: str, materialization: str
    ) -> None:
        """
        Materialise the rendered SELECT according to the requested kind (`table`, `view`, ...).
        The default implementation delegates to `create_or_replace_*` hooks.
        """
        if materialization == "view":
            self._create_or_replace_view(target_sql, select_body, node)
        else:
            self._create_or_replace_table(target_sql, select_body, node)

    @abstractmethod
    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        """
        Engine-specific implementation for CREATE OR REPLACE VIEW ... AS <body>.
        """
        ...

    @abstractmethod
    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        """
        Engine-specific implementation for CREATE OR REPLACE TABLE ... AS <body>.
        """
        ...

    # ---------- Resolution helpers ----------
    def _resolve_ref(self, name: str, env: Environment) -> str:
        dep = REGISTRY.get_node(name) if hasattr(REGISTRY, "get_node") else REGISTRY.nodes[name]
        if dep.meta.get("materialized") == "ephemeral":
            return self._render_ephemeral_sql(dep.name, env)
        return self._format_relation_for_ref(name)

    def _resolve_source(self, source_name: str, table_name: str) -> str:
        cfg = REGISTRY.sources.get(source_name, {}).get(table_name)
        if not cfg or "identifier" not in cfg:
            raise KeyError(f"Unknown source {source_name}.{table_name}")
        return self._format_source_reference(cfg, source_name, table_name)

    # ---------- Logging ----------
    def _log_dep(self, node_name: str, rel: str, frame: TFrame) -> None:
        sql_log = logging.getLogger("flowforge.sql")
        if sql_log.isEnabledFor(logging.DEBUG) or os.getenv("FLOWFORGE_SQL_DEBUG") == "1":
            cols = ", ".join(self._columns_of(frame))
            sql_log.debug(f"py:{node_name} dep {rel} cols=[{cols}]")

    # ---------- Abstrakte Frame-Hooks ----------
    @abstractmethod
    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> TFrame: ...

    @abstractmethod
    def _materialize_relation(self, relation: str, df: TFrame, node: Node) -> None: ...

    def _validate_required(
        self, node_name: str, inputs: Any, requires: dict[str, set[str]]
    ) -> None:
        """
        inputs: either TFrame (single dependency) or dict[str, TFrame] (multiple dependencies)
        raises: ValueError with a clear explanation when columns/keys are missing
        """
        if not requires:
            return
        from flowforge.validation import validate_required_columns

        validate_required_columns(node_name, inputs, requires)

    def _columns_of(self, frame: TFrame) -> list[str]:
        """List of columns for debug logging."""
        columns = getattr(frame, "columns", None)
        if columns is not None:
            return [str(c) for c in list(columns)]
        raise NotImplementedError(
            "_columns_of needs to be implemented for non-pandas frame types"
        )

    def _is_frame(self, obj: Any) -> bool:
        """Is 'obj' a valid frame for this executor?"""
        try:
            from pandas import DataFrame as _PDDataFrame  # type: ignore
        except Exception:  # pragma: no cover - pandas optional
            _PDDataFrame = tuple()  # type: ignore
        return isinstance(obj, _PDDataFrame)

    def _frame_name(self) -> str:
        """Only used when formatting error messages (default)."""
        return "a"
