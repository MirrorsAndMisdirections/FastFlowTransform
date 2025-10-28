# fastflowtransform/executors/base.py
from __future__ import annotations

import contextvars
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from contextlib import suppress
from pathlib import Path
from typing import Any, TypeVar

from jinja2 import Environment
from pandas import DataFrame as _PDDataFrame

from fastflowtransform.api import context as _http_ctx
from fastflowtransform.core import REGISTRY, Node, relation_for, resolve_source_entry
from fastflowtransform.errors import ModelExecutionError
from fastflowtransform.logging import echo_debug
from fastflowtransform.validation import validate_required_columns

# Frame type (pandas.DataFrame, pyspark.sql.DataFrame, snowflake.snowpark.DataFrame, ...)
TFrame = TypeVar("TFrame")


class _ThisProxy:
    """
    Jinja-kompatibler Proxy für {{ this }}:
    - Als String verwendbar ({{ this }}) -> physischer Relationsname.
    - Attribute verfügbar ({{ this.name }}, {{ this.materialized }}, ...)
    """

    def __init__(self, relation: str, materialized: str, schema: str | None, database: str | None):
        self.name = relation  # Back-compat: {{ this.name }}
        self.relation = relation  # Alias, falls jemand {{ this.relation }} nutzt
        self.materialized = materialized
        self.schema = schema
        self.database = database

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"_ThisProxy(name={self.name!r})"


class BaseExecutor[TFrame](ABC):
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
        _RENDER_CFG: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
            "_RENDER_CFG", default=None
        )

        def get_render_cfg() -> dict[str, Any]:
            cfg = _RENDER_CFG.get()
            if cfg is None:
                cfg = {}
                _RENDER_CFG.set(cfg)
            return cfg

        def _config_hook(**kwargs: Any) -> str:
            cfg = get_render_cfg()  # garantiert ein Dict
            cfg.update(kwargs)  # gleiche Referenz, kein erneutes set() nötig
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

        # ---- is_incremental() builtin
        # True iff materialization is 'incremental' AND the target relation already exists.
        if "is_incremental" not in env.globals:

            def _is_incremental() -> bool:
                try:
                    mat = (getattr(node, "meta", {}) or {}).get("materialized", "table")
                    if mat != "incremental":
                        return False
                    rel = relation_for(node.name)
                    return bool(self.exists_relation(rel))
                except Exception:
                    # Be conservative: if anything is off, treat as non-incremental.
                    return False

            env.globals["is_incremental"] = _is_incremental

        raw = Path(node.path).read_text(encoding="utf-8")
        tmpl = env.from_string(raw)

        def _default_ref(name: str) -> str:
            return relation_for(name)

        def _default_source(source_name: str, table_name: str) -> str:
            group = REGISTRY.sources.get(source_name)
            if not group:
                raise KeyError(f"Unknown source {source_name}.{table_name}")
            entry = group.get(table_name)
            if not entry:
                raise KeyError(f"Unknown source {source_name}.{table_name}")
            cfg = resolve_source_entry(entry, self.engine_name, default_identifier=table_name)
            if cfg.get("location"):
                raise KeyError(
                    "Path-based sources require executor context; "
                    "default resolver cannot handle them."
                )
            identifier = cfg.get("identifier")
            if not identifier:
                raise KeyError(f"Source {source_name}.{table_name} missing identifier")
            return identifier

        _RENDER_CFG.set({})

        # expose 'this' to the template: Proxy-Objekt, das wie String wirkt
        this_obj = _ThisProxy(
            relation_for(node.name),
            (getattr(node, "meta", {}) or {}).get("materialized", "table"),
            getattr(self, "schema", None) or getattr(self, "dataset", None),
            getattr(self, "database", None) or getattr(self, "project", None),
        )

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
        Orchestrate SQL models:
          1) Render Jinja (ref/source/this) and strip leading {{ config(...) }}.
          2) If the SQL is full DDL (CREATE …), execute it verbatim (passthrough).
          3) Otherwise, normalize to CREATE OR REPLACE {TABLE|VIEW} AS <body>.
             The body is CTE-aware (keeps WITH … SELECT … intact).
        On failure, raise ModelExecutionError with a helpful snippet.
        """
        sql_rendered = self.render_sql(
            node,
            env,
            ref_resolver=lambda name: self._resolve_ref(name, env),
            source_resolver=self._resolve_source,
        )
        sql = self._strip_leading_config(sql_rendered).strip()

        materialization = (node.meta or {}).get("materialized", "table")
        if materialization == "ephemeral":
            return

        # 1) Direct DDL passthrough (CREATE [OR REPLACE] {TABLE|VIEW} …)
        if self._looks_like_direct_ddl(sql):
            try:
                self._execute_sql_direct(sql, node)
                return
            except NotImplementedError:
                # Engine doesn't implement direct DDL → fall back to normalized materialization.
                pass
            except Exception as e:
                raise ModelExecutionError(
                    node_name=node.name,
                    relation=relation_for(node.name),
                    message=str(e),
                    sql_snippet=sql,
                ) from e

        # 2) Normalized materialization path (CTE-safe body)
        body = self._selectable_body(sql).rstrip(" ;\n\t")
        target_sql = self._format_relation_for_ref(node.name)

        # Centralized SQL preview logging (applies to ALL engines)
        preview = (
            f"=== MATERIALIZE ===\n"
            f"-- model: {node.name}\n"
            f"-- materialized: {materialization}\n"
            f"-- target: {target_sql}\n"
            f"{body}\n"
        )
        echo_debug(preview)

        try:
            self._apply_sql_materialization(node, target_sql, body, materialization)
        except Exception as e:
            preview = f"-- materialized={materialization}\n-- target={target_sql}\n{body}"
            raise ModelExecutionError(
                node_name=node.name,
                relation=relation_for(node.name),
                message=str(e),
                sql_snippet=preview,
            ) from e

    # --- Helpers for materialization & ephemeral inlining (instance methods) ---
    def _first_select_body(self, sql: str) -> str:
        """
        Fallback: extract the substring starting at the first SELECT token.
        If no SELECT is found, return the original string unchanged.
        Prefer using _selectable_body() which is CTE-aware.
        """
        m = re.search(r"\bselect\b", sql, flags=re.I | re.S)
        return sql[m.start() :] if m else sql

    def _strip_leading_config(self, sql: str) -> str:
        """
        Remove a leading Jinja {{ config(...) }} so the engine receives clean SQL.
        """
        return re.sub(
            r"^\s*\{\{\s*config\s*\(.*?\)\s*\}\}\s*",
            "",
            sql,
            flags=re.I | re.S,
        )

    def _strip_leading_sql_comments(self, sql: str) -> tuple[str, int]:
        """
        Remove *only* leading SQL comments and blank lines, return (trimmed_sql, start_idx).

        Supports:
          -- single line comments
          /* block comments */
        """
        # Match chain of: whitespace, comment, whitespace, comment, ...
        # Using DOTALL so block comments spanning lines are handled.
        pat = re.compile(
            r"""^\s*(?:
                                --[^\n]*\n        # line comment
                              | /\*.*?\*/\s*      # block comment
                             )*""",
            re.VERBOSE | re.DOTALL,
        )
        m = pat.match(sql)
        start = m.end() if m else 0
        return sql[start:], start

    def _selectable_body(self, sql: str) -> str:
        """
        Return a valid SELECT-able body for CREATE … AS:

          - If the statement starts (after comments/blank lines) with a CTE (WITH …),
            return from the WITH onward.
          - If it starts with SELECT, return from SELECT onward.
          - Otherwise, fall back to the first SELECT heuristic.
        """
        # Keep original for fallback, but check after stripping comments
        s0 = sql
        s, offset = self._strip_leading_sql_comments(sql)
        s_ws = s.lstrip()  # in case comments left some spaces
        head = s_ws[:6].lower()

        if s_ws.startswith(("with ", "with\n", "with\t")):
            # Return from the start of this WITH (preserve exactly s_ws form)
            # Compute index into original string to retain original casing beyond comments
            idx = s.find(s_ws)
            return sql[offset + (idx if idx >= 0 else 0) :].lstrip()

        if head.startswith("select"):
            idx = s.find(s_ws)
            return sql[offset + (idx if idx >= 0 else 0) :].lstrip()

        # Fallback: first SELECT anywhere in the statement
        return self._first_select_body(s0)

    def _looks_like_direct_ddl(self, sql: str) -> bool:
        """
        True if the rendered SQL starts with CREATE (TABLE|VIEW) so it should be
        executed verbatim as a user-provided DDL statement.
        """
        head = sql.lstrip().lower()
        return (
            head.startswith("create table")
            or head.startswith("create view")
            or head.startswith("create or replace")
        )

    def _execute_sql_direct(self, sql: str, node: Node) -> None:
        """
        Execute a full CREATE … statement as-is. Default: use `self.con.execute(sql)`.
        Engines can override this for custom dispatch. If not available, raise
        NotImplementedError so the caller can fall back to normalized materialization.
        """
        con = getattr(self, "con", None)
        if con is None or not hasattr(con, "execute"):
            raise NotImplementedError("Direct DDL execution is not implemented for this executor.")
        con.execute(sql)

    def _render_ephemeral_sql(self, name: str, env: Environment) -> str:
        """
        Render the SQL for an 'ephemeral' model and return it as a parenthesized
        subquery. This is CTE-safe: we keep the full WITH…SELECT… statement and
        only strip the leading {{ config(...) }} and trailing semicolons.
        """
        node = REGISTRY.get_node(name) if hasattr(REGISTRY, "get_node") else REGISTRY.nodes[name]

        raw = Path(node.path).read_text(encoding="utf-8")
        tmpl = env.from_string(raw)

        sql = tmpl.render(
            ref=lambda n: self._resolve_ref(n, env),
            source=self._resolve_source,
            this=_ThisProxy(
                relation_for(node.name),
                (getattr(node, "meta", {}) or {}).get("materialized", "table"),
                getattr(self, "schema", None) or getattr(self, "dataset", None),
                getattr(self, "database", None) or getattr(self, "project", None),
            ),
        )
        # Remove a leading config block and keep the full, CTE-capable statement
        sql = self._strip_leading_config(sql).strip()
        body = self._selectable_body(sql).rstrip(" ;\n\t")
        return f"(\n{body}\n)"

    # ---------- Python models ----------
    def run_python(self, node: Node) -> None:
        func = REGISTRY.py_funcs[node.name]
        deps = REGISTRY.nodes[node.name].deps or []
        if _http_ctx is not None:
            with suppress(Exception):
                _http_ctx.reset_for_node(node.name)

        # Load inputs
        arg: Any
        if len(deps) == 0:
            arg = None
        elif len(deps) == 1:
            rel = relation_for(deps[0])
            df_in: TFrame = self._read_relation(rel, node, deps)
            arg = df_in  # TFrame
        else:
            frames: dict[str, TFrame] = {}
            for dep in deps:
                rel = relation_for(dep)
                f = self._read_relation(rel, node, deps)
                frames[rel] = f
            arg = frames  # dict[str, TFrame]

        # Validate required columns / structure (frame specific)
        requires = REGISTRY.py_requires.get(node.name, {})
        if deps:
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

        if _http_ctx is not None:
            try:
                snap = _http_ctx.snapshot()
                (node.meta or {}).update({"_http_snapshot": snap})
            except Exception:
                pass

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
        group = REGISTRY.sources.get(source_name)
        if not group:
            known = ", ".join(sorted(REGISTRY.sources.keys())) or "<none>"
            raise KeyError(f"Unknown source '{source_name}'. Known sources: {known}")

        entry = group.get(table_name)
        if not entry:
            known_tables = ", ".join(sorted(group.keys())) or "<none>"
            raise KeyError(
                f"Unknown source table '{source_name}.{table_name}'. Known tables: {known_tables}"
            )

        engine_key = self.engine_name
        try:
            cfg = resolve_source_entry(entry, engine_key, default_identifier=table_name)
        except KeyError as exc:
            raise KeyError(
                f"Source {source_name}.{table_name} missing "
                f"identifier/location for engine '{engine_key}'"
            ) from exc

        cfg = dict(cfg)
        cfg.setdefault("options", {})
        return self._format_source_reference(cfg, source_name, table_name)

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

        validate_required_columns(node_name, inputs, requires)

    def _columns_of(self, frame: TFrame) -> list[str]:
        """List of columns for debug logging."""
        columns = getattr(frame, "columns", None)
        if columns is not None:
            return [str(c) for c in list(columns)]
        raise NotImplementedError("_columns_of needs to be implemented for non-pandas frame types")

    def _is_frame(self, obj: Any) -> bool:
        """Is 'obj' a valid frame for this executor?"""
        return isinstance(obj, _PDDataFrame)

    def _frame_name(self) -> str:
        """Only used when formatting error messages (default)."""
        return "a"

    # ---------- Build meta hook ----------
    def on_node_built(self, node: Node, relation: str, fingerprint: str) -> None:
        """
        Hook invoked after a node has been successfully materialized.
        Engines should override this to write/update the meta table (e.g. _ff_meta).

        Default: no-op.
        """
        return

    # ── Incremental API ───────────────────────────────────────────────
    def exists_relation(self, relation: str) -> bool:  # pragma: no cover - abstract
        """Returns True if physical relation exists (table/view)."""
        raise NotImplementedError

    def create_table_as(self, relation: str, select_sql: str) -> None:  # pragma: no cover
        """CREATE TABLE AS SELECT …"""
        raise NotImplementedError

    def incremental_insert(self, relation: str, select_sql: str) -> None:  # pragma: no cover
        """INSERT-only (Append)."""
        raise NotImplementedError

    def incremental_merge(
        self, relation: str, select_sql: str, unique_key: list[str]
    ) -> None:  # pragma: no cover
        """Best-effort UPSERT; Default fallback via staging delete+insert."""
        raise NotImplementedError

    def alter_table_sync_schema(
        self, relation: str, select_sql: str, *, mode: str = "append_new_columns"
    ) -> None:  # pragma: no cover
        """
        Optional: Additive schema synchronisation. 'mode' = append_new_columns|sync_all_columns.
        Default implementation: No-Op.
        """
        return None

    ENGINE_NAME: str = "generic"

    @property
    def engine_name(self) -> str:
        return getattr(self, "ENGINE_NAME", "generic")
