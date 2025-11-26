# fastflowtransform/cli/run.py
from __future__ import annotations

import os
import textwrap
import threading
import traceback
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import typer

from fastflowtransform.artifacts import (
    RunNodeResult,
    load_last_run_durations,
    write_catalog,
    write_manifest,
    write_run_results,
)
from fastflowtransform.cache import FingerprintCache, can_skip_node
from fastflowtransform.ci.changed_since import (
    compute_affected_models,
    get_changed_models,
)
from fastflowtransform.cli.bootstrap import CLIContext, _prepare_context
from fastflowtransform.cli.options import (
    CacheMode,
    CacheOpt,
    ChangedSinceOpt,
    EngineOpt,
    EnvOpt,
    ExcludeOpt,
    HttpCacheOpt,
    JobsOpt,
    KeepOpt,
    NoCacheOpt,
    OfflineOpt,
    ProjectArg,
    RebuildAllOpt,
    RebuildOnlyOpt,
    SelectOpt,
    VarsOpt,
)
from fastflowtransform.cli.selectors import (
    _compile_selector,
    _parse_select,
    _selected_subgraph_names,
    augment_with_state_modified,
)
from fastflowtransform.config.budgets import (
    BudgetLimit,
    BudgetsConfig,
    load_budgets_config,
)
from fastflowtransform.core import REGISTRY, relation_for
from fastflowtransform.dag import levels as dag_levels
from fastflowtransform.executors.budget import format_bytes
from fastflowtransform.fingerprint import (
    EnvCtx,
    build_env_ctx,
    fingerprint_py,
    fingerprint_sql,
    get_function_source,
)
from fastflowtransform.log_queue import LogQueue
from fastflowtransform.logging import bind_context, bound_context, clear_context, echo, error, warn
from fastflowtransform.meta import ensure_meta_table
from fastflowtransform.run_executor import ScheduleResult, schedule
from fastflowtransform.utils.timefmt import _format_duration_ms


@dataclass
class _RunEngine:
    ctx: Any
    env_name: str
    pred: Callable[[Any], bool] | None
    cache_mode: CacheMode
    force_rebuild: set[str] = field(default_factory=set)
    shared: tuple[Any, Callable, Callable] = field(init=False)
    tls: threading.local = field(default_factory=threading.local, init=False)
    cache: FingerprintCache = field(init=False)
    env_ctx: EnvCtx = field(init=False)
    computed_fps: dict[str, str] = field(default_factory=dict, init=False)
    fps_lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    http_snaps: dict[str, dict] = field(default_factory=dict, init=False)
    budgets_cfg: BudgetsConfig | None = None

    # per-node query stats (aggregated across all queries in that node)
    query_stats: dict[str, dict[str, int]] = field(default_factory=dict, init=False)
    stats_lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def __post_init__(self) -> None:
        echo(f"Profile: {self.env_name} | Engine: {self.ctx.profile.engine}")
        self.shared = self.ctx.make_executor()
        self._configure_budget_limit(self.shared[0])
        with suppress(Exception):
            ensure_meta_table(self.shared[0])
        relevant_env = [k for k in os.environ if k.startswith("FF_")]
        self.env_ctx = build_env_ctx(
            engine=self.ctx.profile.engine,
            profile_name=self.env_name,
            relevant_env_keys=relevant_env,
            sources=getattr(REGISTRY, "sources", {}),
        )
        self.cache = FingerprintCache(
            self.ctx.project, profile=self.env_name, engine=self.ctx.profile.engine
        )
        self.cache.load()

    def _get_runner(self) -> tuple[Any, Callable, Callable]:
        if getattr(self.tls, "runner", None) is None:
            ex, run_sql_shared, run_py_shared = self.shared
            run_sql_wrapped, run_py_wrapped = run_sql_shared, run_py_shared
            if self.ctx.profile.engine == "duckdb" and hasattr(ex, "clone"):
                try:
                    db_path = getattr(ex, "db_path", None)
                    clone_needed = not (isinstance(db_path, str) and db_path.strip() == ":memory:")
                    if clone_needed:
                        ex = ex.clone()

                        def run_sql_wrapped(node, _env=self.ctx.jinja_env, _ex=ex):
                            return _ex.run_sql(node, _env)

                        run_py_wrapped = ex.run_python
                except Exception:
                    pass
            self._configure_budget_limit(ex)
            self.tls.runner = (ex, run_sql_wrapped, run_py_wrapped)
        return self.tls.runner

    def _maybe_fingerprint(self, node: Any, ex: Any) -> str | None:
        supports_sql_fp = all(
            hasattr(ex, a) for a in ("render_sql", "_resolve_ref", "_resolve_source")
        )
        if not (supports_sql_fp or node.kind == "python"):
            return None
        with self.fps_lock:
            dep_fps = {
                d: self.computed_fps.get(d) or self.cache.get(d) or "" for d in (node.deps or [])
            }
        try:
            if node.kind == "sql" and supports_sql_fp:
                rendered = ex.render_sql(
                    node,
                    self.ctx.jinja_env,
                    ref_resolver=lambda nm: ex._resolve_ref(nm, self.ctx.jinja_env),
                    source_resolver=ex._resolve_source,
                )
                return fingerprint_sql(
                    node=node, rendered_sql=rendered, env_ctx=self.env_ctx, dep_fps=dep_fps
                )
            if node.kind == "python":
                func = REGISTRY.py_funcs[node.name]
                src = get_function_source(func)
                return fingerprint_py(
                    node=node, func_src=src, env_ctx=self.env_ctx, dep_fps=dep_fps
                )
        except Exception:
            return None
        return None

    def _executor_namespace(self) -> str | None:
        """
        Best-effort namespace (catalog/database/schema/dataset) to enrich log output.
        """
        if not isinstance(self.shared, tuple) or not self.shared:
            return None
        executor = self.shared[0]
        if executor is None:
            return None
        parts: list[str] = []
        for attr in ("catalog", "database"):
            val = getattr(executor, attr, None)
            if isinstance(val, str) and val.strip():
                parts.append(val.strip())
        for attr in ("dataset", "schema"):
            val = getattr(executor, attr, None)
            if isinstance(val, str) and val.strip():
                parts.append(val.strip())
        return ".".join(parts) if parts else None

    def _qualified_target(self, name: str) -> str | None:
        namespace = self._executor_namespace()
        if not namespace:
            return None
        rel = relation_for(name)
        if not rel:
            return None
        return f"{namespace}.{rel}"

    def _configure_budget_limit(self, executor: Any) -> None:
        if executor is None or not hasattr(executor, "configure_query_budget_limit"):
            return
        engine_name = (self.ctx.profile.engine or "").lower()
        limit = None
        if self.budgets_cfg and engine_name:
            entry = self.budgets_cfg.query_limits.get(engine_name)
            if entry:
                limit = entry.max_bytes
        executor.configure_query_budget_limit(limit)

    def format_run_label(self, name: str) -> str:
        """
        Build the human-facing label for run logs, e.g.:
          fct_events_sql_inline.ff [delta] (catalog.schema.fct_events_sql_inline)

        The storage format is resolved from:
          1) per-model storage config (project.yml → models.storage / meta.storage),
          2) engine defaults (e.g. Databricks/Spark table_format) as a fallback.

        For database engines like DuckDB/Postgres we intentionally hide the
        underlying storage format (e.g. 'parquet') to avoid confusing output.
        """
        qualified = self._qualified_target(name)
        engine = (self.ctx.profile.engine or "").lower()

        # 1) per-model storage.format from meta (preferred)
        fmt_from_meta: str | None = None
        try:
            node = REGISTRY.get_node(name)
            meta = getattr(node, "meta", {}) or {}
            storage_cfg = meta.get("storage") or {}
            if isinstance(storage_cfg, dict):
                val = storage_cfg.get("format")
                if isinstance(val, str) and val.strip():
                    fmt_from_meta = val.strip()
        except Exception:
            fmt_from_meta = None

        fmt: str | None = fmt_from_meta

        # 2) engine-level default format (e.g. Spark table_format) as fallback.
        # Only meaningful for Spark-like engines.
        if fmt is None and engine in {"databricks_spark", "spark"}:
            try:
                executor, _, _ = self.shared
                default_fmt = getattr(executor, "spark_table_format", None)
                if isinstance(default_fmt, str) and default_fmt.strip():
                    fmt = default_fmt.strip()
            except Exception:
                fmt = None

        # For database engines (DuckDB/Postgres/BigQuery), we do not show a format suffix
        # at all to avoid misleading '[parquet]' labels (these engines don't expose
        # a user-selectable table file format in FFT).
        if engine in {"duckdb", "postgres", "postgresql", "bigquery"}:
            fmt_suffix = ""
        else:
            fmt_suffix = f" [{fmt}]" if fmt else ""

        if qualified:
            return f"{name}{fmt_suffix} ({qualified})"
        return f"{name}{fmt_suffix}"

    def run_node(self, name: str) -> None:
        node = REGISTRY.nodes[name]
        ex, run_sql_fn, run_py_fn = self._get_runner()

        # Reset per-node stats if the executor supports it
        with suppress(Exception):
            reset = getattr(ex, "reset_node_stats", None)
            if callable(reset):
                reset()

        if name in self.force_rebuild:
            (run_sql_fn if node.kind == "sql" else run_py_fn)(node)
            cand_fp = self._maybe_fingerprint(node, ex)
            if cand_fp:
                with self.fps_lock:
                    self.computed_fps[name] = cand_fp
                with suppress(Exception):
                    ex.on_node_built(node, relation_for(name), cand_fp)
            # HTTP snapshot (stored in node.meta by the executor)
            with suppress(Exception):
                snap = (getattr(node, "meta", {}) or {}).get("_http_snapshot")
                if snap:
                    self.http_snaps[name] = snap

            # capture per-node stats after successful run
            with suppress(Exception):
                raw_getter = getattr(ex, "get_node_stats", None)
                if callable(raw_getter):
                    getter = cast(Callable[[], dict[str, int] | None], raw_getter)
                    stats = getter()
                    if stats:
                        with self.stats_lock:
                            self.query_stats[name] = stats

            return

        cand_fp = self._maybe_fingerprint(node, ex)
        if cand_fp is not None:
            materialized = (getattr(node, "meta", {}) or {}).get("materialized", "table")
            may_skip = self.cache_mode in (CacheMode.RW, CacheMode.RO)
            if may_skip and can_skip_node(
                node_name=name,
                new_fp=cand_fp,
                cache=self.cache,
                executor=ex,
                materialized=materialized,
            ):
                with self.fps_lock:
                    self.computed_fps[name] = cand_fp
                # Even when skipped, we treat stats as zero → nothing to do
                with self.stats_lock:
                    self.query_stats[name] = {
                        "bytes_scanned": 0,
                        "rows": 0,
                        "query_duration_ms": 0,
                        "cached": True,
                    }
                return

        (run_sql_fn if node.kind == "sql" else run_py_fn)(node)

        if cand_fp is not None:
            with self.fps_lock:
                self.computed_fps[name] = cand_fp
            with suppress(Exception):
                ex.on_node_built(node, relation_for(name), cand_fp)
        # HTTP snapshot (stored in node.meta by the executor)
        with suppress(Exception):
            snap = (getattr(node, "meta", {}) or {}).get("_http_snapshot")
            if snap:
                self.http_snaps[name] = snap

        # capture per-node stats after successful run
        with suppress(Exception):
            raw_getter = getattr(ex, "get_node_stats", None)
            if callable(raw_getter):
                getter = cast(Callable[[], dict[str, int] | None], raw_getter)
                stats = getter()
                if stats:
                    with self.stats_lock:
                        self.query_stats[name] = stats

    @staticmethod
    def before(_name: str, lvl_idx: int | None = None) -> None:
        return

    @staticmethod
    def on_error(name: str, err: BaseException) -> None:
        _node = REGISTRY.get_node(name)
        if isinstance(err, KeyError):
            echo(
                _error_block(
                    f"Model failed: {name} (KeyError)",
                    _pretty_exc(err),
                    "• Check column names in your upstream tables (seeds/SQL).\n"
                    "• For >1 deps: dict keys are physical relations (relation_for), "
                    "e.g. 'orders'.\n"
                    "• (Optional) Log input columns in the executor before the call.",
                )
            )
            raise typer.Exit(1) from err
        body = _pretty_exc(err)
        if os.getenv("FFT_TRACE") == "1":
            body += "\n\n" + traceback.format_exc()
        echo(_error_block(f"Model failed: {name}", body, "• See cause above."))
        raise typer.Exit(1) from err

    def persist_on_success(self, result: ScheduleResult) -> None:
        if not result.failed and (self.cache_mode in (CacheMode.RW, CacheMode.WO)):
            self.cache.update_many(self.computed_fps)
            self.cache.save()

    @staticmethod
    def print_timings(result: ScheduleResult) -> None:
        if not result.per_node_s:
            return
        echo("\nRuntime per model")
        echo("─────────────────")
        for name in sorted(result.per_node_s, key=lambda k: k):
            ms = int(result.per_node_s[name] * 1000)
            echo(f"• {name:<30} {ms:>6} ms")
        echo(f"\nTotal runtime: {result.total_s:.3f}s")


def _pretty_exc(e: BaseException) -> str:
    return "".join(traceback.format_exception_only(type(e), e)).strip()


def _error_block(title: str, body: str, hint: str | None = None) -> str:
    border = "─" * 70
    lines = [f"✖ {title}", "", textwrap.dedent(body).rstrip()]
    if hint:
        lines += ["", "Hints:", textwrap.dedent(hint).rstrip()]
    text = "│ \n│ ".join("\n".join(lines).splitlines())
    return f"\n┌{border}\n{text}\n└{border}\n"


def _normalize_node_names_or_warn(names: list[str] | None) -> set[str]:
    out: set[str] = set()
    for tok in _parse_select(names or []):
        try:
            node = REGISTRY.get_node(tok)
        except KeyError:
            warn(f"Unknown model in --rebuild: {tok}")
            continue

        if _is_snapshot_model(node):
            warn(
                f"Ignoring snapshot model in --rebuild: {tok} "
                "(snapshot models are not executed via 'fft run'; "
                "use 'fft snapshot run' instead)."
            )
            continue

        out.add(node.name)

    return out


def _abbr(e: str) -> str:
    mapping = {
        "duckdb": "DUCK",
        "postgres": "PG",
        "bigquery": "BQ",
        "databricks_spark": "SPK",
        "snowflake_snowpark": "SNOW",
    }
    return mapping.get(e, e.upper()[:4])


def _is_snapshot_model(node: Any) -> bool:
    """
    Return True if this node is a snapshot model (materialized='snapshot').
    """
    meta = getattr(node, "meta", {}) or {}
    mat = str(meta.get("materialized") or "").lower()
    return mat == "snapshot"


def _check_metric_limits(
    *,
    scope: str,
    metric_name: str,
    value: int,
    limits: BudgetLimit,
) -> tuple[bool, bool]:
    """
    Check a single metric against warn/error thresholds.

    Returns (warn_triggered, error_triggered).
    """
    if value <= 0:
        return False, False

    # Decide how to render the metric & thresholds
    if metric_name == "bytes_scanned":
        value_str = format_bytes(value)
        warn_str = format_bytes(limits.warn) if limits.warn else None
        err_str = format_bytes(limits.error) if limits.error else None
        unit_label = "bytes_scanned"
    elif metric_name == "rows":
        value_str = f"{value:,} rows"
        warn_str = f"{limits.warn:,} rows" if limits.warn else None
        err_str = f"{limits.error:,} rows" if limits.error else None
        unit_label = "rows"
    else:  # "query_duration_ms"
        value_str = _format_duration_ms(value)
        warn_str = _format_duration_ms(limits.warn) if limits.warn else None
        err_str = _format_duration_ms(limits.error) if limits.error else None
        unit_label = "query_duration_ms"

    # Prefer error over warn (avoid double-logging)
    if limits.error and value > limits.error:
        error(
            f"[BUDGET] {scope}: {unit_label} {value_str} exceeds "
            f"error limit {err_str} (budgets.yml)."
        )
        return False, True

    if limits.warn and value > limits.warn:
        warn(
            f"[BUDGET] {scope}: {unit_label} {value_str} exceeds "
            f"warn limit {warn_str} (budgets.yml)."
        )
        return True, False

    return False, False


def _value_and_limits_str(
    metric_name: str,
    value: int,
    limits: BudgetLimit,
) -> tuple[str, str | None, str | None]:
    if metric_name == "bytes_scanned":
        v_str = format_bytes(value)
        w_str = format_bytes(limits.warn) if limits.warn else None
        e_str = format_bytes(limits.error) if limits.error else None
    elif metric_name == "rows":
        v_str = f"{value:,} rows"
        w_str = f"{limits.warn:,} rows" if limits.warn else None
        e_str = f"{limits.error:,} rows" if limits.error else None
    else:  # query_duration_ms
        v_str = _format_duration_ms(value)
        w_str = _format_duration_ms(limits.warn) if limits.warn else None
        e_str = _format_duration_ms(limits.error) if limits.error else None
    return v_str, w_str, e_str


def _eval_metric(scope: str, metric_name: str, value: int, limits: BudgetLimit) -> str:
    """
    Evaluate {warn,error} thresholds for a single metric.

    Returns status: "ok" | "warn" | "error".
    Emits log lines for warn/error.
    """
    if value <= 0:
        return "ok"
    if not limits.warn and not limits.error:
        return "ok"

    v_str, w_str, e_str = _value_and_limits_str(metric_name, value, limits)
    unit_label = metric_name

    # Prefer error over warn
    if limits.error and value > limits.error:
        error(f"[BUDGET] {scope}: {unit_label} {v_str} exceeds error limit {e_str} (budgets.yml).")
        return "error"

    if limits.warn and value > limits.warn:
        warn(f"[BUDGET] {scope}: {unit_label} {v_str} exceeds warn limit {w_str} (budgets.yml).")
        return "warn"

    return "ok"


def _aggregate_totals(stats_by_model: dict[str, dict[str, Any]]) -> dict[str, int]:
    totals = {"bytes_scanned": 0, "rows": 0, "query_duration_ms": 0}
    for s in stats_by_model.values():
        totals["bytes_scanned"] += int(s.get("bytes_scanned", 0) or 0)
        totals["rows"] += int(s.get("rows", 0) or 0)
        totals["query_duration_ms"] += int(s.get("query_duration_ms", 0) or 0)
    return totals


def _evaluate_total_budgets(
    cfg: Any,
    totals: dict[str, int],
    budgets_summary: dict[str, Any],
) -> bool:
    had_error = False
    if not cfg.total:
        return had_error

    for metric_name in ("bytes_scanned", "rows", "query_duration_ms"):
        limits: BudgetLimit | None = getattr(cfg.total, metric_name)
        if not limits:
            continue
        value = totals.get(metric_name, 0)
        status = _eval_metric("total (all models)", metric_name, value, limits)
        if status != "ok" or (limits.warn or limits.error):
            budgets_summary["total"][metric_name] = {
                "value": value,
                "warn": limits.warn,
                "error": limits.error,
                "status": status,
            }
        if status == "error":
            had_error = True

    return had_error


def _evaluate_model_budgets(
    cfg: Any,
    stats_by_model: dict[str, dict[str, Any]],
    budgets_summary: dict[str, Any],
) -> bool:
    had_error = False

    for model_name, metrics in (cfg.models or {}).items():
        s = stats_by_model.get(model_name)
        if not s:
            continue

        model_summary: dict[str, Any] = {}
        for metric_name in ("bytes_scanned", "rows", "query_duration_ms"):
            limits: BudgetLimit | None = getattr(metrics, metric_name)
            if not limits:
                continue
            value = int(s.get(metric_name, 0) or 0)
            status = _eval_metric(f"model '{model_name}'", metric_name, value, limits)
            if status != "ok" or (limits.warn or limits.error):
                model_summary[metric_name] = {
                    "value": value,
                    "warn": limits.warn,
                    "error": limits.error,
                    "status": status,
                }
            if status == "error":
                had_error = True

        if model_summary:
            budgets_summary["models"][model_name] = model_summary

    return had_error


def _aggregate_tag_totals(
    cfg: Any,
    stats_by_model: dict[str, dict[str, Any]],
) -> dict[str, dict[str, int]]:
    tag_totals: dict[str, dict[str, int]] = {
        tag: {"bytes_scanned": 0, "rows": 0, "query_duration_ms": 0} for tag in (cfg.tags or {})
    }

    for model_name, s in stats_by_model.items():
        try:
            node = REGISTRY.get_node(model_name)
        except KeyError:
            continue

        meta = getattr(node, "meta", {}) or {}
        tags = meta.get("tags") or []
        tags_str = [str(t) for t in tags]

        for tag in tags_str:
            if tag not in tag_totals:
                continue
            aggr = tag_totals[tag]
            aggr["bytes_scanned"] += int(s.get("bytes_scanned", 0) or 0)
            aggr["rows"] += int(s.get("rows", 0) or 0)
            aggr["query_duration_ms"] += int(s.get("query_duration_ms", 0) or 0)

    return tag_totals


def _evaluate_tag_budgets(
    cfg: Any,
    tag_totals: dict[str, dict[str, int]],
    budgets_summary: dict[str, Any],
) -> bool:
    had_error = False
    if not cfg.tags:
        return had_error

    for tag, metrics in (cfg.tags or {}).items():
        aggr = tag_totals.get(tag) or {}
        tag_summary: dict[str, Any] = {}
        for metric_name in ("bytes_scanned", "rows", "query_duration_ms"):
            limits: BudgetLimit | None = getattr(metrics, metric_name)
            if not limits:
                continue
            value = int(aggr.get(metric_name, 0) or 0)
            status = _eval_metric(
                f"tag '{tag}' (all models with this tag)", metric_name, value, limits
            )
            if status != "ok" or (limits.warn or limits.error):
                tag_summary[metric_name] = {
                    "value": value,
                    "warn": limits.warn,
                    "error": limits.error,
                    "status": status,
                }
            if status == "error":
                had_error = True

        if tag_summary:
            budgets_summary["tags"][tag] = tag_summary

    return had_error


def _resolve_budgets_cfg(project_dir: Path, engine_: _RunEngine) -> Any | None:
    cfg = engine_.budgets_cfg
    if cfg is not None:
        return cfg

    try:
        cfg = load_budgets_config(project_dir)
    except Exception as exc:  # pragma: no cover - CLI error path
        # Parsing error is considered fatal: surface a clear message and fail the run.
        error(f"Failed to parse budgets.yml: {exc}")
        raise typer.Exit(1) from exc

    engine_.budgets_cfg = cfg
    return cfg


def _evaluate_budgets(
    project_dir: Path,
    engine_: _RunEngine,
) -> tuple[bool, dict[str, Any] | None]:
    """
    Enforce budgets.yml against collected query_stats.

    Returns:
      (had_error_budget: bool, budgets_summary: dict | None)
    """
    cfg = _resolve_budgets_cfg(project_dir, engine_)

    if cfg is None:
        # No budgets.yml → nothing to enforce
        return False, None

    stats_by_model: dict[str, dict[str, Any]] = getattr(engine_, "query_stats", {}) or {}
    if not stats_by_model:
        # No stats collected (e.g. purely Python models) → nothing to enforce
        return False, None

    totals = _aggregate_totals(stats_by_model)

    # Summary structure for run_results.json
    budgets_summary: dict[str, Any] = {
        "total": {},
        "models": {},
        "tags": {},
    }

    had_error = False
    had_error |= _evaluate_total_budgets(cfg, totals, budgets_summary)
    had_error |= _evaluate_model_budgets(cfg, stats_by_model, budgets_summary)

    if cfg.tags:
        tag_totals = _aggregate_tag_totals(cfg, stats_by_model)
        had_error |= _evaluate_tag_budgets(cfg, tag_totals, budgets_summary)

    # If budgets_summary is entirely empty, return None for clarity
    if not any(budgets_summary[section] for section in ("total", "models", "tags")):
        return had_error, None

    return had_error, budgets_summary


# ----------------- helpers (run function) -----------------


def _build_engine_ctx(project, env_name, engine, vars, cache, no_cache):
    ctx = _prepare_context(project, env_name, engine, vars)
    cache_mode = CacheMode.OFF if no_cache else cache
    engine_ = _RunEngine(
        ctx=ctx,
        pred=None,
        env_name=env_name,
        cache_mode=cache_mode,
        force_rebuild=set(),
        budgets_cfg=ctx.budgets_cfg,
    )
    return ctx, engine_


def _select_predicate_and_raw(
    executor_engine: _RunEngine,
    ctx: CLIContext,
    select: SelectOpt,
    *,
    include_snapshots: bool = False,
) -> tuple[list[str], Callable[[Any], bool], list[str]]:
    select_tokens = _parse_select(select or [])
    base_tokens = [t for t in select_tokens if not t.startswith("state:modified")]
    _, base_pred = _compile_selector(base_tokens)

    select_pred = base_pred
    if select_tokens and any(t.startswith("state:modified") for t in select_tokens):
        executor = executor_engine.shared[0]
        modified_set = executor_engine.cache.modified_set(ctx.jinja_env, executor)
        select_pred = augment_with_state_modified(select_tokens, base_pred, modified_set)

    raw_selected = []
    for k, v in REGISTRY.nodes.items():
        if not select_pred(v):
            continue
        if not include_snapshots and _is_snapshot_model(v):
            continue
        raw_selected.append(k)
    return select_tokens, select_pred, raw_selected


def _wanted_names(
    select_tokens: list[str], exclude: ExcludeOpt, raw_selected: list[str]
) -> set[str]:
    return _selected_subgraph_names(
        REGISTRY.nodes,
        select_tokens=select_tokens,
        exclude_tokens=exclude,
        seed_names=set(raw_selected),
    )


def _apply_changed_since_filter(
    ctx: CLIContext,
    wanted: set[str],
    select: SelectOpt,
    exclude: ExcludeOpt,
    changed_since: str | None,
) -> set[str]:
    """
    If --changed-since is provided, restrict the selection to models whose
    files changed since the given git ref PLUS their upstream/downstream
    neighbors.

    Semantics:
      - Without --select/--exclude:
          wanted = affected_models
      - With --select or --exclude:
          wanted = wanted ∩ affected_models

      (So you can combine tag/namespace selectors with --changed-since.)
    """
    if not changed_since:
        return wanted

    project_dir = ctx.project
    if not isinstance(project_dir, Path):
        project_dir = Path(project_dir)

    changed = get_changed_models(project_dir, changed_since)
    affected = compute_affected_models(changed, REGISTRY.nodes)

    if not affected:
        # Nothing affected by changes → nothing to run
        return set()

    # If user also provided selectors/excludes, intersect with those.
    if (select and len(select) > 0) or (exclude and len(exclude) > 0):
        return wanted & affected

    # No further selectors → affected models define the universe
    return affected


def _explicit_targets(
    rebuild_only: RebuildOnlyOpt, rebuild: bool, select: SelectOpt, raw_selected: list[str]
) -> list[str]:
    rebuild_only_names = _normalize_node_names_or_warn(rebuild_only)
    if rebuild_only_names:
        return [n for n in (rebuild_only or []) if n in REGISTRY.nodes]
    if rebuild and select:
        return raw_selected
    return []


def _maybe_exit_if_empty(wanted: set[str], explicit_targets: list[str]) -> None:
    if not wanted and not explicit_targets:
        typer.secho(
            "Nothing to run (empty selection after applying --select/--exclude).", fg="yellow"
        )
        raise typer.Exit(0)


def _compute_force_rebuild(
    explicit_targets: list[str], rebuild: bool, wanted: set[str]
) -> set[str]:
    if explicit_targets:
        return set(explicit_targets)
    if rebuild:
        return set(wanted)
    return set()


def _levels_for_run(explicit_targets: list[str], wanted: set[str]) -> list[list[str]]:
    if explicit_targets:
        return [explicit_targets]
    sub_nodes = {k: v for k, v in REGISTRY.nodes.items() if k in wanted}
    return dag_levels(sub_nodes)


def _run_schedule(engine_, lvls, jobs, keep_going, ctx):
    logq = LogQueue()
    started_at = datetime.now(UTC).isoformat(timespec="seconds")

    bind_context(run_id=started_at)

    # Best-effort: use previous run timings to batch small models per worker.
    try:
        prev_durations_s = load_last_run_durations(ctx.project)
    except Exception:
        prev_durations_s = {}

    def _run_node_with_ctx(name: str) -> None:
        with bound_context(node=name):
            engine_.run_node(name)

    result = schedule(
        lvls,
        jobs=jobs,
        fail_policy="keep_going" if keep_going else "fail_fast",
        run_node=_run_node_with_ctx,
        before=engine_.before,
        on_error=None,
        logger=logq,
        engine_abbr=_abbr(ctx.profile.engine),
        name_width=100,
        name_formatter=engine_.format_run_label,
        durations_s=prev_durations_s,
    )

    finished_at = datetime.now(UTC).isoformat(timespec="seconds")
    return result, logq, started_at, finished_at


def _write_artifacts(
    ctx: CLIContext,
    result: ScheduleResult,
    started_at: str,
    finished_at: str,
    engine_: _RunEngine,
    budgets: dict[str, Any] | None,
) -> None:
    write_manifest(ctx.project)

    node_results: list[RunNodeResult] = []
    failed = result.failed or {}
    all_names = set(result.per_node_s.keys()) | set(failed.keys())

    # stats accumulated by the executor per node (if available)
    per_node_stats = getattr(engine_, "query_stats", {}) or {}

    for name in sorted(all_names):
        dur_s = float(result.per_node_s.get(name, 0.0))
        status = "error" if name in failed else "success"
        msg = str(failed.get(name)) if name in failed else None

        stats = per_node_stats.get(name) or {}
        bytes_scanned = int(stats.get("bytes_scanned", 0))
        rows = int(stats.get("rows", 0))
        q_ms = int(stats.get("query_duration_ms", 0))

        node_results.append(
            RunNodeResult(
                name=name,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=int(dur_s * 1000),
                message=msg,
                http=engine_.http_snaps.get(name),
                bytes_scanned=bytes_scanned,
                rows=rows,
                query_duration_ms=q_ms,
            )
        )

    write_run_results(
        ctx.project,
        started_at=started_at,
        finished_at=finished_at,
        node_results=node_results,
        budgets=budgets,
    )


def _attempt_catalog(ctx: CLIContext) -> None:
    try:
        execu, _, _ = ctx.make_executor()
        write_catalog(ctx.project, execu)
    except Exception:
        pass


def _emit_logs_and_errors(logq: LogQueue, result: ScheduleResult, engine_: _RunEngine) -> None:
    for line in logq.drain():
        echo(line)
    if result.failed:
        for name, err in result.failed.items():
            engine_.on_error(name, err)


# ----------------- run function -----------------


def run(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
    select: SelectOpt = None,
    exclude: ExcludeOpt = None,
    jobs: JobsOpt = "1",
    keep_going: KeepOpt = False,
    cache: CacheOpt = CacheMode.RW,
    no_cache: NoCacheOpt = False,
    rebuild: RebuildAllOpt = False,
    rebuild_only: RebuildOnlyOpt = None,
    offline: OfflineOpt = False,
    http_cache: HttpCacheOpt = None,
    changed_since: ChangedSinceOpt = None,
) -> None:
    # HTTP/API-Flags → ENV, damit fastflowtransform.api.http sie liest
    if offline:
        os.environ["FF_HTTP_OFFLINE"] = "1"
    if http_cache:
        os.environ["FF_HTTP_CACHE_MODE"] = str(
            http_cache.value if hasattr(http_cache, "value") else http_cache
        )

    ctx, engine_ = _build_engine_ctx(project, env_name, engine, vars, cache, no_cache)
    bind_context(engine=ctx.profile.engine, env=env_name)

    select_tokens, _, raw_selected = _select_predicate_and_raw(engine_, ctx, select)
    wanted = _wanted_names(select_tokens=select_tokens, exclude=exclude, raw_selected=raw_selected)

    wanted = _apply_changed_since_filter(
        ctx=ctx,
        wanted=wanted,
        select=select,
        exclude=exclude,
        changed_since=changed_since,
    )

    explicit_targets = _explicit_targets(rebuild_only, rebuild, select, raw_selected)
    _maybe_exit_if_empty(wanted, explicit_targets)

    engine_.force_rebuild = _compute_force_rebuild(explicit_targets, rebuild, wanted)
    lvls = _levels_for_run(explicit_targets, wanted)

    result, logq, started_at, finished_at = _run_schedule(engine_, lvls, jobs, keep_going, ctx)

    # Evaluate budgets.yml based on collected query stats
    budget_error, budgets_summary = _evaluate_budgets(ctx.project, engine_)

    _write_artifacts(ctx, result, started_at, finished_at, engine_, budgets_summary)

    _attempt_catalog(ctx)
    _emit_logs_and_errors(logq, result, engine_)

    if result.failed or budget_error:
        raise typer.Exit(1)

    engine_.persist_on_success(result)
    engine_.print_timings(result)
    echo("✓ Done")
    clear_context()


def register(app: typer.Typer) -> None:
    app.command(
        help=(
            "Loads the project, builds the DAG, and runs every model."
            "\n\nExample:\n  fft run . --env dev"
        )
    )(run)


__all__ = [
    "CacheMode",
    "register",
    "run",
]
