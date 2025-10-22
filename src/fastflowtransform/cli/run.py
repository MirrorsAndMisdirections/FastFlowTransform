from __future__ import annotations

import logging
import os
import textwrap
import threading
import traceback
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

import typer

from fastflowtransform.cache import FingerprintCache, can_skip_node
from fastflowtransform.core import REGISTRY, relation_for
from fastflowtransform.dag import levels as dag_levels
from fastflowtransform.fingerprint import (
    EnvCtx,
    build_env_ctx,
    fingerprint_py,
    fingerprint_sql,
    get_function_source,
)
from fastflowtransform.incremental import run_or_dispatch as run_sql_with_incremental
from fastflowtransform.log_queue import LogQueue
from fastflowtransform.meta import ensure_meta_table
from fastflowtransform.run_executor import ScheduleResult, schedule

from .bootstrap import _prepare_context
from .logging_utils import LOG
from .options import (
    CacheMode,
    CacheOpt,
    EngineOpt,
    EnvOpt,
    ExcludeOpt,
    JobsOpt,
    KeepOpt,
    NoCacheOpt,
    ProjectArg,
    RebuildAllOpt,
    RebuildOnlyOpt,
    SelectOpt,
    VarsOpt,
)
from .selectors import _compile_selector, _parse_select, _selected_subgraph_names


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

    def __post_init__(self) -> None:
        if LOG.isEnabledFor(logging.INFO):
            typer.echo(f"Profile: {self.env_name} | Engine: {self.ctx.profile.engine}")
        self.shared = self.ctx.make_executor()
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
                    ex = ex.clone()

                    def _run_sql_duckdb(n):
                        # Planner: intercept incremental materializations
                        return run_sql_with_incremental(ex, n, self.ctx.jinja_env)

                    run_sql_wrapped = _run_sql_duckdb
                    run_py_wrapped = ex.run_python
                except Exception:
                    pass
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

    def run_node(self, name: str) -> None:
        node = REGISTRY.nodes[name]
        ex, run_sql_fn, run_py_fn = self._get_runner()

        if name in self.force_rebuild:
            (run_sql_fn if node.kind == "sql" else run_py_fn)(node)
            cand_fp = self._maybe_fingerprint(node, ex)
            if cand_fp:
                with self.fps_lock:
                    self.computed_fps[name] = cand_fp
                with suppress(Exception):
                    ex.on_node_built(node, relation_for(name), cand_fp)
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
                return

        (run_sql_fn if node.kind == "sql" else run_py_fn)(node)

        if cand_fp is not None:
            with self.fps_lock:
                self.computed_fps[name] = cand_fp
            with suppress(Exception):
                ex.on_node_built(node, relation_for(name), cand_fp)

    @staticmethod
    def before(_name: str, lvl_idx: int | None = None) -> None:
        return

    @staticmethod
    def on_error(name: str, err: BaseException) -> None:
        _node = REGISTRY.get_node(name)
        if isinstance(err, KeyError):
            typer.echo(
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
        typer.echo(_error_block(f"Model failed: {name}", body, "• See cause above."))
        raise typer.Exit(1) from err

    def persist_on_success(self, result: ScheduleResult) -> None:
        if not result.failed and (self.cache_mode in (CacheMode.RW, CacheMode.WO)):
            self.cache.update_many(self.computed_fps)
            self.cache.save()

    @staticmethod
    def print_timings(result: ScheduleResult) -> None:
        if not result.per_node_s:
            return
        typer.echo("\nRuntime per model")
        typer.echo("─────────────────")
        for name in sorted(result.per_node_s, key=lambda k: k):
            ms = int(result.per_node_s[name] * 1000)
            typer.echo(f"• {name:<30} {ms:>6} ms")
        typer.echo(f"\nTotal runtime: {result.total_s:.3f}s")


def _pretty_exc(e: BaseException) -> str:
    return "".join(traceback.format_exception_only(type(e), e)).strip()


def _error_block(title: str, body: str, hint: str | None = None) -> str:
    border = "─" * 70
    lines = [f"✖ {title}", "", textwrap.dedent(body).rstrip()]
    if hint:
        lines = ["", "Hints:", textwrap.dedent(hint).rstrip()]
    text = "│ \n│ ".join("\n".join(lines).splitlines())
    return f"\n┌{border}\n{text}\n└{border}\n"


def _normalize_node_names_or_warn(names: list[str] | None) -> set[str]:
    out: set[str] = set()
    for tok in _parse_select(names or []):
        try:
            out.add(REGISTRY.get_node(tok).name)
        except KeyError:
            LOG.warning(f"Unknown model in --rebuild: {tok}")
    return out


def run(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
    select: SelectOpt = None,
    exclude: ExcludeOpt = None,
    jobs: JobsOpt = 1,
    keep_going: KeepOpt = False,
    cache: CacheOpt = CacheMode.RW,
    no_cache: NoCacheOpt = False,
    rebuild: RebuildAllOpt = False,
    rebuild_only: RebuildOnlyOpt = None,
) -> None:
    ctx = _prepare_context(project, env_name, engine, vars)
    cache_mode = CacheMode.OFF if no_cache else cache

    _, raw_pred = _compile_selector(select)
    raw_selected: list[str] = [k for k, v in REGISTRY.nodes.items() if raw_pred(v)]

    wanted: set[str] = _selected_subgraph_names(
        REGISTRY.nodes,
        select_tokens=select,
        exclude_tokens=exclude,
    )

    rebuild_only_names = _normalize_node_names_or_warn(rebuild_only)

    explicit_targets: list[str] = []
    if rebuild_only_names:
        explicit_targets = [n for n in (rebuild_only or []) if n in REGISTRY.nodes]
    elif rebuild and select:
        explicit_targets = raw_selected

    if not wanted and not explicit_targets:
        typer.secho(
            "Nothing to run (empty selection after applying --select/--exclude).",
            fg="yellow",
        )
        raise typer.Exit(0)

    if explicit_targets:
        force_rebuild = set(explicit_targets)
    elif rebuild:
        force_rebuild = set(wanted)
    else:
        force_rebuild = set()

    if explicit_targets:
        lvls = [explicit_targets]
    else:
        sub_nodes = {k: v for k, v in REGISTRY.nodes.items() if k in wanted}
        lvls = dag_levels(sub_nodes)

    engine_ = _RunEngine(
        ctx=ctx,
        pred=None,
        env_name=env_name,
        cache_mode=cache_mode,
        force_rebuild=force_rebuild,
    )

    logq = LogQueue()

    def _abbr(e: str) -> str:
        mapping = {
            "duckdb": "DUCK",
            "postgres": "PG",
            "bigquery": "BQ",
            "databricks_spark": "SPK",
            "snowflake_snowpark": "SNOW",
        }
        return mapping.get(e, e.upper()[:4])

    result: ScheduleResult = schedule(
        lvls,
        jobs=jobs,
        fail_policy="keep_going" if keep_going else "fail_fast",
        run_node=engine_.run_node,
        before=engine_.before,
        on_error=None,
        logger=logq,
        engine_abbr=_abbr(ctx.profile.engine),
        name_width=28,
    )
    for line in logq.drain():
        typer.echo(line)
    if result.failed:
        for name, err in result.failed.items():
            engine_.on_error(name, err)
    if result.failed:
        raise typer.Exit(1)
    engine_.persist_on_success(result)
    engine_.print_timings(result)
    typer.echo("✓ Done")


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
