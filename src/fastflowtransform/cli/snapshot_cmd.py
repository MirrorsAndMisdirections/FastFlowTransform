from __future__ import annotations

import typer

from fastflowtransform.cli.bootstrap import CLIContext, _prepare_context
from fastflowtransform.cli.options import (
    EngineOpt,
    EnvOpt,
    ExcludeOpt,
    JobsOpt,
    KeepOpt,
    ProjectArg,
    SelectOpt,
    VarsOpt,
)
from fastflowtransform.cli.run import (
    CacheMode,
    _attempt_catalog,
    _emit_logs_and_errors,
    _evaluate_budgets,
    _levels_for_run,
    _run_schedule,
    _RunEngine,
    _select_predicate_and_raw,
    _wanted_names,
    _write_artifacts,
)
from fastflowtransform.core import REGISTRY, relation_for
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.logging import bind_context, clear_context, echo

snapshot = typer.Typer(help="Snapshot materialization commands.")


class _SnapshotRunEngine(_RunEngine):
    """
    Variant of _RunEngine that calls executor.run_snapshot_sql(...) for
    SQL nodes instead of the normal run_sql path.
    """

    def run_node(self, name: str) -> None:
        node = REGISTRY.nodes[name]
        ex, _run_sql_fn, _run_py_fn = self._get_runner()
        if node.kind != "sql":
            raise TypeError(
                f"Snapshot run only supports SQL models, but node '{name}' is kind={node.kind!r}."
            )
        # No fingerprint / cache skipping: snapshots always execute.
        ex.run_snapshot_sql(node, self.ctx.jinja_env)


def _prune_snapshots(
    executor: BaseExecutor, snapshot_names: set[str], keep_last: int, dry_run: bool
) -> None:
    """
    Apply per-model pruning using executor.snapshot_prune(...) where available.
    """
    for name in sorted(snapshot_names):
        node = REGISTRY.nodes[name]
        meta = getattr(node, "meta", {}) or {}

        unique_key = meta.get("unique_key") or meta.get("primary_key") or []
        unique_key_list = [unique_key] if isinstance(unique_key, str) else list(unique_key or [])

        if not unique_key_list:
            echo(f"Skipping prune for {name}: missing unique_key/primary_key.")
            continue

        if not hasattr(executor, "snapshot_prune"):
            eng = getattr(executor, "engine_name", "unknown")
            echo(f"Skipping prune for {name}: snapshot_prune not implemented for engine '{eng}'.")
            continue

        rel = relation_for(name)
        prefix = "[DRY-RUN] " if dry_run else ""
        echo(f"{prefix}Pruning snapshot {name} (relation={rel}, keep_last={keep_last})")
        executor.snapshot_prune(rel, unique_key_list, keep_last=keep_last, dry_run=dry_run)


@snapshot.command("run")
def snapshot_run(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
    select: SelectOpt = None,
    exclude: ExcludeOpt = None,
    jobs: JobsOpt = "1",
    keep_going: KeepOpt = False,
    prune: bool = typer.Option(
        False,
        "--prune",
        help="Prune historical snapshot rows after a successful run.",
    ),
    keep_last: int = typer.Option(
        3,
        "--keep-last",
        min=1,
        help="Number of latest versions per key to keep when pruning.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show pruning actions without modifying any data.",
    ),
) -> None:
    """
    Execute only snapshot models (materialized='snapshot').

    Selection works like `fft run` but the final set is restricted to snapshot
    models. Use --prune/--keep-last/--dry-run for retention.
    """
    ctx: CLIContext = _prepare_context(project, env_name, engine, vars)
    bind_context(engine=ctx.profile.engine, env=env_name)

    engine_ = _SnapshotRunEngine(
        ctx=ctx,
        pred=None,
        env_name=env_name,
        cache_mode=CacheMode.OFF,  # snapshots always run; no cache skipping
        force_rebuild=set(),
    )

    # Selection identical to run(), but we filter to snapshots afterwards.
    select_tokens, _, raw_selected = _select_predicate_and_raw(
        engine_, ctx, select, include_snapshots=True
    )
    wanted_all = _wanted_names(
        select_tokens=select_tokens, exclude=exclude, raw_selected=raw_selected
    )

    # Restrict to snapshot models only
    snapshot_names: set[str] = {
        name
        for name in wanted_all
        if (getattr(REGISTRY.nodes[name], "meta", {}) or {}).get("materialized") == "snapshot"
    }

    if not snapshot_names:
        typer.secho(
            "Nothing to run (no snapshot models in selection).",
            fg="yellow",
        )
        clear_context()
        raise typer.Exit(0)

    # Build DAG levels for the full wanted set so dependency validation still runs.
    lvls_all = _levels_for_run([], wanted_all)
    # Only execute snapshot nodes while preserving their relative order.
    lvls = [lvl for lvl in ([n for n in level if n in snapshot_names] for level in lvls_all) if lvl]

    result, logq, started_at, finished_at = _run_schedule(engine_, lvls, jobs, keep_going, ctx)

    # Evaluate budgets.yml based on collected query stats
    budget_error, budgets_summary = _evaluate_budgets(ctx.project, engine_)

    _write_artifacts(ctx, result, started_at, finished_at, engine_, budgets_summary)
    _attempt_catalog(ctx)
    _emit_logs_and_errors(logq, result, engine_)

    if result.failed or budget_error:
        clear_context()
        raise typer.Exit(1)

    # Optional retention
    if prune:
        executor = engine_.shared[0]
        _prune_snapshots(executor, snapshot_names, keep_last, dry_run)

    engine_.persist_on_success(result)
    engine_.print_timings(result)
    echo("✓ Snapshot run done")
    clear_context()


def register(app: typer.Typer) -> None:
    app.add_typer(snapshot, name="snapshot")
