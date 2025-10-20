from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import typer
import yaml

from flowforge import testing
from flowforge.core import REGISTRY
from flowforge.dag import topo_sort

from .bootstrap import _get_test_con, _prepare_context
from .options import (
    EngineOpt,
    EnvOpt,
    ProjectArg,
    SelectOpt,
    VarsOpt,
)
from .selectors import _compile_selector


@dataclass
class DQResult:
    kind: str
    table: str
    column: str | None
    ok: bool
    msg: str | None
    ms: int


def _execute_models(
    order: Iterable[str],
    run_sql: Callable[[Any], Any],
    run_py: Callable[[Any], Any],
    *,
    before: Callable[[str, Any], None] | None = None,
    on_error: Callable[[str, Any, Exception], None] | None = None,
) -> None:
    for name in order:
        node = REGISTRY.nodes[name]
        if before:
            before(name, node)
        try:
            (run_sql if node.kind == "sql" else run_py)(node)
        except Exception as exc:
            if on_error is None:
                raise
            on_error(name, node, exc)


def _maybe_print_marker(con: Any) -> None:
    if os.getenv("FLOWFORGE_SQL_DEBUG") == "1":
        typer.echo(getattr(con, "marker", "NO_SHIM"))


def _run_models(
    pred: Callable[[Any], bool],
    run_sql: Callable[[Any], Any],
    run_py: Callable[[Any], Any],
    *,
    before: Callable[[str, Any], None] | None = None,
    on_error: Callable[[str, Any, Exception], None] | None = None,
) -> None:
    order = [n for n in topo_sort(REGISTRY.nodes) if pred(REGISTRY.nodes[n])]
    _execute_models(order, run_sql, run_py, before=before, on_error=on_error)


def _load_tests(proj: Path) -> list[dict]:
    cfg_path = proj / "project.yml"
    if not cfg_path.exists():
        return []
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    return cfg.get("tests") or []


def _apply_legacy_tag_filter(tests: list[dict], tokens: list[str]) -> list[dict]:
    if len(tokens) != 1 or tokens[0].startswith(("tag:", "type:", "kind:")):
        return tests
    legacy_tag = tokens[0]

    def has_tag(t: dict) -> bool:
        tags = t.get("tags") or []
        return (legacy_tag in tags) if isinstance(tags, list) else (legacy_tag == tags)

    return [t for t in tests if has_tag(t)]


def _run_dq_tests(con: Any, tests: Iterable[dict]) -> list[DQResult]:
    results: list[DQResult] = []
    for t in tests:
        kind = t["type"]
        table: str | None = None
        col: str | None = None
        if kind.startswith("reconcile_"):
            if "left" in t and "right" in t:
                lt = (t.get("left") or {}).get("table")
                rt = (t.get("right") or {}).get("table")
                table = f"{lt} ⇔ {rt}"
            elif "source" in t and "target" in t:
                st = (t.get("source") or {}).get("table")
                tt = (t.get("target") or {}).get("table")
                table = f"{st} ⇒ {tt}"
            else:
                table = "<reconcile>"
        else:
            table = t.get("table")
            if not isinstance(table, str) or not table:
                raise typer.BadParameter("Missing or invalid 'table' in test config")
            col = t.get("column")

        t0 = time.perf_counter()
        ok, msg = _exec_test_kind(con, kind, t, table, col)
        ms = int((time.perf_counter() - t0) * 1000)

        results.append(DQResult(kind=kind, table=table, column=col, ok=ok, msg=msg, ms=ms))
    return results


def _exec_test_kind(con: Any, kind: str, t: dict, table: Any, col: Any) -> tuple[bool, str | None]:
    try_map = {
        "not_null": lambda: testing.not_null(con, table, col),
        "unique": lambda: testing.unique(con, table, col),
        "greater_equal": lambda: testing.greater_equal(con, table, col, t.get("threshold", 0)),
        "non_negative_sum": lambda: testing.non_negative_sum(con, table, col),
        "row_count_between": lambda: testing.row_count_between(
            con, table, t.get("min", 1), t.get("max")
        ),
        "freshness": lambda: testing.freshness(con, table, col, t["max_delay_minutes"]),
        "reconcile_equal": lambda: testing.reconcile_equal(
            con,
            t["left"],
            t["right"],
            abs_tolerance=t.get("abs_tolerance"),
            rel_tolerance_pct=t.get("rel_tolerance_pct"),
        ),
        "reconcile_ratio_within": lambda: testing.reconcile_ratio_within(
            con,
            t["left"],
            t["right"],
            min_ratio=t["min_ratio"],
            max_ratio=t["max_ratio"],
        ),
        "reconcile_diff_within": lambda: testing.reconcile_diff_within(
            con,
            t["left"],
            t["right"],
            max_abs_diff=t["max_abs_diff"],
        ),
        "reconcile_coverage": lambda: testing.reconcile_coverage(
            con,
            t["source"],
            t["target"],
            source_where=t.get("source_where"),
            target_where=t.get("target_where"),
        ),
    }

    fn = try_map.get(kind)
    if fn is None:
        raise typer.BadParameter(f"Unknown test type: {kind}")

    try:
        fn()
        return True, None
    except testing.TestFailure as e:
        return False, str(e)
    except Exception as e:
        return False, f"Unexpected error: {e.__class__.__name__}: {e}"


def _print_summary(results: list[DQResult]) -> None:
    passed = sum(1 for r in results if r.ok)
    failed = len(results) - passed

    typer.echo("\nData Quality Summary")
    typer.echo("────────────────────")
    for r in results:
        mark = "✅" if r.ok else "❌"
        scope = f"{r.table}" + (f".{r.column}" if r.column else "")
        typer.echo(f"{mark} {r.kind:<18} {scope:<40} ({r.ms}ms)")
        if not r.ok and r.msg:
            typer.echo(f"   ↳ {r.msg}")

    typer.echo("\nTotals")
    typer.echo("──────")
    typer.echo(f"✓ passed: {passed}")
    typer.echo(f"✗ failed: {failed}")


def test(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
    select: SelectOpt = None,
) -> None:
    ctx = _prepare_context(project, env_name, engine, vars)
    tokens, pred = _compile_selector(select)
    execu, run_sql, run_py = ctx.make_executor()

    con = _get_test_con(execu)
    _maybe_print_marker(con)

    _run_models(pred, run_sql, run_py)

    tests = _load_tests(ctx.project)
    tests = _apply_legacy_tag_filter(tests, tokens)
    if not tests:
        typer.secho("No tests configured.", fg="bright_black")
        raise typer.Exit(code=0)

    results = _run_dq_tests(con, tests)
    _print_summary(results)

    failed = sum(not r.ok for r in results)
    raise typer.Exit(code=2 if failed > 0 else 0)


def register(app: typer.Typer) -> None:
    app.command(
        help=(
            "Materializes models and runs configured data-quality checks."
            "\n\nExample:\n  flowforge test . --env dev --select batch"
        )
    )(test)


__all__ = [
    "DQResult",
    "register",
    "test",
]
