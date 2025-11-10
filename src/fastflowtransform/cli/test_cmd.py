# fastflowtransform/cli/test_cmd.py
from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import typer
import yaml

from fastflowtransform import testing
from fastflowtransform.cli.bootstrap import _get_test_con, _prepare_context
from fastflowtransform.cli.options import (
    EngineOpt,
    EnvOpt,
    ProjectArg,
    SelectOpt,
    SkipBuildOpt,
    VarsOpt,
)
from fastflowtransform.cli.selectors import _compile_selector
from fastflowtransform.core import REGISTRY
from fastflowtransform.dag import topo_sort
from fastflowtransform.errors import ModelExecutionError
from fastflowtransform.logging import echo
from fastflowtransform.schema_loader import Severity, TestSpec, load_schema_tests
from fastflowtransform.test_registry import TESTS


@dataclass
class DQResult:
    kind: str
    table: str
    column: str | None
    ok: bool
    msg: str | None
    ms: int
    severity: Severity = "error"
    param_str: str = ""
    example_sql: str | None = None


def _print_model_error_block(node_name: str, relation: str, message: str, sql: str | None) -> None:
    header = "┌" + "─" * 70
    footer = "└" + "─" * 70
    echo(header)
    echo(f"│ Model: {node_name}  (relation: {relation})")
    echo(f"│ Error: {message}")
    if sql:
        echo("│ SQL (tail):")
        for line in sql.splitlines():
            echo("│   " + line)
    echo(footer)


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
                # Convert known domain error to friendly output
                if isinstance(exc, ModelExecutionError):
                    _print_model_error_block(exc.node_name, exc.relation, str(exc), exc.sql_snippet)
                    raise typer.Exit(1) from exc
                raise Exception from exc
            on_error(name, node, exc)


def _maybe_print_marker(con: Any) -> None:
    if os.getenv("FFT_SQL_DEBUG") == "1":
        echo(getattr(con, "marker", "NO_SHIM"))


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


def _is_legacy_test_token(tokens: list[str]) -> bool:
    return len(tokens) == 1 and not tokens[0].startswith(("tag:", "type:", "kind:"))


def _apply_legacy_tag_filter(
    tests: list[Any], tokens: list[str], *, legacy_token: bool
) -> list[Any]:
    if not legacy_token:
        return tests
    legacy_tag = tokens[0]

    def has_tag(t: Any) -> bool:
        # Dict (altes Format)
        if isinstance(t, dict):
            tags = t.get("tags") or []
            return (legacy_tag in tags) if isinstance(tags, list) else (legacy_tag == tags)
        # TestSpec (neues Schema)
        if isinstance(t, TestSpec):
            return legacy_tag in (t.tags or [])
        return False

    return [t for t in tests if has_tag(t)]


def _run_dq_tests(con: Any, tests: Iterable[Any]) -> list[DQResult]:
    results: list[DQResult] = []
    for t in tests:
        severity: Severity
        if isinstance(t, TestSpec):
            kind = t.type
            col = t.column
            severity = t.severity
            params: dict[str, Any] = t.params or {}
            display_table = t.table
            table_for_exec = t.table
        else:
            kind = t["type"]
            _sev = str(t.get("severity", "error")).lower()
            severity = "warn" if _sev == "warn" else "error"
            params = dict(t)
            col = t.get("column")
            if kind.startswith("reconcile_"):
                if isinstance(t.get("left"), dict) and isinstance(t.get("right"), dict):
                    lt = (t.get("left") or {}).get("table")
                    rt = (t.get("right") or {}).get("table")
                    display_table = f"{lt} ⇔ {rt}"
                elif isinstance(t.get("source"), dict) and isinstance(t.get("target"), dict):
                    st = (t.get("source") or {}).get("table")
                    tt = (t.get("target") or {}).get("table")
                    display_table = f"{st} ⇒ {tt}"
                else:
                    display_table = "<reconcile>"
                table_for_exec = t.get("table")
            else:
                table_for_exec = t.get("table")
                if not isinstance(table_for_exec, str) or not table_for_exec:
                    raise typer.BadParameter("Missing or invalid 'table' in test config")
                display_table = table_for_exec

        # Dispatch via registry if available; otherwise fallback to legacy map
        t0 = time.perf_counter()
        if kind in TESTS:
            ok, msg, example = TESTS[kind](con, table_for_exec, col, params)
        else:
            ok, msg = _exec_test_kind(con, kind, params, table_for_exec, col)
            example = None
        ms = int((time.perf_counter() - t0) * 1000)

        # Build short parameter display for the summary line
        param_str = _format_params_for_summary(kind, params)

        results.append(
            DQResult(
                kind=kind,
                table=str(display_table),
                column=col,
                ok=ok,
                msg=msg,
                ms=ms,
                severity=severity,
                param_str=param_str,
                example_sql=example,
            )
        )
    return results


def _exec_test_kind(con: Any, kind: str, t: dict, table: Any, col: Any) -> tuple[bool, str | None]:
    # Guard for column-required tests in legacy path
    def _need_col() -> str:
        if not isinstance(col, str) or not col:
            raise typer.BadParameter(f"Test '{kind}' requires a non-empty 'column' parameter")
        return col

    try_map = {
        "not_null": lambda: testing.not_null(con, table, _need_col()),
        "unique": lambda: testing.unique(con, table, _need_col()),
        "greater_equal": lambda: testing.greater_equal(
            con, table, _need_col(), t.get("threshold", 0)
        ),
        "non_negative_sum": lambda: testing.non_negative_sum(con, table, _need_col()),
        "row_count_between": lambda: testing.row_count_between(
            con, table, t.get("min", 1), t.get("max")
        ),
        "freshness": lambda: testing.freshness(con, table, _need_col(), t["max_delay_minutes"]),
        "accepted_values": lambda: testing.accepted_values(
            con, table, col, values=t.get("values", []), where=t.get("where")
        ),
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
    failed = sum((not r.ok) and (r.severity != "warn") for r in results)
    warned = sum((not r.ok) and (r.severity == "warn") for r in results)

    echo("\nData Quality Summary")
    echo("────────────────────")
    for r in results:
        mark = "✅" if r.ok else "❕" if r.severity == "warn" else "❌"
        scope = f"{r.table}" + (f".{r.column}" if r.column else "")
        kind_with_params = f"{r.kind}"
        if r.param_str:
            kind_with_params += f" {r.param_str}"
        echo(f"{mark} {kind_with_params:<28} {scope:<40} ({r.ms}ms)")
        if not r.ok and r.msg:
            echo(f"   ↳ {r.msg}")
        if not r.ok and r.example_sql:
            echo(f"   ↳ e.g. SQL: {r.example_sql}")

    echo("\nTotals")
    echo("──────")
    echo(f"✓ passed: {passed}")
    if warned:
        echo(f"! warned: {warned}")
    echo(f"✗ failed: {failed}")


def _format_params_for_summary(kind: str, params: dict[str, Any]) -> str:
    """Format a short, readable parameter snippet for the summary line."""
    if not params:
        return ""
    # Common keys first for stable display
    keys = []
    if "column" in params:
        keys.append("column")
    if "values" in params:
        keys.append("values")
    if "where" in params:
        keys.append("where")
    # Add remaining keys deterministically
    for k in sorted(params.keys()):
        if k not in keys and k not in ("type", "table", "severity", "tags"):
            keys.append(k)
    parts: list[str] = []
    for k in keys:
        v = params.get(k)
        preview_len = 4
        if k == "values" and isinstance(v, list):
            preview = v if len(v) <= preview_len else [*v[:3], "…"]
            parts.append(f"values={preview}")
        elif v is not None:
            parts.append(f"{k}={v}")
    return "(" + ", ".join(parts) + ")" if parts else ""


def test(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
    select: SelectOpt = None,
    skip_build: SkipBuildOpt = False,
) -> None:
    # _ensure_logging()
    ctx = _prepare_context(project, env_name, engine, vars)
    tokens, pred = _compile_selector(select)
    has_model_matches = any(pred(node) for node in REGISTRY.nodes.values())
    legacy_tag_only = _is_legacy_test_token(tokens) and not has_model_matches
    execu, run_sql, run_py = ctx.make_executor()

    con = _get_test_con(execu)
    _maybe_print_marker(con)

    model_pred = (lambda _n: True) if legacy_tag_only else pred
    # Run models; if a model fails, show friendly error then exit(1).
    if not skip_build:
        _run_models(model_pred, run_sql, run_py)

    # 1) project.yml tests
    tests: list[Any] = _load_tests(ctx.project)
    # 2) schema YAML tests
    tests.extend(load_schema_tests(ctx.project))
    # 3) optional legacy tagfilter (e.g., "batch")
    tests = _apply_legacy_tag_filter(tests, tokens, legacy_token=legacy_tag_only)
    if not tests:
        typer.secho("No tests configured.", fg="bright_black")
        raise typer.Exit(code=0)

    results = _run_dq_tests(con, tests)
    _print_summary(results)

    # Exit code: count only ERROR fails
    failed = sum((not r.ok) and (r.severity != "warn") for r in results)
    raise typer.Exit(code=2 if failed > 0 else 0)


def register(app: typer.Typer) -> None:
    app.command(
        help=(
            "Materializes models and runs configured data-quality checks."
            "\n\nExample:\n  fft test . --env dev --select batch"
        )
    )(test)


__all__ = [
    "DQResult",
    "register",
    "test",
]
