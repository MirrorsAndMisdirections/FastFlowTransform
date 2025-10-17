# src/flowforge/utest.py
from __future__ import annotations

import difflib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from sqlalchemy import text

from .core import REGISTRY, Node, relation_for

# ---------- Specifications ----------


@dataclass
class UnitCase:
    name: str
    inputs: dict[str, dict]  # rel -> {rows|csv}
    expect: dict  # {relation?, rows?, order_by?, any_order?, approx?, ignore_columns?, subset?}


@dataclass
class UnitSpec:
    model: str
    engine: str | None
    defaults: dict
    cases: list[UnitCase]
    path: Path
    project_dir: Path


# ---------- Discovery & Defaults ----------


def _deep_merge(base: Any, override: Any) -> Any:
    """
    Recursive merge for dicts. Lists/scalars are replaced entirely.
    (Perfectly adequate for our DSL.)
    """
    if isinstance(base, dict) and isinstance(override, dict):
        out = dict(base)
        for k, v in override.items():
            out[k] = _deep_merge(out.get(k), v)
        return out
    # Fallback: replace (lists and scalars included)
    return override if override is not None else base


def discover_unit_specs(
    project_dir: Path, path: str | None = None, only_model: str | None = None
) -> list[UnitSpec]:
    files = [Path(path)] if path else list((project_dir / "tests" / "unit").glob("*.yml"))
    specs: list[UnitSpec] = []
    for f in files:
        data = yaml.safe_load(f.read_text(encoding="utf-8")) or {}
        model = data.get("model")
        if not model:
            continue
        if only_model and model != only_model:
            continue
        defaults = data.get("defaults", {}) or {}
        engine = data.get("engine")
        cases_raw = data.get("cases", []) or []
        cases: list[UnitCase] = []
        for c in cases_raw:
            base = {"inputs": defaults.get("inputs", {}), "expect": defaults.get("expect", {})}
            merged = _deep_merge(
                base, {"inputs": c.get("inputs", {}), "expect": c.get("expect", {})}
            )
            cases.append(UnitCase(name=c["name"], inputs=merged["inputs"], expect=merged["expect"]))
        specs.append(
            UnitSpec(
                model=model,
                engine=engine,
                defaults=defaults,
                cases=cases,
                path=f,
                project_dir=project_dir.resolve(),
            )
        )
    return specs


# ---------- Input loaders ----------


def _load_relation_from_rows(executor: Any, rel: str, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    if hasattr(executor, "con"):  # DuckDB
        executor.con.register("_ff_unit_tmp", df)
        executor.con.execute(f'create or replace table "{rel}" as select * from _ff_unit_tmp')
        try:
            executor.con.unregister("_ff_unit_tmp")  # duckdb >= 0.8
        except Exception:
            executor.con.execute("drop view if exists _ff_unit_tmp")
        return
    if hasattr(executor, "engine"):  # Postgres
        schema = getattr(executor, "schema", None)
        df.to_sql(
            rel, executor.engine, if_exists="replace", index=False, schema=schema, method="multi"
        )
        return
    raise RuntimeError("Unit tests: unsupported executor backend")


def _load_relation_from_csv(executor: Any, rel: str, csv_path: Path) -> None:
    df = pd.read_csv(csv_path)
    _load_relation_from_rows(executor, rel, df.to_dict(orient="records"))


def _read_result(executor: Any, rel: str) -> pd.DataFrame:
    if hasattr(executor, "con"):  # DuckDB
        return executor.con.table(rel).df()
    if hasattr(executor, "engine"):  # Postgres
        schema = getattr(executor, "schema", None)
        qualified = f'"{schema}"."{rel}"' if schema else f'"{rel}"'

        with executor.engine.begin() as conn:
            return pd.read_sql_query(text(f"select * from {qualified}"), conn)
    raise RuntimeError("Unit tests: unsupported executor backend for reading results")


def _project_root_for_spec(spec: UnitSpec) -> Path:
    # bevorzugt Registry
    if getattr(REGISTRY, "project_dir", None):
        return Path(REGISTRY.get_project_dir()).resolve()
    # heuristisch: nach oben laufen, bis 'models/' existiert
    p = spec.path.resolve()
    for parent in [p.parent, *list(p.parents)]:
        if (parent / "models").is_dir():
            return parent
    return spec.path.parent  # letzter Fallback


# ---------- Assertions ----------


class UnitAssertionFailure(Exception):
    pass


# def assert_rows_equal(
#     actual_df: pd.DataFrame,
#     expect_rows: list[dict],
#     *,
#     order_by: list[str] | None = None,
#     any_order: bool = False,
#     approx: dict[str, float] | None = None,
#     ignore_columns: list[str] | None = None,
#     subset: bool = False,
# ):
#     exp = pd.DataFrame(expect_rows)

#     # Drop ignored columns (if present)
#     if ignore_columns:
#         actual_df = actual_df.drop(
#             columns=[c for c in ignore_columns if c in actual_df.columns], errors="ignore"
#         )
#         exp = exp.drop(columns=[c for c in ignore_columns if c in exp.columns], errors="ignore")

#     # Missing columns?
#     missing = set(exp.columns) - set(actual_df.columns)
#     if missing and not subset:
#         raise UnitAssertionFailure(f"Missing columns in actual: {sorted(missing)}")

#     # Ordering
#     if order_by:
#         actual_df = actual_df.sort_values(order_by).reset_index(drop=True)
#         exp = exp.sort_values(order_by).reset_index(drop=True)
#     elif any_order:
#         common = sorted(set(exp.columns) & set(actual_df.columns))
#         if common:
#             actual_df = actual_df.sort_values(common).reset_index(drop=True)
#             exp = exp.sort_values(common).reset_index(drop=True)

#     # Numeric tolerances
#     approx = approx or {}
#     approx_checked_cols: list[str] = []
#     for col, tol in approx.items():
#         if col in exp.columns and col in actual_df.columns:
#             try:
#                 tol_f = float(tol)
#             except Exception as err:
#                 raise UnitAssertionFailure(
#                     f"Invalid approx tolerance for column '{col}': {tol!r} (must be a number)"
#                 ) from err

#             # Cast to numeric; non-numeric values become NaN and are treated as zero diff
#             a_num = pd.to_numeric(actual_df[col], errors="coerce")
#             e_num = pd.to_numeric(exp[col], errors="coerce")

#             diff = (a_num - e_num).abs().fillna(0)
#             bad = diff > tol_f
#             if bad.any():
#                 raise UnitAssertionFailure(
#                     f"Approx mismatch in '{col}' (tol={tol_f}). "
#                     f"expected={e_num[bad].tolist()} vs actual={a_num[bad].tolist()}"
#                 )
#             # Align values so the exact equality check below does not fail
#             actual_df[col] = exp[col]
#             approx_checked_cols.append(col)

#     # Subset assertion?
#     if subset:
#         # Check that every expected row occurs in the actual result.
#         # Full multiset checks are expensive; use tuple comparison instead.
#         if exp.empty:
#             return
#         key_cols = list(exp.columns)

#         # Represent rows as tuples for stable comparison
#         def _rows_as_tuples(df: pd.DataFrame) -> list[tuple]:
#             return [
#                 tuple(df[c].iloc[i] if c in df.columns else None for c in key_cols)
#                 for i in range(len(df))
#             ]

#         exp_rows = _rows_as_tuples(exp)
#         act_rows = _rows_as_tuples(actual_df)
#         for r in exp_rows:
#             if r not in act_rows:
#                 raise UnitAssertionFailure(f"Expected row {r} not found in actual")
#         return

#     # Exact comparison for the remaining data (treat NaN == NaN)
#     A = actual_df[exp.columns].fillna("__NA__")
#     E = exp.fillna("__NA__")
#     if not A.equals(E):
#         a_csv = A.to_csv(index=False)
#         e_csv = E.to_csv(index=False)
#         diff = "\n".join(
#             difflib.unified_diff(
#                 e_csv.splitlines(),
#                 a_csv.splitlines(),
#                 fromfile="expected",
#                 tofile="actual",
#                 lineterm="",
#             )
#         )
#         raise UnitAssertionFailure(f"Rows differ:\n{diff}")


def assert_rows_equal(
    actual_df: pd.DataFrame,
    expect_rows: list[dict],
    *,
    order_by: list[str] | None = None,
    any_order: bool = False,
    approx: dict[str, float] | None = None,
    ignore_columns: list[str] | None = None,
    subset: bool = False,
) -> None:
    exp = pd.DataFrame(expect_rows)

    actual_df, exp = _drop_ignored_columns(actual_df, exp, ignore_columns)
    _assert_columns_present(actual_df, exp, subset)

    actual_df, exp = _apply_ordering(actual_df, exp, order_by, any_order)

    if subset:
        _assert_subset_present(actual_df, exp)
        return

    _assert_exact_equal(actual_df, exp)


# ----------------- Helfer -----------------


def _drop_ignored_columns(
    actual_df: pd.DataFrame,
    exp: pd.DataFrame,
    ignore_columns: list[str] | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not ignore_columns:
        return actual_df, exp

    cols_actual = [c for c in ignore_columns if c in actual_df.columns]
    cols_exp = [c for c in ignore_columns if c in exp.columns]

    if cols_actual:
        actual_df = actual_df.drop(columns=cols_actual, errors="ignore")
    if cols_exp:
        exp = exp.drop(columns=cols_exp, errors="ignore")
    return actual_df, exp


def _assert_columns_present(actual_df: pd.DataFrame, exp: pd.DataFrame, subset: bool) -> None:
    if subset:
        return
    missing = set(exp.columns) - set(actual_df.columns)
    if missing:
        raise UnitAssertionFailure(f"Missing columns in actual: {sorted(missing)}")


def _apply_ordering(
    actual_df: pd.DataFrame,
    exp: pd.DataFrame,
    order_by: list[str] | None,
    any_order: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if order_by:
        actual_df = actual_df.sort_values(order_by).reset_index(drop=True)
        exp = exp.sort_values(order_by).reset_index(drop=True)
        return actual_df, exp

    if any_order:
        common = sorted(set(exp.columns) & set(actual_df.columns))
        if common:
            actual_df = actual_df.sort_values(common).reset_index(drop=True)
            exp = exp.sort_values(common).reset_index(drop=True)

    return actual_df, exp


def _apply_approx_equalization(
    actual_df: pd.DataFrame,
    exp: pd.DataFrame,
    approx: dict[str, float],
) -> list[str]:
    """Vergleicht numerische Spalten mit Toleranz. Bei Erfolg wird actual auf exp ausgerichtet,
    damit die spätere Exaktprüfung nicht fehlschlägt."""
    checked: list[str] = []
    for col, tol in approx.items():
        if col not in exp.columns or col not in actual_df.columns:
            continue

        try:
            tol_f = float(tol)
        except (TypeError, ValueError) as err:
            raise UnitAssertionFailure(
                f"Invalid approx tolerance for column '{col}': {tol!r} (must be a number)"
            ) from err

        a_num = pd.to_numeric(actual_df[col], errors="coerce")
        e_num = pd.to_numeric(exp[col], errors="coerce")

        diff = (a_num - e_num).abs().fillna(0)
        bad = diff > tol_f
        if bad.any():
            raise UnitAssertionFailure(
                f"Approx mismatch in '{col}' (tol={tol_f}). "
                f"expected={e_num[bad].tolist()} vs actual={a_num[bad].tolist()}"
            )

        # Gleichziehen, damit equals() später nicht stolpert
        actual_df[col] = exp[col]
        checked.append(col)

    return checked


def _assert_subset_present(actual_df: pd.DataFrame, exp: pd.DataFrame) -> None:
    if exp.empty:
        return

    key_cols = list(exp.columns)
    exp_rows = _rows_as_tuples(exp, key_cols)
    act_rows = _rows_as_tuples(actual_df, key_cols)

    missing: list[tuple] = [r for r in exp_rows if r not in act_rows]
    if missing:
        raise UnitAssertionFailure(f"Expected row {missing[0]} not found in actual")


def _rows_as_tuples(df: pd.DataFrame, key_cols: Iterable[str]) -> list[tuple]:
    idx_range = range(len(df))
    return [tuple(df[c].iloc[i] if c in df.columns else None for c in key_cols) for i in idx_range]


def _assert_exact_equal(actual_df: pd.DataFrame, exp: pd.DataFrame) -> None:
    A = actual_df[exp.columns].fillna("__NA__")
    E = exp.fillna("__NA__")
    if A.equals(E):
        return

    a_csv = A.to_csv(index=False)
    e_csv = E.to_csv(index=False)
    diff = "\n".join(
        difflib.unified_diff(
            e_csv.splitlines(),
            a_csv.splitlines(),
            fromfile="expected",
            tofile="actual",
            lineterm="",
        )
    )
    raise UnitAssertionFailure(f"Rows differ:\n{diff}")


# ---------- Runner ----------


def validate_inputs_cover_deps(node: Node, inputs: dict[str, dict]) -> tuple[list[str], list[str]]:
    # Inform the user if expected deps are missing (heuristic only).
    # Map logical dependency -> physical relation
    expected = [relation_for(d) for d in (node.deps or [])]
    missing = [r for r in expected if r not in inputs]
    return expected, missing


# def run_unit_specs(
#     specs: list[UnitSpec], executor: Any, jenv: Any, only_case: str | None = None
# ) -> int:
#     failures = 0
#     for spec in specs:
#         node = REGISTRY.nodes.get(spec.model)
#         if not node:
#             print(f"⚠️  Model '{spec.model}' not found (in {spec.path})")
#             failures += 1
#             continue

#         for case in spec.cases:
#             if only_case and case.name != only_case:
#                 continue
#             print(f"→ {spec.model} :: {case.name}")

#             # 1) Load inputs
#             expected_deps, missing = validate_inputs_cover_deps(node, case.inputs)
#             if missing:
#                 print(
#                     f"   ⚠️ inputs do not cover all deps: missing {missing}"
#                     + f"(expected {expected_deps})"
#                 )

#             for rel, cfg in case.inputs.items():
#                 if "rows" in cfg:
#                     _load_relation_from_rows(executor, rel, cfg["rows"])
#                 elif "csv" in cfg:
#                     csv_val = cfg["csv"]
#                     csv_path = Path(csv_val)
#                     if not csv_path.is_absolute():
#                         base = (
#                             spec.project_dir if spec.project_dir else _project_root_for_spec(spec)
#                         )
#                         csv_path = (
#                             (base / csv_val).resolve()
#                             if not Path(csv_val).is_absolute()
#                             else Path(csv_val)
#                         )
#                     _load_relation_from_csv(executor, rel, csv_path)
#                 else:
#                     print(f"   ❌ invalid input for relation '{rel}'")
#                     failures += 1
#                     continue

#             # 2) Execute only the target model
#             try:
#                 if node.kind == "sql":
#                     executor.run_sql(node, jenv)
#                 else:
#                     executor.run_python(node)
#             except Exception as e:
#                 print(f"   ❌ execution failed: {type(e).__name__}: {e}")
#                 failures += 1
#                 continue

#             # 3) Read result & compare
#             target_rel = case.expect.get("relation") or relation_for(spec.model)
#             try:
#                 df = _read_result(executor, target_rel)
#             except Exception as e:
#                 print(f"   ❌ cannot read result '{target_rel}': {e}")
#                 failures += 1
#                 continue

#             try:
#                 assert_rows_equal(
#                     df,
#                     case.expect.get("rows", []),
#                     order_by=case.expect.get("order_by"),
#                     any_order=case.expect.get("any_order", False),
#                     approx=case.expect.get("approx"),
#                     ignore_columns=case.expect.get("ignore_columns"),
#                     subset=case.expect.get("subset", False),
#                 )
#                 print("   ✅ ok")
#             except UnitAssertionFailure as e:
#                 print(f"   ❌ {e}")
#                 failures += 1

#     return failures


def run_unit_specs(
    specs: list[UnitSpec], executor: Any, jenv: Any, only_case: str | None = None
) -> int:
    failures = 0

    for spec in specs:
        node = REGISTRY.nodes.get(spec.model)
        if not node:
            print(f"⚠️  Model '{spec.model}' not found (in {spec.path})")
            failures += 1
            continue

        for case in spec.cases:
            if only_case and case.name != only_case:
                continue
            print(f"→ {spec.model} :: {case.name}")

            # 1) Inputs laden/prüfen (inkrementiert failures bei ungültigen Inputs)
            failures += _load_inputs_for_case(executor, spec, case, node)

            # 2) Nur Target-Model ausführen
            ok, err = _execute_node(executor, node, jenv)
            if not ok:
                print(f"   ❌ execution failed: {err}")
                failures += 1
                continue

            # 3) Ergebnis lesen
            ok, df_or_exc, target_rel = _read_target_df(executor, spec, case)
            if not ok:
                print(f"   ❌ cannot read result '{target_rel}': {df_or_exc}")
                failures += 1
                continue
            df = df_or_exc

            # 4) Erwartungen prüfen
            ok, msg = _assert_expected_rows(df, case)
            if ok:
                print("   ✅ ok")
            else:
                print(f"   ❌ {msg}")
                failures += 1

    return failures


# ----------------- Helper -----------------


def _load_inputs_for_case(executor: Any, spec: Any, case: Any, node: Any) -> int:
    """
    Lädt alle in 'case.inputs' deklarierten Relationen.
    Gibt die Anzahl der Fehlkonfigurationen zurück (wird zum failures-Zähler addiert).
    """
    failures = 0

    expected_deps, missing = validate_inputs_cover_deps(node, case.inputs)
    if missing:
        print(
            f"   ⚠️ inputs do not cover all deps: missing {missing}" + f" (expected {expected_deps})"
        )

    for rel, cfg in (case.inputs or {}).items():
        try:
            if "rows" in cfg:
                _load_relation_from_rows(executor, rel, cfg["rows"])
            elif "csv" in cfg:
                csv_path = _resolve_csv_path(spec, cfg["csv"])
                _load_relation_from_csv(executor, rel, csv_path)
            else:
                print(f"   ❌ invalid input for relation '{rel}'")
                failures += 1
        except Exception as e:
            print(f"   ❌ failed loading input for '{rel}': {e}")
            failures += 1

    return failures


def _resolve_csv_path(spec: Any, csv_val: str) -> Path:
    csv_path = Path(csv_val)
    if csv_path.is_absolute():
        return csv_path
    base = spec.project_dir if getattr(spec, "project_dir", None) else _project_root_for_spec(spec)
    return (Path(base) / csv_val).resolve()


def _execute_node(executor: Any, node: Any, jenv: Any) -> tuple[bool, str | None]:
    try:
        if getattr(node, "kind", None) == "sql":
            executor.run_sql(node, jenv)
        else:
            executor.run_python(node)
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _read_target_df(executor: Any, spec: Any, case: Any) -> tuple[bool, Any, str]:
    target_rel = case.expect.get("relation") or relation_for(spec.model)
    try:
        df = _read_result(executor, target_rel)
        return True, df, target_rel
    except Exception as e:
        return False, e, target_rel


def _assert_expected_rows(df: Any, case: Any) -> tuple[bool, str | None]:
    try:
        assert_rows_equal(
            df,
            case.expect.get("rows", []),
            order_by=case.expect.get("order_by"),
            any_order=case.expect.get("any_order", False),
            approx=case.expect.get("approx"),
            ignore_columns=case.expect.get("ignore_columns"),
            subset=case.expect.get("subset", False),
        )
        return True, None
    except UnitAssertionFailure as e:
        return False, str(e)
    except AssertionError as e:
        # Falls assert_rows_equal in manchen Pfaden nur AssertionError wirft
        return False, str(e)
