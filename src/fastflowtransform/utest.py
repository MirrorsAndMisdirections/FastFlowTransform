# src/fastflowtransform/utest.py
from __future__ import annotations

import difflib
import hashlib
import json
import os
import uuid
from collections.abc import Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import pandas as pd
import yaml
from sqlalchemy import text

from fastflowtransform.cache import FingerprintCache, can_skip_node
from fastflowtransform.fingerprint import (
    EnvCtx,
    build_env_ctx,
    fingerprint_py,
    fingerprint_sql,
    get_function_source,
)
from fastflowtransform.meta import delete_meta_for_node

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
        # unique temp name per call to avoid clashes under parallel runs
        tmp_name = f"_ff_unit_tmp_{uuid.uuid4().hex[:12]}"
        executor.con.register(tmp_name, df)
        try:
            executor.con.execute(f'create or replace table "{rel}" as select * from {tmp_name}')
        finally:
            # DuckDB >= 0.8: unregister exists; otherwise drop the view fallback
            try:
                executor.con.unregister(tmp_name)
            except Exception:
                executor.con.execute(f"drop view if exists {tmp_name}")
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


# ---------- Cache and Fingerprint Helpers ----------


def _normalize_for_hash(obj: Any) -> Any:
    """Deterministic, JSON-serializable shape: dicts sorted, tuples/sets normalized."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, Path):
        return obj.as_posix()
    if isinstance(obj, (list, tuple, set)):
        # Preserve list order; sort sets/tuples into a list for determinism
        if isinstance(obj, list):
            return [_normalize_for_hash(x) for x in obj]
        return sorted(
            [_normalize_for_hash(x) for x in obj], key=lambda x: json.dumps(x, sort_keys=True)
        )
    if isinstance(obj, dict):
        return {k: _normalize_for_hash(obj[k]) for k in sorted(obj.keys())}
    # Fallback to string representation (stable enough for primitives we don't expect here)
    return str(obj)


def _sha256_hex(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _digest_file(path: Path) -> str | None:
    """Return SHA-256 hex of file contents if readable; else None."""
    try:
        data = path.read_bytes()
    except Exception:
        return None
    return _sha256_hex(data)


def _resolve_csv_path(spec: Any, csv_val: str) -> Path:
    """
    Resolve CSV path for utests with robust fallbacks:
      1) absolute path → return as-is
      2) spec.path's parent (YAML dir)
      3) spec.project_dir (project root)
      4) derived project root via _project_root_for_spec(spec)
      5) current working directory

    We return the first candidate that exists; if none exist, return the first candidate anyway.
    """
    p = Path(csv_val)
    if p.is_absolute():
        return p

    candidates: list[Path] = []

    # 1) YAML file directory
    sp = getattr(spec, "path", None)
    if sp:
        with suppress(Exception):
            candidates.append(Path(sp).parent / p)

    # 2) Explicit project_dir
    proj_dir = getattr(spec, "project_dir", None)
    if proj_dir:
        candidates.append(Path(proj_dir) / p)

    # 3) Heuristic project root
    with suppress(Exception):
        candidates.append(_project_root_for_spec(spec) / p)

    # 4) CWD
    candidates.append(Path.cwd() / p)

    for c in candidates:
        if c.exists():
            return c.resolve()

    # If nothing exists, return best guess (first candidate) to surface a clear error upstream
    return (candidates[0] if candidates else (Path.cwd() / p)).resolve()


def _extract_defaults_inputs(spec: Any) -> dict[str, Any]:
    """Return defaults.inputs as dict or {}. Works for dict, namespace/dataclass, mapping-like."""
    defaults = getattr(spec, "defaults", None)

    # Case 1: dict / Mapping
    if isinstance(defaults, Mapping):
        val = cast(Mapping[str, Any], defaults).get("inputs", {})
        return val if isinstance(val, dict) else {}

    if defaults is None:
        return {}

    # Case 2: object with attribute 'inputs' (e.g. SimpleNamespace / dataclass)
    val = getattr(defaults, "inputs", None)
    if isinstance(val, dict):
        return val

    # Case 3: mapping-like object with get()
    get = getattr(defaults, "get", None)
    if callable(get):
        try:
            val = get("inputs")
            return val if isinstance(val, dict) else {}
        except Exception:
            return {}

    return {}


def _fingerprint_case_inputs(spec: Any, case: Any) -> str:
    """
    Compute a deterministic fingerprint of the EFFECTIVE inputs for a case.
    Merges spec.defaults.inputs and case.inputs (case overrides), then:
      - For rows: include normalized rows.
      - For csv: include the resolved path AND its file content digest if available.
    """
    # Gather defaults.inputs robustly
    defaults_inputs = _extract_defaults_inputs(spec)

    case_inputs = getattr(case, "inputs", None) or {}

    effective_inputs = _deep_merge(defaults_inputs, case_inputs)

    norm: dict[str, Any] = {}
    for rel, cfg in (effective_inputs or {}).items():
        item = {}
        if isinstance(cfg, dict):
            # rows
            if "rows" in cfg:
                item["rows"] = _normalize_for_hash(cfg["rows"])
            # csv
            if "csv" in cfg and isinstance(cfg["csv"], str):
                csv_path = _resolve_csv_path(spec, cfg["csv"])
                item["csv_path"] = csv_path.as_posix()
                file_hash = _digest_file(csv_path)
                if file_hash:
                    item["csv_sha256"] = file_hash
                else:
                    # Fallback: include path string only if unreadable
                    item.setdefault("csv_unreadable", True)
        else:
            # Unknown shape: include normalized raw value
            item["value"] = _normalize_for_hash(cfg)
        norm[rel] = item

    payload = {"inputs": _normalize_for_hash(norm)}
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    return _sha256_hex(data)


# ---------- Assertions ----------


class UnitAssertionFailure(Exception):
    pass


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
    # Apply numeric approximations (align actual to expected within tolerances)
    if approx:
        _apply_approx_equalization(actual_df, exp, approx)

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
    """Compares numeric columns with tolerance."""
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


@dataclass
class UtestCtx:
    executor: Any
    jenv: Any
    engine_name: str
    env_ctx: EnvCtx
    cache: FingerprintCache | None
    cache_mode: str
    computed_fps: dict[str, str] = field(default_factory=dict)
    failures: int = 0


def _normalize_cache_mode(cache_mode: str | Any) -> str:
    if not isinstance(cache_mode, str):
        cache_mode = getattr(cache_mode, "value", str(cache_mode))
    v = cache_mode.lower()
    if v not in {"off", "ro", "rw"}:
        raise ValueError(f"unknown cache_mode: {cache_mode}")
    return v


def _detect_engine_name(executor: Any) -> str:
    if hasattr(executor, "con"):
        return "duckdb"
    if hasattr(executor, "engine"):
        return "postgres"
    if hasattr(executor, "client"):  # pragma: no cover
        return "bigquery"
    return "unknown"


def _make_env_ctx(engine_name: str) -> EnvCtx:
    return build_env_ctx(
        engine=engine_name,
        profile_name="utest",
        relevant_env_keys=[k for k in os.environ if k.startswith("FF_")],
        sources=getattr(REGISTRY, "sources", {}),
    )


def _make_cache(project_dir: Path | None, engine_name: str) -> FingerprintCache | None:
    if project_dir is None:
        return None
    cache = FingerprintCache(project_dir, profile="utest", engine=engine_name)
    cache.load()
    return cache


def _get_project_dir_safe() -> Path | None:
    try:
        return REGISTRY.get_project_dir()
    except Exception:
        return None


def _fingerprint_case(node: Any, spec: Any, case: Any, ctx: UtestCtx) -> str | None:
    # 1) casespezifische Dep-FPs
    dep_fps = {
        "__case__": f"{getattr(spec, 'path', 'spec')}::{getattr(case, 'name', 'case')}",
        "__inputs__": _fingerprint_case_inputs(spec, case),
    }
    try:
        if node.kind == "sql":  # pragma: no cover
            sql = ctx.executor.render_sql(
                node,
                ctx.jenv,
                ref_resolver=lambda nm: ctx.executor._resolve_ref(nm, ctx.jenv),
                source_resolver=ctx.executor._resolve_source,
            )
            return fingerprint_sql(
                node=node, rendered_sql=sql, env_ctx=ctx.env_ctx, dep_fps=dep_fps
            )
        # python
        func = REGISTRY.py_funcs[node.name]
        src = get_function_source(func)
        return fingerprint_py(node=node, func_src=src, env_ctx=ctx.env_ctx, dep_fps=dep_fps)
    except Exception:
        return None  # fingerprint optional


def _maybe_skip_by_cache(node: Any, cand_fp: str | None, ctx: UtestCtx) -> bool:
    if not (cand_fp and ctx.cache and ctx.cache_mode in {"ro", "rw"}):
        return False
    materialized = (getattr(node, "meta", {}) or {}).get("materialized", "table")
    if can_skip_node(
        node_name=node.name,
        new_fp=cand_fp,
        cache=ctx.cache,
        executor=ctx.executor,
        materialized=materialized,
    ):
        print("   ↻ skipped (utest cache hit)")
        if ctx.cache_mode == "rw":
            ctx.computed_fps.setdefault(node.name, cand_fp)
        return True
    return False


def _execute_and_update_cache(node: Any, cand_fp: str | None, ctx: UtestCtx) -> bool:
    ok, err = _execute_node(ctx.executor, node, ctx.jenv)
    if not ok:
        print(f"   ❌ execution failed: {err}")
        ctx.failures += 1
        return False
    if cand_fp and ctx.cache and ctx.cache_mode == "rw":
        ctx.computed_fps[node.name] = cand_fp
    return True


def _read_and_assert(spec: Any, case: Any, ctx: UtestCtx) -> None:
    ok, df_or_exc, target_rel = _read_target_df(ctx.executor, spec, case)
    if not ok:
        print(f"   ❌ cannot read result '{target_rel}': {df_or_exc}")
        ctx.failures += 1
        return
    ok2, msg = _assert_expected_rows(df_or_exc, case)
    if ok2:
        print("   ✅ ok")
    else:
        print(f"   ❌ {msg}")
        ctx.failures += 1


def run_unit_specs(
    specs: list[UnitSpec],
    executor: Any,
    jenv: Any,
    only_case: str | None = None,
    *,
    cache_mode: str = "off",
    reuse_meta: bool = False,
) -> int:
    """
    Execute discovered unit-test specs. Returns the number of failed cases.

    Args:
        cache_mode: 'off' | 'ro' | 'rw'. Default 'off' for deterministic runs.
        reuse_meta: reserved (no-op).
    """
    cache_mode = _normalize_cache_mode(cache_mode)

    project_dir = _get_project_dir_safe()
    engine_name = _detect_engine_name(executor)
    env_ctx = _make_env_ctx(engine_name)
    cache = _make_cache(project_dir, engine_name)

    ctx = UtestCtx(
        executor=executor,
        jenv=jenv,
        engine_name=engine_name,
        env_ctx=env_ctx,
        cache=cache,
        cache_mode=cache_mode,
    )

    for spec in specs:
        node = REGISTRY.nodes.get(spec.model)
        if not node:
            print(f"⚠️  Model '{spec.model}' not found (in {spec.path})")
            ctx.failures += 1
            continue

        for case in spec.cases:
            if only_case and case.name != only_case:
                continue
            print(f"→ {spec.model} :: {case.name}")

            if not reuse_meta:
                with suppress(Exception):
                    delete_meta_for_node(executor, node.name)

            cand_fp = _fingerprint_case(node, spec, case, ctx)

            # Inputs laden/prüfen (zählt failures selbst)
            ctx.failures += _load_inputs_for_case(executor, spec, case, node)

            # ggf. Skip
            if _maybe_skip_by_cache(node, cand_fp, ctx):
                _read_and_assert(spec, case, ctx)
                continue

            # ausführen + ggf. Cache aktualisieren
            if not _execute_and_update_cache(node, cand_fp, ctx):
                continue

            # Ergebnis prüfen
            _read_and_assert(spec, case, ctx)

    # Cache persistieren (nur rw)
    if ctx.cache and ctx.computed_fps and ctx.cache_mode == "rw":  # pragma: no cover
        ctx.cache.update_many(ctx.computed_fps)
        ctx.cache.save()

    return ctx.failures


# ----------------- Helper -----------------


def _load_inputs_for_case(executor: Any, spec: Any, case: Any, node: Any) -> int:
    """
    Loads all declared relations in 'case.inputs'.
    Returns the count of failed inputs.
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
