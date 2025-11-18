# fastflowtransform/testing/discovery.py

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel, create_model

from fastflowtransform.core import REGISTRY
from fastflowtransform.errors import ModelConfigError, ModuleLoadError
from fastflowtransform.logging import get_logger
from fastflowtransform.testing.registry import DQParamsBase, register_sql_test

logger = get_logger("dq_discovery")


# ---------------------------------------------------------------------------
# Helper: parse leading {{ config(...) }} for test SQL files
# ---------------------------------------------------------------------------


def _parse_test_config(text: str, path: Path) -> dict[str, Any]:
    """
    Parse the leading `{{ config(...) }}` header from a SQL-based DQ test file.

    Expected pattern (subset of the model config parser):

        {{ config(
            type="no_future_orders",
            params=["where"]
        ) }}

    Returns a plain dict with at least 'type', optionally 'params'.
    Raises ModelConfigError on malformed config.
    """
    head = text[:2000]

    m = re.search(
        r"^\s*\{\{\s*config\s*\((?P<args>.*?)\)\s*\}\}",
        head,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return {}
    args = m.group("args").strip()
    if not args:
        return {}
    src = f"__CFG__({args})"

    try:
        node = ast.parse(src, mode="eval")
        if not isinstance(node.body, ast.Call):
            return {}
    except Exception as exc:
        raise ModelConfigError(
            f"invalid syntax in test config: {exc}",
            path=str(path),
            field=None,
            hint="Ensure {{ config(...) }} contains comma-separated key=value literals.",
        ) from exc

    cfg: dict[str, Any] = {}
    for kw in node.body.keywords:
        if kw.arg is None:
            val_src = ast.get_source_segment(src, kw.value) or "<expr>"
            raise ModelConfigError(
                f"unsupported **kwargs (got {val_src})",
                path=str(path),
                field="**kwargs",
                hint="Use explicit key=value pairs; test configs must use literals.",
            )
        field = kw.arg
        try:
            cfg[field] = ast.literal_eval(kw.value)
        except Exception as err:
            val_src = ast.get_source_segment(src, kw.value) or "<expr>"
            raise ModelConfigError(
                f"invalid literal (quote strings, no expressions): {val_src}",
                path=str(path),
                field=field,
                hint=(
                    "All values must be JSON/Python literals (e.g. 'no_future_orders', ['where'])."
                ),
            ) from err
    return cfg


# ---------------------------------------------------------------------------
# SQL-based tests
# ---------------------------------------------------------------------------


def _build_params_model_from_config(
    test_type: str,
    path: Path,
    cfg: dict[str, Any],
) -> type[BaseModel] | None:
    """
    Build a Pydantic model from config(params=[...]) in the SQL test file.

    Example in .ff.sql:

        {{ config(
            type="no_future_orders",
            params=["where"]
        ) }}

    This becomes a model roughly equivalent to:

        class DQTestParams_no_future_orders(DQParamsBase):
            where: Any | None = None

    DQParamsBase enforces extra="forbid", so:
      - unknown keys are rejected (typos → clear error),
      - declared keys are typed as Any,
      - all declared params are optional (default None).
    """
    raw_params = cfg.get("params")
    if not raw_params:
        return None

    if not isinstance(raw_params, (list, tuple)) or not all(isinstance(p, str) for p in raw_params):
        raise ModelConfigError(
            "config(params=...) must be a list of strings",
            path=str(path),
            field="params",
            hint="Example: params=['where', 'min_amount']",
        )

    params: list[str] = [str(p) for p in raw_params]

    # Build a dynamic model with optional Any fields and extra='forbid'
    field_defs: dict[str, tuple[Any, Any]] = {}
    for name in params:
        field_defs[name] = (Any, None)
    model_name = f"DQTestParams_{test_type}"

    ParamsModel = create_model(
        model_name,
        __base__=DQParamsBase,
        # cast to satisfy the type checker; runtime behaviour is correct
        **cast(dict[str, Any], field_defs),
    )

    return ParamsModel


def discover_sql_tests(project_dir: Path) -> None:
    """
    Discover SQL-based DQ tests under tests/**/*.ff.sql.

    Each file must:
      - start with a {{ config(type="...", params=[...]) }} block
      - contain exactly one SELECT that returns a scalar "violation count".

    For each test we:
      - parse config(type=..., params=[...])
      - build a Pydantic params model from `params` (if provided)
      - register the test via `register_sql_test(...)`

    Runtime behaviour:
      - params from project.yml are validated against that model (unknown keys → error)
      - Jinja template is rendered with:
            table, column, params, where
        (where = params.get('where'), always present)
      - the SQL is executed and interpreted as "violation count".
    """
    tests_dir = project_dir / "tests"
    if not tests_dir.exists():
        return

    # Ensure the Jinja env is initialized, even though register_sql_test
    # will call REGISTRY.get_env() again internally.
    REGISTRY.get_env()

    for path in sorted(tests_dir.rglob("*.ff.sql")):
        try:
            text = path.read_text(encoding="utf-8")
        except Exception as exc:
            logger.error("Failed to read SQL test file %s: %s", path, exc)
            continue

        cfg = _parse_test_config(text, path)
        tname = cfg.get("type")
        if not tname or not isinstance(tname, str):
            logger.warning(
                "%s: SQL test file missing config(type='...'); skipping",
                path,
            )
            continue

        try:
            params_model = _build_params_model_from_config(tname, path, cfg)
        except ModelConfigError:
            # Config errors should be fatal, like for models
            raise

        register_sql_test(
            kind=tname,
            path=path,
            params_model=params_model,
        )
        logger.debug("Registered SQL DQ test '%s' from %s", tname, path)


# ---------------------------------------------------------------------------
# Python-based tests
# ---------------------------------------------------------------------------


def discover_python_tests(project_dir: Path) -> None:
    """
    Discover Python-based DQ tests under tests/**/*.ff.py.

    The files should define functions decorated with @dq_test("type_name").
    Importing the module is enough; the decorator will register the runner.
    """
    tests_dir = project_dir / "tests"
    if not tests_dir.exists():
        return

    for path in sorted(tests_dir.rglob("*.ff.py")):
        # Reuse the Registry's module loader so we get consistent behaviour.
        try:
            REGISTRY._load_py_module(path)
        except ModuleLoadError:
            # Fail fast: broken DQ test modules should not be silently ignored.
            raise
        except Exception as exc:
            # Other exceptions: surface as a clear message
            raise ModuleLoadError(f"Failed to import DQ test module {path}: {exc}") from exc
