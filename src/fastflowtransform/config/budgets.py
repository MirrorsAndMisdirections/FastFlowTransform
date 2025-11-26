# fastflowtransform/config/budgets.py
from __future__ import annotations

import re as _re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


def _parse_duration_to_ms(value: Any) -> Any:
    """
    Parse a human-friendly duration into milliseconds.

    Supported examples:
      - 5000        → 5000 ms
      - "5000"      → 5000 ms
      - "250ms"     → 250 ms
      - "10s"       → 10_000 ms
      - "1.5m"      → 90_000 ms
      - "2h"        → 7_200_000 ms
      - "1d"        → 86_400_000 ms

    If parsing fails, returns the original value (so Pydantic can raise
    a clear error if it expects an int).
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        iv = int(value)
        return iv if iv > 0 else None

    if not isinstance(value, str):
        return value

    text = value.strip().lower().replace("_", "").replace(" ", "")
    if not text:
        return None

    m = _re.match(r"^([0-9]*\.?[0-9]+)(ms|s|m|h|d)?$", text)
    if not m:
        # Not a duration pattern, let Pydantic validate it later
        return value

    num_str, unit = m.groups()
    try:
        num = float(num_str)
    except ValueError:
        return value

    unit = unit or "ms"
    factor = {
        "ms": 1,
        "s": 1000,
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
    }.get(unit)

    if factor is None:
        return value

    ms = int(num * factor)
    return ms if ms > 0 else None


def _normalize_duration_limits(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Walk the deserialized budgets.yml and normalize only
    `query_duration_ms.warn` / `.error` to integer milliseconds using
    _parse_duration_to_ms.

    Shape (reminder):

        version: 1
        total:
          query_duration_ms: { warn: "10m", error: "30m" }
        models:
          my_model:
            query_duration_ms: { ... }
        tags:
          my_tag:
            query_duration_ms: { ... }
    """
    if not isinstance(raw, dict):
        return raw

    def _fix_metrics(metrics: Any) -> None:
        if not isinstance(metrics, dict):
            return
        qdm = metrics.get("query_duration_ms")
        if not isinstance(qdm, dict):
            return
        for key in ("warn", "error"):
            if key in qdm:
                qdm[key] = _parse_duration_to_ms(qdm[key])

    total = raw.get("total")
    if isinstance(total, dict):
        _fix_metrics(total)

    for section_name in ("models", "tags"):
        section = raw.get(section_name)
        if not isinstance(section, dict):
            continue
        for _, metrics in section.items():
            if isinstance(metrics, dict):
                _fix_metrics(metrics)

    return raw


class BudgetLimit(BaseModel):
    """
    Thresholds for a single metric.

    After preprocessing, values are integers (e.g. bytes, rows, ms). You
    can use either bare numbers or numeric strings:

        warn: 5000000000
        error: "10_000_000_000"

    For query_duration_ms only, we additionally support:
        "10m", "30s", "2h", "1d", "250ms"
    (these are converted to ms before validation).
    """

    model_config = ConfigDict(extra="forbid")

    warn: int | None = None
    error: int | None = None

    @field_validator("warn", "error", mode="before")
    @classmethod
    def _normalize_int(cls, v: Any) -> int | None:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            iv = int(v)
            return iv if iv > 0 else None
        if isinstance(v, str):
            text = v.strip().replace("_", "").replace(",", "")
            if not text:
                return None
            if not text.isdigit():
                # At this point we've already tried to parse duration strings
                # for query_duration_ms; non-numeric leftovers here are an error.
                raise ValueError(f"budget limits must be integers or numeric strings, got {v!r}")
            iv = int(text)
            return iv if iv > 0 else None
        raise TypeError("budget limits must be integers or strings")


class BudgetMetrics(BaseModel):
    """
    Metrics we can budget against; all are optional.

      bytes_scanned     → sum of bytes across all SQL queries
      rows              → sum of rows across all SQL queries
      query_duration_ms → sum of query durations (ms), not wall-clock
    """

    model_config = ConfigDict(extra="forbid")

    bytes_scanned: BudgetLimit | None = None
    rows: BudgetLimit | None = None
    query_duration_ms: BudgetLimit | None = None


class QueryLimitConfig(BaseModel):
    """
    Per-engine query limit configuration.

    Currently only `max_bytes` is supported.
    """

    model_config = ConfigDict(extra="forbid")

    max_bytes: int | None = None

    @field_validator("max_bytes", mode="before")
    @classmethod
    def _normalize_int(cls, v: Any) -> int | None:
        return BudgetLimit._normalize_int(v)


class BudgetsConfig(BaseModel):
    """
    Strict representation of budgets.yml.

    Example:

        version: 1

        # Global (across all models in fft run)
        total:
          bytes_scanned:
            warn: 5000000000
            error: 10000000000

        # Per model limits
        models:
          fct_events:
            bytes_scanned:
              warn: 1000000000
              error: 2000000000

        # Per tag limits (aggregated over all models with that tag)
        tags:
          heavy:
            bytes_scanned:
              warn: 5000000000
              error: 8000000000

        # Optional per-engine query guard limits
        query_limits:
          duckdb:
            max_bytes: 2000000000
    """

    model_config = ConfigDict(extra="forbid")

    version: int = 1

    total: BudgetMetrics | None = None
    models: dict[str, BudgetMetrics] = Field(default_factory=dict)
    tags: dict[str, BudgetMetrics] = Field(default_factory=dict)
    query_limits: dict[str, QueryLimitConfig] = Field(default_factory=dict)


def load_budgets_config(project_dir: Path) -> BudgetsConfig | None:
    """
    Read budgets.yml under `project_dir` and validate it strictly.

    Missing file → returns None (no budgets enforced).
    Invalid file → raises, caller should wrap into a user-friendly error.
    """
    project_dir = Path(project_dir)
    cfg_path = project_dir / "budgets.yml"
    if not cfg_path.exists():
        return None

    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    raw = _normalize_duration_limits(raw)
    return BudgetsConfig.model_validate(raw)
