# fastflowtransform/utils/timefmt.py
from __future__ import annotations


def format_duration_minutes(minutes: float | None) -> str:
    """Render minute durations using friendly units (m/h/d)."""
    if minutes is None:
        return "-"
    mins = float(minutes)
    if mins >= 1440:  # 1 day
        return f"{mins / 1440:.1f}d"
    if mins >= 60:
        return f"{mins / 60:.1f}h"
    return f"{mins:.1f}m"


def _format_duration_ms(ms: int) -> str:
    """
    Human-friendly duration from milliseconds.
    """
    if ms < 1000:
        return f"{ms} ms"
    sec = ms / 1000.0
    if sec < 60:
        return f"{sec:.1f} s"
    minutes = sec / 60.0
    if minutes < 60:
        return f"{minutes:.1f} min"
    hours = minutes / 60.0
    return f"{hours:.1f} h"
