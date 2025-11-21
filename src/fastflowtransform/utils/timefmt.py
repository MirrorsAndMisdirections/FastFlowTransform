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
