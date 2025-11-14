"""
Python macros: exposed as Jinja globals & filters by FFT core.
They run at *render time* (not as SQL UDFs).
"""

import re
from typing import Any


def slugify(value: str) -> str:
    """Make a URL-friendly slug at render time."""
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return re.sub(r"-{2,}", "-", value).strip("-")


def mask_email(email: str) -> str:
    """Redact local part of an email (render-time)."""
    if "@" not in email:
        return email
    local, domain = email.split("@", 1)
    if not local:
        return email
    return f"{local[0]}***@{domain}"


def csv_values(rows: list[dict[str, Any]], cols: list[str]) -> str:
    """
    Produce a SQL VALUES(...) list for small lookup tables at render time.
    Example: csv_values([{'k':1,'v':'x'}], ['k','v']) -> "(1, 'x')"
    """

    def lit(v):
        if v is None:
            return "NULL"
        if isinstance(v, (int, float)):
            return str(v)
        s = str(v).replace("'", "''")
        return f"'{s}'"

    tuples = []
    for row in rows:
        tuples.append("(" + ", ".join(lit(row.get(c)) for c in cols) + ")")
    return ", ".join(tuples)
