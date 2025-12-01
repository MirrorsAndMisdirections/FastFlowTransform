import json
from typing import Any

from jinja2.runtime import Undefined as JinjaUndefined


def sql_literal(value: Any) -> str:
    """
    Convert a Python value into a SQL literal string.

    - None      -> "NULL"
    - bool      -> "TRUE"/"FALSE"
    - int/float -> "123" (no quotes)
    - str       -> quoted with single quotes and escaped
    - other     -> JSON-dumped and treated as a string literal
    """
    if value is None or isinstance(value, JinjaUndefined):
        return "NULL"

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, str):
        # Simple quote-escape for single quotes
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    # Fallback: JSON (or str) and quote it
    try:
        json_text = json.dumps(value, separators=(",", ":"), sort_keys=True)
    except TypeError:
        json_text = str(value)
    return "'" + json_text.replace("'", "''") + "'"
