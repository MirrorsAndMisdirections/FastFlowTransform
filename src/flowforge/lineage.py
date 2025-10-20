from __future__ import annotations

import inspect
import re
from typing import Any

# ────────────────────────────────────────────────────────────────────────────────
# Data structures
# ────────────────────────────────────────────────────────────────────────────────

LineageItem = dict[str, Any]  # { "from_relation": str, "from_column": str, "transformed": bool }
LineageMap = dict[str, list[LineageItem]]  # out_col -> [LineageItem, ...]


# ────────────────────────────────────────────────────────────────────────────────
# SQL lineage (heuristic)
# ────────────────────────────────────────────────────────────────────────────────

_FROM_RE = re.compile(
    r"\b(from|join)\s+([a-zA-Z_][\w\.\$\"`]*)\s+(?:as\s+)?([a-zA-Z_][\w\$]*)",
    flags=re.IGNORECASE,
)
_SEL_RE = re.compile(r"\bselect\b(.*?)\bfrom\b", flags=re.IGNORECASE | re.DOTALL)


def _split_select_list(select_clause: str) -> list[str]:
    """
    Split a SELECT clause into top-level comma-separated expressions.
    Handles parentheses depth; does not handle quoted commas - good enough for common cases.
    """
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    for ch in select_clause:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def _alias_map_from_sql(sql: str) -> dict[str, str]:
    """
    Build alias -> relation map by scanning FROM/JOIN clauses in the *rendered* SQL.
    """
    out: dict[str, str] = {}
    for _, rel, alias in _FROM_RE.findall(sql):
        # Strip quoting if present
        rel_clean = rel.strip('"`')
        out[alias] = rel_clean
    return out


def _append_lineage(lineage: LineageMap, out_col: str | None, item: LineageItem) -> None:
    """
    Append a lineage item to the map if an output column name is available.
    This guards Optional[str] and keeps type-checkers happy.
    """
    if not out_col:
        return
    lineage.setdefault(out_col, []).append(item)


def infer_sql_lineage(rendered_sql: str, ref_map: dict[str, str] | None = None) -> LineageMap:
    """
    Infer a mapping from output column -> upstream sources (relation.column) for common patterns:
      - <alias>.<col> AS <out>
      - <col> AS <out>             (relation unknown)
      - FUNC(<alias>.<col>) AS <out>  → transformed=True
      - bare <alias>.<col>          → out=<col>, direct
    Joins with aliases are resolved via <alias> → relation from FROM/JOIN.
    """
    lineage: LineageMap = {}
    if not rendered_sql:
        return lineage

    alias_map = ref_map or _alias_map_from_sql(rendered_sql)

    m = _SEL_RE.search(rendered_sql)
    if not m:
        return lineage
    select_clause = m.group(1)
    exprs = _split_select_list(select_clause)

    # Patterns
    as_pat = re.compile(r"^(?P<expr>.+?)\s+as\s+(?P<alias>[a-zA-Z_][\w\$]*)$", re.IGNORECASE)
    qual_col = re.compile(r"^(?P<a>[a-zA-Z_]\w*)\.(?P<c>[a-zA-Z_]\w*)$")
    func_of_qual = re.compile(
        r"^[a-zA-Z_]\w*\s*\(\s*(?P<a>[a-zA-Z_]\w*)\.(?P<c>[a-zA-Z_]\w*)\s*\)\s*$", re.IGNORECASE
    )

    for raw in exprs:
        expr = raw.strip()
        if expr == "*" or not expr:
            continue

        out_col: str | None = None
        expr_only = expr
        m_as = as_pat.match(expr)
        if m_as:
            out_col = m_as.group("alias")
            expr_only = m_as.group("expr").strip()

        # func(alias.col)
        m_func = func_of_qual.match(expr_only)
        if m_func:
            a, c = m_func.group("a"), m_func.group("c")
            rel = alias_map.get(a)
            item = {
                "from_relation": rel or "?",
                "from_column": c,
                "transformed": True,
            }
            if out_col is None:
                out_col = c  # best-effort
            _append_lineage(lineage, out_col, item)
            continue

        # alias.col
        m_q = qual_col.match(expr_only)
        if m_q:
            a, c = m_q.group("a"), m_q.group("c")
            rel = alias_map.get(a)
            item = {
                "from_relation": rel or "?",
                "from_column": c,
                "transformed": False,
            }
            if out_col is None:
                out_col = c
            _append_lineage(lineage, out_col, item)
            continue

        # plain col (no qualifier) - we can only map column name with unknown relation
        m_col = re.match(r"^[a-zA-Z_]\w*$", expr_only)
        if m_col:
            c = expr_only
            item = {"from_relation": "?", "from_column": c, "transformed": False}
            if out_col is None:
                out_col = c
            _append_lineage(lineage, out_col, item)
            continue

        # func(col) or complex expression → mark as transformed with unknown relation/col
        _append_lineage(
            lineage, out_col, {"from_relation": "?", "from_column": "?", "transformed": True}
        )

    return lineage


# ────────────────────────────────────────────────────────────────────────────────
# Python (pandas) lineage (very light heuristic)
# ────────────────────────────────────────────────────────────────────────────────

_ASSIGN_RE = re.compile(
    r"""\b(?P<lhs>[_a-zA-Z]\w*)\s*\[\s*['"](?P<out>[_a-zA-Z]\w*)['"]\s*\]\s*=\s*
        (?P<src>[_a-zA-Z]\w*)\s*\[\s*['"](?P<col>[_a-zA-Z]\w*)['"]\s*\]""",
    re.VERBOSE,
)
_RENAME_RE = re.compile(
    r"""\.rename\s*\(\s*columns\s*=\s*\{(?P<pairs>.*?)\}\s*\)""",
    re.DOTALL,
)
_PAIR_RE = re.compile(r"""['"](?P<old>[_a-zA-Z]\w*)['"]\s*:\s*['"](?P<new>[_a-zA-Z]\w*)['"]""")
_ASSIGN_LAMBDA_RE = re.compile(
    r"""\.assign\s*\(\s*([_a-zA-Z]\w*)\s*=\s*lambda\s+\w+\s*:\s*(?P<body>[^,)]+)\)""",
    re.DOTALL,
)
_BODY_SRC_COL = re.compile(r"""\[\s*['"](?P<col>[_a-zA-Z]\w*)['"]\s*\]""")


def infer_py_lineage(
    func: Any, requires: dict | None = None, source_code: str | None = None
) -> LineageMap:
    """
    Very small regex-based inference for common pandas patterns:
      - out["x"] = df["y"]                      → x <- y (direct)
      - df.rename(columns={"y": "x"})           → x <- y (transformed=True)
      - .assign(x=lambda d: d["y"].str.upper()) → x <- y (transformed=True)  [best-effort]
    Relation is unknown ("?"); full mapping across multiple inputs would require deeper analysis.
    """
    code = source_code or ""
    try:
        if not code and func is not None:
            code = inspect.getsource(func)
    except Exception:
        pass

    lineage: LineageMap = {}
    if not code:
        return lineage

    # Assign pattern: out["x"] = df["y"]
    for m in _ASSIGN_RE.finditer(code):
        out_col = m.group("out")
        src_col = m.group("col")
        _append_lineage(
            lineage, out_col, {"from_relation": "?", "from_column": src_col, "transformed": False}
        )

    # Rename pattern: .rename(columns={"old":"new"})
    for m in _RENAME_RE.finditer(code):
        pairs = m.group("pairs")
        for p in _PAIR_RE.finditer(pairs):
            old, new = p.group("old"), p.group("new")
            _append_lineage(
                lineage, new, {"from_relation": "?", "from_column": old, "transformed": True}
            )

    # assign(x=lambda d: ...)
    for m in _ASSIGN_LAMBDA_RE.finditer(code):
        out_col = m.group(1)
        body = m.group("body")
        m2 = _BODY_SRC_COL.search(body)
        if m2:
            src_col = m2.group("col")
            _append_lineage(
                lineage,
                out_col,
                {"from_relation": "?", "from_column": src_col, "transformed": True},
            )

    return lineage


# ────────────────────────────────────────────────────────────────────────────────
# Overrides (YAML / SQL comment directives)
# ────────────────────────────────────────────────────────────────────────────────

_LINEAGE_DIRECTIVE = re.compile(
    r"""--\s*@lineage\s+([_a-zA-Z]\w*)\s*:\s*([_a-zA-Z]\w*)\.([_a-zA-Z]\w*)
        (?:\s*\(\s*(transformed)\s*\))?""",
    re.IGNORECASE | re.VERBOSE,
)


def parse_sql_lineage_overrides(sql_text: str) -> LineageMap:
    """
    Parse optional SQL comment directives:
        -- @lineage email_upper: users.email (transformed)
    """
    out: LineageMap = {}
    for m in _LINEAGE_DIRECTIVE.finditer(sql_text or ""):
        out_col, rel, col, tr = m.group(1), m.group(2), m.group(3), m.group(4)
        out.setdefault(out_col, []).append(
            {"from_relation": rel, "from_column": col, "transformed": bool(tr)}
        )
    return out


def merge_lineage(*maps: LineageMap | None) -> LineageMap:
    """
    Merge multiple lineage maps. Later maps override/extend earlier ones by output column.
    If a later map provides any entries for a column, it replaces previous entries for that column.
    """
    merged: LineageMap = {}
    for mp in maps:
        if not mp:
            continue
        for out_col, items in mp.items():
            merged[out_col] = list(items)
    return merged
