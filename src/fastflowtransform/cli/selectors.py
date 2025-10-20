from __future__ import annotations

import fnmatch
from collections.abc import Callable, Iterable
from typing import Any, cast

from fastflowtransform.core import relation_for


def _parse_select(parts: list[str]) -> list[str]:
    """Accept multiple --select occurrences or a single space-separated string."""
    out: list[str] = []
    for p in parts:
        out.extend(s for s in str(p).split() if s)
    return out


def _selector(predicates: Iterable[Callable[[Any], bool]]) -> Callable[[Any], bool]:
    preds = list(predicates)
    if not preds:
        return lambda n: True
    return lambda n: all(p(n) for p in preds)


def _build_predicates(tokens: list[str]) -> list[Callable[[Any], bool]]:
    """
    Supported tokens:
      - name glob: e.g. orders*, marts_*  (matches Node.name and physical relation name)
      - tag:<tag> : matches Node.meta['tags'] (list or str)
      - type:<view|table|ephemeral> : matches Node.meta['materialized'] (default 'table')
      - kind:<sql|python> : matches Node.kind
    AND across tokens.
    """
    preds: list[Callable[[Any], bool]] = []
    for tok in tokens:
        if tok.startswith("tag:"):
            want = tok.split(":", 1)[1]

            def _p(n, w=want):
                tags = (getattr(n, "meta", {}) or {}).get("tags")
                if isinstance(tags, list):
                    return w in tags
                return tags == w

            preds.append(_p)
        elif tok.startswith("type:"):
            want = tok.split(":", 1)[1]
            preds.append(
                cast(
                    Callable[[Any], bool],
                    lambda n, w=want: (getattr(n, "meta", {}) or {}).get("materialized", "table")
                    == w,
                )
            )
        elif tok.startswith("kind:"):
            want = tok.split(":", 1)[1]
            preds.append(cast(Callable[[Any], bool], lambda n, w=want: n.kind == w))
        else:
            pattern = tok
            preds.append(
                cast(
                    Callable[[Any], bool],
                    lambda n, pat=pattern: fnmatch.fnmatch(n.name, pat)
                    or fnmatch.fnmatch(relation_for(n.name), pat),
                )
            )
    return preds


def _compile_selector(
    select_opt: list[str] | None,
) -> tuple[list[str], Callable[[Any], bool]]:
    """Normalize `--select` values and return tokens plus predicate."""
    tokens = _parse_select(select_opt or [])
    return tokens, _selector(_build_predicates(tokens))


def _selected_subgraph_names(
    nodes: dict[str, Any],
    select_tokens: list[str] | None,
    exclude_tokens: list[str] | None,
) -> set[str]:
    """
    Compute the reduced set of node names to execute:
      1) Seeds = nodes matching --select (or all if --select omitted)
      2) Remove excluded nodes and their downstream closure
      3) Final = upstream-closure of remaining seeds within the remaining graph
    This guarantees that all dependencies of every kept seed are present;
    if a required dep was excluded, the affected seed is dropped.
    """
    if not nodes:
        return set()

    _, sel_pred = _compile_selector(select_tokens or [])
    _, ex_pred = _compile_selector(exclude_tokens or [])

    all_names = set(nodes.keys())
    seeds = {n for n in all_names if (sel_pred(nodes[n]) if select_tokens else True)}
    if not seeds:
        return set()

    deps_map: dict[str, set[str]] = {n: set(nodes[n].deps or []) for n in all_names}
    rev_map: dict[str, set[str]] = {n: set() for n in all_names}
    for u, ds in deps_map.items():
        for d in ds:
            if d in rev_map:
                rev_map[d].add(u)

    excluded = {n for n in all_names if (ex_pred(nodes[n]) if exclude_tokens else False)}
    if excluded:
        stack = list(excluded)
        downstream = set()
        while stack:
            cur = stack.pop()
            for v in rev_map.get(cur, ()):
                if v not in downstream and v not in excluded:
                    downstream.add(v)
                    stack.append(v)
        removed = excluded | downstream
    else:
        removed = set()

    remaining = all_names - removed
    seeds = seeds - removed
    if not seeds:
        return set()

    result: set[str] = set()
    stack = list(seeds)
    while stack:
        cur = stack.pop()
        if cur in result or cur not in remaining:
            continue
        result.add(cur)
        for d in deps_map.get(cur, ()):
            if d in remaining:
                stack.append(d)
    return result
