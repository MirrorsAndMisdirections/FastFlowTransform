# src/fastflowtransform/dag.py
import heapq
import re
from collections import defaultdict

from .core import Node, relation_for
from .errors import DependencyNotFoundError, ModelCycleError


def topo_sort(nodes: dict[str, Node]) -> list[str]:
    missing = {
        n.name: sorted({d for d in n.deps if d not in nodes})
        for n in nodes.values()
        if any(d not in nodes for d in n.deps)
    }
    if missing:
        raise DependencyNotFoundError(missing)

    indeg = {k: 0 for k in nodes}
    out: dict[str, set[str]] = defaultdict(set)
    for n in nodes.values():
        for d in set(n.deps):
            out[d].add(n.name)
            indeg[n.name] += 1

    heap = [k for k, deg in indeg.items() if deg == 0]
    heapq.heapify(heap)
    order: list[str] = []
    while heap:
        u = heapq.heappop(heap)
        order.append(u)
        for v in sorted(out.get(u, ())):
            indeg[v] -= 1
            if indeg[v] == 0:
                heapq.heappush(heap, v)

    if len(order) != len(nodes):
        cyclic = [k for k, deg in indeg.items() if deg > 0]
        raise ModelCycleError(f"Cycle detected among nodes: {', '.join(sorted(cyclic))}")
    return order


def levels(nodes: dict[str, Node]) -> list[list[str]]:
    """
    Returns a level-wise topological ordering.
    - Each inner list contains nodes with no prerequisites inside the remaining
      graph (i.e. eligible to run in parallel).
    - Ordering within a level is lexicographically stable.
    - Validation for missing deps/cycles matches topo_sort.
    """
    # Fehlende Deps einsammeln (nur Modell-Refs; sources sind keine Nodes)
    missing = {
        n.name: sorted({d for d in (n.deps or []) if d not in nodes})
        for n in nodes.values()
        if any(d not in nodes for d in (n.deps or []))
    }
    if missing:
        raise DependencyNotFoundError(missing)

    indeg = {k: 0 for k in nodes}
    out: dict[str, set[str]] = defaultdict(set)
    for n in nodes.values():
        for d in set(n.deps or []):
            out[d].add(n.name)
            indeg[n.name] += 1

    # Start-Level: alle 0-Indegree
    current = sorted([k for k, deg in indeg.items() if deg == 0])
    lvls: list[list[str]] = []
    seen_count = 0

    while current:
        lvls.append(current)
        next_zero: set[str] = set()
        for u in current:
            seen_count += 1
            for v in sorted(out.get(u, ())):
                indeg[v] -= 1
                if indeg[v] == 0:
                    next_zero.add(v)
        current = sorted(next_zero)

    if seen_count != len(nodes):
        cyclic = [k for k, deg in indeg.items() if deg > 0]
        raise ModelCycleError(f"Cycle detected among nodes: {', '.join(sorted(cyclic))}")
    return lvls


def _mm_id(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9_]", "_", name)
    return "_" + s if s and s[0].isdigit() else (s or "_node")


def _quote_label(s: str) -> str:
    s = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def _source_node_id(source_name: str, table_name: str) -> str:
    return _mm_id(f"src_{source_name}_{table_name}")


def mermaid(
    nodes: dict[str, Node],
    source_links: dict[tuple[str, str], dict[str, str]] | None = None,
    model_source_refs: dict[str, list[tuple[str, str]]] | None = None,
) -> str:
    lines = [
        "flowchart TD",
        "  classDef sql fill:#e8f1ff,stroke:#5b8def,color:#0a1f44;",
        "  classDef py  fill:#e9fbf1,stroke:#2bb673,color:#0b2e1f;",
    ]
    if source_links:
        lines.append("  classDef source fill:#fef3c7,stroke:#f59e0b,color:#78350f;")

    # Nodes
    for n in sorted(nodes.values(), key=lambda x: x.name):
        nid = _mm_id(n.name)
        phys = relation_for(n.name)
        label = _quote_label(f"{n.name}<br/>({phys})")
        if n.kind == "python":
            lines.append(f"  {nid}({label})")
            lines.append(f"  class {nid} py;")
        else:
            lines.append(f"  {nid}[{label}]")
            lines.append(f"  class {nid} sql;")
        lines.append(f'  click {nid} "{n.name}.html" "View model"')

    source_ids: dict[tuple[str, str], str] = {}
    if source_links:
        for (src_name, tbl_name), meta in sorted(source_links.items(), key=lambda x: x[0]):
            sid = _source_node_id(src_name, tbl_name)
            label = _quote_label(meta.get("label") or f"{src_name}.{tbl_name}")
            lines.append(f"  {sid}[[{label}]]")
            lines.append(f"  class {sid} source;")
            link = meta.get("file")
            if link:
                lines.append(f'  click {sid} "{link}" "View source"')
            source_ids[(src_name, tbl_name)] = sid

    # Edges
    for n in nodes.values():
        tgt = _mm_id(n.name)
        for d in n.deps:
            if d in nodes:
                lines.append(f"  {_mm_id(d)} --> {tgt}")
        if model_source_refs and source_ids:
            for ref in model_source_refs.get(n.name, []):
                ref_id = source_ids.get(ref)
                if ref_id is None:
                    continue
                sid = ref_id
                lines.append(f"  {sid} --> {tgt}")

    lines.append("")
    return "\n".join(lines)
