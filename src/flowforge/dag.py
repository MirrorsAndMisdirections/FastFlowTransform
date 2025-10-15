# # src/flowforge/dag.py
# from __future__ import annotations
# from collections import defaultdict
# from typing import Dict, List, Set
# import heapq
# import re

# from .core import Node, relation_for
# from .errors import DependencyNotFoundError, ModelCycleError


# def topo_sort(nodes: Dict[str, Node]) -> List[str]:
#     """
#     Kahn algorithm with validation and deterministic output:
#       - Missing deps → DependencyNotFoundError with a map {node: [missing...]}
#       - Cycles → ModelCycleError with the affected nodes
#       - Nodes on the same level are ordered alphabetically
#     """
#     # 1) Collect missing dependencies cleanly
#     missing_map: Dict[str, List[str]] = {}
#     for n in nodes.values():
#         miss = [d for d in (n.deps or []) if d not in nodes]
#         if miss:
#             missing_map[n.name] = sorted(set(miss))
#     if missing_map:
#         # The exception accepts a map, which keeps the error helpful
#         raise DependencyNotFoundError(missing_map)

#     # 2) Build the graph (avoid duplicate deps)
#     indeg: Dict[str, int] = {k: 0 for k in nodes}
#     out: Dict[str, Set[str]] = defaultdict(set)
#     for n in nodes.values():
#         for d in set(n.deps or []):
#             out[d].add(n.name)
#             indeg[n.name] += 1

#     # 3) Kahn with min-heap → deterministic ordering
#     heap: List[str] = [k for k, deg in indeg.items() if deg == 0]
#     heapq.heapify(heap)

#     order: List[str] = []
#     while heap:
#         u = heapq.heappop(heap)
#         order.append(u)
#         for v in sorted(out.get(u, ())):
#             indeg[v] -= 1
#             if indeg[v] == 0:
#                 heapq.heappush(heap, v)

#     if len(order) != len(nodes):
#         cyclic = [k for k, deg in indeg.items() if deg > 0]
#         raise ModelCycleError(f"Cycle detected among nodes: {', '.join(sorted(cyclic))}")

#     return order


# def _mm_id(name: str) -> str:
#     """Mermaid-safe ID (no dots/hyphen etc.)."""
#     s = re.sub(r"[^A-Za-z0-9_]", "_", name)
#     if s and s[0].isdigit():
#         s = "_" + s
#     return s or "_node"


# def _escape_label(s: str) -> str:
#     # Escapen, damit die Shape-Klammern nicht „innen“ kollidieren
#     s = s.replace("\\", "\\\\")
#     for ch in ("[", "]", "(", ")", "{", "}"):
#         s = s.replace(ch, "\\" + ch)
#     return s


# def mermaid(nodes: Dict[str, Node], include_missing: bool = False) -> str:
#     """
#     Return a Mermaid flowchart (top-down).
#     - SQL models: blue, Python models: green
#     - Label includes logical name + physical relation (relation_for)
#     - Optional: show unknown/missing deps as dashed edges
#     """
#     lines: List[str] = []
#     lines.append("flowchart TD")
#     lines.append("  %% Styles")
#     lines.append("  classDef sql fill:#e8f1ff,stroke:#5b8def,color:#0a1f44;")
#     lines.append("  classDef py  fill:#e9fbf1,stroke:#2bb673,color:#0b2e1f;")
#     lines.append("  classDef src fill:#fff7e6,stroke:#f5a623,color:#4a2a00;")

#     # Nodes (stable sort)
#     for n in sorted(nodes.values(), key=lambda x: x.name):
#         nid = _mm_id(n.name)
#         phys = relation_for(n.name)
#         label = _escape_label(f"{n.name}<br/>({phys})")
#         # Stadium shape for both; customize if needed
#         if n.kind == "python":
#             lines.append(f"  {nid}({label})")
#             lines.append(f"  class {nid} py;")
#         else:
#             lines.append(f"  {nid}[{label}]")
#             lines.append(f"  class {nid} sql;")

#     # Edges
#     for n in nodes.values():
#         tgt = _mm_id(n.name)
#         for d in (n.deps or []):
#             if d in nodes:
#                 src = _mm_id(d)
#                 lines.append(f"  {src} --> {tgt}")
#             elif include_missing:
#                 sid = _mm_id('missing_' + d)
#                 lines.append(f"  {sid}[[{d}]]")
#                 lines.append(f"  class {sid} src;")
#                 lines.append(f"  {sid} -.-> {tgt}")

#     lines.append("")
#     return "\n".join(lines) + "\n"


# src/flowforge/dag.py
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


def _mm_id(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9_]", "_", name)
    return "_" + s if s and s[0].isdigit() else (s or "_node")


def _quote_label(s: str) -> str:
    # Nur für Mermaid-Label: <br/> ist ok (mit securityLevel 'loose')
    s = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def mermaid(nodes: dict[str, Node]) -> str:
    lines = [
        "flowchart TD",
        "  classDef sql fill:#e8f1ff,stroke:#5b8def,color:#0a1f44;",
        "  classDef py  fill:#e9fbf1,stroke:#2bb673,color:#0b2e1f;",
    ]

    # Nodes
    for n in sorted(nodes.values(), key=lambda x: x.name):
        nid = _mm_id(n.name)
        phys = relation_for(n.name)
        # Wichtig: Label quoten, KEINE Backslashes vor Klammern
        label = _quote_label(f"{n.name}<br/>({phys})")
        if n.kind == "python":
            lines.append(f"  {nid}({label})")  # runde Ecken
            lines.append(f"  class {nid} py;")
        else:
            lines.append(f"  {nid}[{label}]")  # Rechteck
            lines.append(f"  class {nid} sql;")

    # Edges
    for n in nodes.values():
        tgt = _mm_id(n.name)
        for d in n.deps:
            if d in nodes:
                lines.append(f"  {_mm_id(d)} --> {tgt}")

    lines.append("")
    return "\n".join(lines)
