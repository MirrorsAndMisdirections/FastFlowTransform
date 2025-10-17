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
