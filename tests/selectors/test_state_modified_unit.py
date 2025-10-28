from pathlib import Path

from fastflowtransform.cli.selectors import _downstream_closure
from fastflowtransform.core import REGISTRY, Node


def test_downstream_closure_simple():
    REGISTRY.nodes.clear()
    REGISTRY.nodes.update(
        {
            "a.ff": Node("a.ff", "sql", path=Path("dummy"), deps=[]),
            "b.ff": Node("b.ff", "sql", path=Path("dummy"), deps=["a.ff"]),
            "c.ff": Node("c.ff", "sql", path=Path("dummy"), deps=["b.ff"]),
            "x.ff": Node("x.ff", "sql", path=Path("dummy"), deps=[]),
        }
    )
    ds = _downstream_closure({"a.ff"})
    assert ds == {"a.ff", "b.ff", "c.ff"}
