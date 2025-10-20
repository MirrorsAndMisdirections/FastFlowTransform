# src/fastflowtransform/decorators.py  (or wherever your decorator lives)
from __future__ import annotations

import inspect
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, ParamSpec, Protocol, TypeVar, cast

from .core import REGISTRY, relation_for  # relation_for is required for normalization
from .errors import ModuleLoadError

P = ParamSpec("P")
R_co = TypeVar("R_co", covariant=True)


class HasFFMeta(Protocol[P, R_co]):
    __ff_name__: str
    __ff_deps__: list[str]
    __ff_require__: Any
    __ff_path__: Path

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co: ...


def _normalize_require(deps: list[str], require: Any | None) -> dict[str, set[str]]:
    """
    Accepts:
      - None
      - Iterable[str]  (for a single dependency)
      - Mapping[str, Iterable[str]]  (for multiple dependencies)
    Keys may be logical dependency names (e.g. 'orders.ff' or 'users_enriched');
    they are mapped to physical relations via relation_for(...).
    """
    if not require:
        return {}
    if len(deps) == 1 and not isinstance(require, Mapping):
        cols = set(cast(Iterable[str], require))
        return {relation_for(deps[0]): cols}
    if isinstance(require, Mapping):
        out: dict[str, set[str]] = {}
        for k, cols in require.items():
            out[relation_for(cast(str, k))] = set(cast(Iterable[str], cols))
        return out
    raise TypeError(
        "require must be a list/set for single dependency "
        "or a dict[dep_name, list[str]] for multiple dependencies"
    )


def model(
    name: str | None = None,
    deps: Sequence[str] | None = None,
    require: Any | None = None,  # <-- NEW: required columns
) -> Callable[[Callable[P, R_co]], HasFFMeta[P, R_co]]:
    """
    Decorator to register a Python model.

    Args:
        name: Logical node name in the DAG (defaults to function name).
        deps: Upstream node names (e.g., ['users.ff']).
        require:
            - Single dependency: Iterable[str] of required columns from that dependency.
            - Multiple dependencies: Mapping[dep_name, Iterable[str]]
              (dep_name = logical name or physical relation).
    """

    def deco(func: Callable[P, R_co]) -> HasFFMeta[P, R_co]:
        f_any = cast(Any, func)
        fname = name or f_any.__name__
        fdeps = list(deps) if deps is not None else []

        # Attach metadata to the function (keeps backward compatibility)
        f_any.__ff_name__ = fname
        f_any.__ff_deps__ = fdeps

        # Normalize require and mirror it on the function and inside the registry
        req_norm = _normalize_require(fdeps, require)
        f_any.__ff_require__ = req_norm  # useful for tooling/loaders
        REGISTRY.py_requires[fname] = req_norm  # executors read this directly

        # Determine the source path (better error message if it fails)
        src: str | None = inspect.getsourcefile(func)
        if src is None:
            try:
                src = inspect.getfile(func)
            except Exception as e:
                raise ModuleLoadError(
                    f"Cannot determine source path for model '{fname}': {e}"
                ) from e

        f_any.__ff_path__ = Path(src).resolve()

        # Register the function
        REGISTRY.py_funcs[fname] = func
        return cast(HasFFMeta[P, R_co], func)

    return deco
