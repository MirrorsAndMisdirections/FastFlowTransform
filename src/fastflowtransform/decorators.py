# src/fastflowtransform/decorators.py
from __future__ import annotations

import inspect
import os
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, ParamSpec, Protocol, TypeVar, cast

from fastflowtransform.core import REGISTRY, relation_for
from fastflowtransform.errors import ModuleLoadError

P = ParamSpec("P")
R_co = TypeVar("R_co", covariant=True)


class HasFFMeta(Protocol[P, R_co]):
    __ff_name__: str
    __ff_deps__: list[str]
    __ff_require__: Any
    __ff_path__: Path
    __ff_tags__: list[str]
    __ff_kind__: str
    __ff_meta__: dict[str, Any]

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
    require: Any | None = None,
    *,
    tags: Sequence[str] | None = None,
    kind: str = "python",
    materialized: str | None = None,
    meta: Mapping[str, Any] | None = None,
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
        tags: Optional tags for selection (e.g. ['demo','env']).
        kind: Logical kind; defaults to 'python' (useful for selectors kind:python).
        materialized: Shorthand for meta['materialized']; mirrors config(materialized='...').
        meta: Arbitrary metadata for executors/docs (merged with materialized if provided).
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

        f_any.__ff_tags__ = list(tags) if tags else []
        f_any.__ff_kind__ = kind or "python"

        metadata = dict(meta) if meta else {}
        if materialized is not None:
            metadata["materialized"] = materialized
        f_any.__ff_meta__ = metadata

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


def engine_model(
    *, only: str | tuple[str, ...], **model_kwargs: Any
) -> Callable[[Callable[P, R_co]], HasFFMeta[P, R_co]]:
    allowed = {only} if isinstance(only, str) else {e.lower() for e in only}

    def deco(fn):
        current = os.getenv("FF_ENGINE", "").lower()
        if current in allowed:
            return model(**model_kwargs)(fn)
        return fn  # stays undecorated → no registry entry

    return deco
