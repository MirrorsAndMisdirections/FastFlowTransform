# src/fastflowtransform/fingerprint.py
from __future__ import annotations

import hashlib
import inspect
import json
import os
import textwrap
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .core import Node, relation_for

# ---------- Canonicalization helpers ----------


def _as_primitive(obj: Any) -> Any:
    """
    Convert complex Python objects into JSON-serializable primitives with stable ordering.
    - dicts → {sorted keys}
    - sets  → sorted lists
    - tuples → lists
    - Path  → string path
    - Node  → minimal stable representation (name/kind/path/dep names)
    """
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, Node):
        return {
            "name": obj.name,
            "kind": obj.kind,
            "path": str(obj.path),
            "deps": sorted(list(obj.deps or [])),
            # meta intentionally omitted unless caller passes it explicitly
        }
    if isinstance(obj, (list, tuple)):
        return [_as_primitive(x) for x in obj]
    if isinstance(obj, set):
        return sorted(_as_primitive(x) for x in obj)
    if isinstance(obj, Mapping):
        return {str(k): _as_primitive(v) for k, v in sorted(obj.items(), key=lambda kv: str(kv[0]))}
    # Fallback: repr() is stable enough for primitives; for functions use dedicated source helper
    return repr(obj)


def _stable_dumps(obj: Any) -> str:
    """
    Serialize an object to a deterministic JSON string:
    - keys sorted
    - minimal separators
    - non-ASCII preserved
    """
    prim = _as_primitive(obj)
    return json.dumps(prim, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash_hex(payload: str) -> str:
    """Return a hex SHA-256 over the given payload string."""
    h = hashlib.sha256()
    h.update(payload.encode("utf-8"))
    return h.hexdigest()


def _normalize_sql(sql: str) -> str:
    """
    Normalize SQL minimally for cross-platform consistency:
    - Normalize line endings to '\n'
    - Strip trailing whitespace on each line
    NOTE: We intentionally DO NOT collapse spaces or comments; even small changes
    should alter the fingerprint as per acceptance criteria.
    """
    lines = sql.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    return "\n".join(line.rstrip() for line in lines).strip("\n")


# ---------- sources.yml normalization ----------


def normalized_sources_blob(sources: Mapping[str, Any] | None) -> str:
    """
    Return a stable JSON blob for a sources.yml mapping.
    Keys are sorted recursively; absent input becomes "{}".
    """
    return _stable_dumps(sources or {})


# ---------- environment context ----------


@dataclass(frozen=True)
class EnvCtx:
    """
    Stable environment context used for fingerprinting.
    Include only inputs that should invalidate compiled artifacts when they change.
    """

    engine: str
    profile: str
    env_vars: Mapping[str, str]
    sources_json: str

    def to_payload(self) -> Mapping[str, Any]:
        return {
            "engine": self.engine,
            "profile": self.profile,
            "env": {k: self.env_vars.get(k, "") for k in sorted(self.env_vars.keys())},
            "sources": self.sources_json,
        }


def build_env_ctx(
    *,
    engine: str,
    profile_name: str,
    relevant_env_keys: Iterable[str] = (),
    sources: Mapping[str, Any] | None = None,
) -> EnvCtx:
    """
    Construct an EnvCtx from engine/profile + a curated set of environment variables
    and the (normalized) sources.yml mapping.
    Only the provided environment keys are captured; all others are ignored.
    """
    env_subset: dict[str, str] = {}
    for key in sorted(set(relevant_env_keys)):
        val = os.getenv(key)
        if val is not None:
            env_subset[key] = val
    return EnvCtx(
        engine=str(engine),
        profile=str(profile_name),
        env_vars=env_subset,
        sources_json=normalized_sources_blob(sources),
    )


# ---------- robust function source retrieval ----------


def get_function_source(func: Any) -> str:
    """
    Return a best-effort, stable source string for a Python callable.

    Strategy (in order):
    1) inspect.getsource(func)  → dedented string
    2) Read the defining file (co_filename) and slice starting at co_firstlineno
       until the next top-level def/class (heuristic). Dedent as needed.
    3) Final fallback: combine qualified name and bytecode to ensure stability.

    This ensures fingerprinting works even for dynamically loaded modules, lambdas,
    or environments where inspect cannot read the original file (e.g., zipimport).
    """
    # 1) The happy path
    try:
        src = inspect.getsource(func)
        return textwrap.dedent(src).strip()
    except Exception:
        pass

    # 2) Slice from file using code object hints
    try:
        code = getattr(func, "__code__", None)
        if code and isinstance(code.co_firstlineno, int) and code.co_filename:
            file_path = Path(code.co_filename)
            # Read as binary + decode to be robust to odd encodings
            with open(file_path, "rb") as fh:
                raw = fh.read()
            text = raw.decode("utf-8", errors="replace")
            start = max(code.co_firstlineno - 1, 0)

            lines = text.splitlines()
            # Heuristic: collect until the next top-level def/class (same or less indent)
            buf: list[str] = []
            base_indent = None
            for idx in range(start, len(lines)):
                line = lines[idx]
                buf.append(line)
                # capture base indentation from the first non-empty line
                if base_indent is None and line.strip():
                    base_indent = len(line) - len(line.lstrip())
                # stop when we hit a new top-level def/class after the first line
                if (
                    idx > start
                    and line
                    and not line.startswith(" " * (base_indent or 0))
                    and line.lstrip().startswith(("def ", "class ", "@"))
                ):
                    buf.pop()  # don't include the new top-level symbol
                    break
            sliced = "\n".join(buf)
            return textwrap.dedent(sliced).strip()
    except Exception:
        pass

    # 3) Last resort: qualname + bytecode hash
    try:
        qual = getattr(func, "__qualname__", getattr(func, "__name__", "anonymous"))
        bc = getattr(getattr(func, "__code__", None), "co_code", b"")
        payload = f"{qual}\nBYTECODE:{hashlib.sha256(bc).hexdigest()}"
        return payload
    except Exception:
        return "UNKNOWN_FUNCTION"


# ---------- Fingerprint calculators ----------


def fingerprint_sql(
    *,
    node: Node | str,
    rendered_sql: str,
    env_ctx: EnvCtx | Mapping[str, Any],
    dep_fps: Mapping[str, str] | None = None,
) -> str:
    """
    Compute a stable fingerprint for a SQL model.
    Inputs:
      - node         : Node or node name for stable identity and relation
      - rendered_sql : final SQL after templating (ref()/source() resolved as in executor)
      - env_ctx      : EnvCtx or compatible mapping (engine, profile, selected env vars, sources)
      - dep_fps      : mapping of dependency name → fingerprint (to invalidate downstream)
    """
    n_name = node.name if isinstance(node, Node) else str(node)
    payload = {
        "kind": "sql",
        "node": n_name,
        "relation": relation_for(n_name),
        "sql": _normalize_sql(rendered_sql),
        "env": env_ctx.to_payload() if isinstance(env_ctx, EnvCtx) else _as_primitive(env_ctx),
        "deps": _as_primitive(sorted((dep_fps or {}).items(), key=lambda kv: kv[0])),
    }
    return _hash_hex(_stable_dumps(payload))


def fingerprint_py(
    *,
    node: Node | str,
    func_src: str,
    env_ctx: EnvCtx | Mapping[str, Any],
    dep_fps: Mapping[str, str] | None = None,
) -> str:
    """
    Compute a stable fingerprint for a Python model.
    Inputs:
      - node     : Node or node name
      - func_src : normalized function source (use get_function_source)
      - env_ctx  : EnvCtx or compatible mapping
      - dep_fps  : mapping of dependency name → fingerprint
    """
    n_name = node.name if isinstance(node, Node) else str(node)
    payload = {
        "kind": "python",
        "node": n_name,
        "relation": relation_for(n_name),
        "func_src": func_src.replace("\r\n", "\n").replace("\r", "\n").strip(),
        "env": env_ctx.to_payload() if isinstance(env_ctx, EnvCtx) else _as_primitive(env_ctx),
        "deps": _as_primitive(sorted((dep_fps or {}).items(), key=lambda kv: kv[0])),
    }
    return _hash_hex(_stable_dumps(payload))
