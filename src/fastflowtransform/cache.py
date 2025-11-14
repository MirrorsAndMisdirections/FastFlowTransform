# src/fastflowtransform/cache.py
from __future__ import annotations

import builtins
import hashlib
import inspect
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment

from fastflowtransform.core import REGISTRY, relation_for
from fastflowtransform.dag import topo_sort
from fastflowtransform.meta import relation_exists as _relation_exists_engine


@dataclass
class FingerprintCache:
    """
    Lightweight, project-scoped fingerprint store.

    The cache is persisted under:
        <project>/.fastflowtransform/cache/<profile>-<engine>.json

    Schema:
    {
      "version": 1,
      "engine": "<engine>",
      "profile": "<profile>",
      "entries": { "<node_name>": "<sha256-hex>", ... }
    }
    """

    project_dir: Path
    profile: str
    engine: str
    version: int = 1
    entries: dict[str, str] = field(default_factory=dict)

    @property
    def path(self) -> Path:
        base = self.project_dir / ".fastflowtransform" / "cache"
        base.mkdir(parents=True, exist_ok=True)
        filename = f"{self.profile}-{self.engine}.json"
        return base / filename

    def load(self) -> None:
        """Load cache file if present; silently do nothing when missing or corrupt."""
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and raw.get("version") == self.version:
                self.entries = dict(raw.get("entries") or {})
        except Exception:
            # On any error, start with an empty cache
            self.entries = {}

    def save(self) -> None:
        """Persist cache atomically."""
        payload = {
            "version": self.version,
            "engine": self.engine,
            "profile": self.profile,
            "entries": self.entries,
        }
        tmp_fd, tmp_name = tempfile.mkstemp(prefix=".ff-cache-", dir=str(self.path.parent))
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, sort_keys=True, indent=2)
            os.replace(tmp_name, self.path)
        finally:
            try:
                if os.path.exists(tmp_name):
                    os.remove(tmp_name)
            except Exception:
                pass

    def get(self, node_name: str) -> str | None:
        """Return cached fingerprint for a node or None."""
        return self.entries.get(node_name)

    def set(self, node_name: str, fingerprint: str) -> None:
        """Set cached fingerprint for a node name."""
        self.entries[node_name] = fingerprint

    def update_many(self, fps: Mapping[str, str]) -> None:
        """Bulk update cache entries."""
        for k, v in fps.items():
            self.entries[k] = v

    # ------------------------ read-only fingerprint computation ------------------------
    def _env_ctx_blob(self) -> str:
        """
        Build a stable JSON blob for environment context used in the fingerprint:
          - engine (from cache instance)
          - profile (from cache instance)
          - all FF_* environment variables (key+value)
          - normalized sources.yml (best-effort)
        """
        ff_env = {k: v for k, v in os.environ.items() if k.startswith("FF_")}
        try:
            src_norm = yaml.safe_dump(REGISTRY.sources or {}, sort_keys=True)
        except Exception:
            src_norm = ""
        payload = {
            "engine": self.engine,
            "profile": self.profile,
            "ff_env": ff_env,
            "sources": src_norm,
        }
        return json.dumps(payload, sort_keys=True)

    def compute_current(self, env: Environment, executor: Any) -> dict[str, str]:
        """
        Compute CURRENT fingerprints for all registered nodes (read-only).
        Uses the documented formula:
          - SQL: rendered SQL (via executor.render_sql to mirror real run)
          - Python: function source (inspect.getsource) with file-content fallback
          - env_ctx blob (engine/profile/FF_* vars/sources.yml)
          - dependency fingerprints chained downstream
        Does NOT write to disk.
        """
        env_ctx_blob = self._env_ctx_blob()

        # Preload sources for SQL / Python
        sql_render: dict[str, str] = {}
        py_src: dict[str, str] = {}

        for name, node in REGISTRY.nodes.items():
            if node.kind == "sql":
                try:
                    # Render with same substitutions as in run()
                    sql_render[name] = executor.render_sql(node, env)
                except Exception:
                    # Fallback: raw template content to still capture file changes
                    try:
                        raw = node.path.read_text(encoding="utf-8") if node.path else ""
                    except Exception:
                        raw = ""
                    sql_render[name] = raw
            else:
                func = REGISTRY.py_funcs.get(name)
                src = ""
                if func is not None:
                    try:
                        src = inspect.getsource(func)
                    except Exception:
                        try:
                            src = node.path.read_text(encoding="utf-8") if node.path else ""
                        except Exception:
                            src = ""
                py_src[name] = src

        def _hash(parts: list[str]) -> str:
            h = hashlib.sha256()
            for part in parts:
                h.update(part.encode("utf-8"))
                h.update(b"\x00")
            return h.hexdigest()

        current: dict[str, str] = {}
        order = topo_sort(REGISTRY.nodes)
        for name in order:
            node = REGISTRY.nodes[name]
            dep_fps = [current[d] for d in (node.deps or []) if d in current]
            if node.kind == "sql":
                blob = ["sql", name, sql_render.get(name, ""), env_ctx_blob, *dep_fps]
            else:
                blob = ["py", name, py_src.get(name, ""), env_ctx_blob, *dep_fps]
            current[name] = _hash(blob)
        return current

    def modified_set(self, env: Environment, executor: Any) -> builtins.set[str]:
        """
        Return the set of nodes whose CURRENT fingerprint differs from saved cache.
        Missing saved entries count as modified.
        """
        # Ensure we have the saved entries loaded
        if not self.entries:
            self.load()
        current = self.compute_current(env, executor)
        modified = {n for n, fp in current.items() if self.entries.get(n) != fp}
        return modified


# ------------------------ artifact existence helpers ------------------------


def relation_exists(executor: Any, relation: str) -> bool:
    """
    Compatibility wrapper that delegates to the engine-aware implementation.
    """
    return _relation_exists_engine(executor, relation)


def can_skip_node(
    *,
    node_name: str,
    new_fp: str,
    cache: FingerprintCache,
    executor: Any,
    materialized: str,
) -> bool:
    """
    Decide whether a node can be skipped based on:
      - identical fingerprint to cached entry
      - and existing materialized relation (unless ephemeral)
    """
    old = cache.get(node_name)
    if old is None or old != new_fp:
        return False
    if materialized == "ephemeral":
        return True
    rel = relation_for(node_name)
    return relation_exists(executor, rel)
