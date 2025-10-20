# src/flowforge/cache.py
from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .core import relation_for
from .meta import relation_exists as _relation_exists_engine


@dataclass
class FingerprintCache:
    """
    Lightweight, project-scoped fingerprint store.

    The cache is persisted under:
        <project>/.flowforge/cache/<profile>-<engine>.json

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
        base = self.project_dir / ".flowforge" / "cache"
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
