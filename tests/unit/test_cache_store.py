# tests/unit/test_cache_store.py
from __future__ import annotations

from pathlib import Path

from flowforge.cache import FingerprintCache


def test_cache_persist_roundtrip(tmp_path: Path):
    proj = tmp_path
    c1 = FingerprintCache(proj, profile="dev", engine="duckdb")
    c1.load()  # empty start
    c1.set("users.ff", "abc")
    c1.set("orders.ff", "def")
    c1.save()

    c2 = FingerprintCache(proj, profile="dev", engine="duckdb")
    c2.load()
    assert c2.get("users.ff") == "abc"
    assert c2.get("orders.ff") == "def"
