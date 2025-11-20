import importlib
import json
from pathlib import Path

import pytest

from fastflowtransform.api import context as ctx, http


def _seed_cache(http_mod, cache_dir: Path, url: str, params: dict | None, body_obj) -> None:
    """
    Create a valid cache entry for the (GET, url, params) combination.
    """
    key = http_mod._cache_key("GET", url, params, headers={})
    body = json.dumps(body_obj, ensure_ascii=False).encode("utf-8")
    http_mod._write_cache(key, 200, {}, body, url)


@pytest.mark.unit
@pytest.mark.http
def test_get_json_offline_cache_hit_records_stats(monkeypatch, tmp_path):
    # Set ENV variables and reload the module so it picks them up
    monkeypatch.setenv("FF_HTTP_OFFLINE", "1")
    monkeypatch.setenv("FF_HTTP_CACHE_DIR", str(tmp_path))
    # Cache mode does not matter here; rw is the default
    importlib.reload(http)

    url = "https://api.example.com/users"
    params = {"page": 1}
    payload = {"data": [{"id": 1, "email": "a@example.com"}]}
    _seed_cache(http, Path(tmp_path), url, params, payload)

    # Initialize telemetry context for the node (same as the executor does)
    ctx.reset_for_node("unit_node")

    out = http.get_json(url, params=params)
    assert out == payload  # exactly the payload stored in the cache

    snap = ctx.snapshot()
    # Exactly one request, one cache hit, offline true, bytes greater than zero
    assert snap["requests"] == 1
    assert snap["cache_hits"] == 1
    assert snap["used_offline"] is True
    assert snap["bytes"] > 0
    assert isinstance(snap["keys"], list) and len(snap["keys"]) == 1


@pytest.mark.unit
@pytest.mark.http
def test_get_json_cache_hit_online_not_reported_offline(monkeypatch, tmp_path):
    monkeypatch.setenv("FF_HTTP_OFFLINE", "0")
    monkeypatch.setenv("FF_HTTP_CACHE_DIR", str(tmp_path))
    importlib.reload(http)

    url = "https://api.example.com/users"
    params = {"page": 1}
    payload = {"data": [{"id": 1}]}
    _seed_cache(http, Path(tmp_path), url, params, payload)

    ctx.reset_for_node("online_node")

    out = http.get_json(url, params=params)
    assert out == payload

    snap = ctx.snapshot()
    assert snap["cache_hits"] == 1
    assert snap["used_offline"] is False
