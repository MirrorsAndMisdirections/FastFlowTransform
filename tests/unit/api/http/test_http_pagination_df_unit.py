import importlib
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from fastflowtransform.api import context as ctx, http


def _seed_cache(http_mod, cache_dir: Path, url: str, params: dict | None, body_obj) -> None:
    key = http_mod._cache_key("GET", url, params, headers={})
    body = json.dumps(body_obj, ensure_ascii=False).encode("utf-8")
    http_mod._write_cache(key, 200, {}, body, url)


@pytest.mark.unit
@pytest.mark.http
def test_get_df_pagination_concatenates(monkeypatch, tmp_path):
    """
    get_df with a paginator joins two offline pages correctly.
    """
    monkeypatch.setenv("FF_HTTP_OFFLINE", "1")
    monkeypatch.setenv("FF_HTTP_CACHE_DIR", str(tmp_path))
    # Mode rw (default) can read; offline plus cache hit makes the choice irrelevant
    importlib.reload(http)

    url1 = "https://api.example.com/users?page=1"
    url2 = "https://api.example.com/users?page=2"

    page1 = {"data": [{"id": 1, "email": "a@x"}, {"id": 2, "email": "b@x"}], "next": url2}
    page2 = {"data": [{"id": 3, "email": "c@x"}], "next": None}

    # Seed the cache the same way get_json/get_df access it (params
    # usually None when the query string is baked in)
    _seed_cache(http, Path(tmp_path), url1, None, page1)
    _seed_cache(http, Path(tmp_path), url2, None, page2)

    ctx.reset_for_node("unit_pager")

    def paginator(u: str, p: dict | None, js: dict):
        nxt = js.get("next")
        return {"next_request": {"url": nxt}} if nxt else None

    df = http.get_df(url1, paginator=paginator, record_path=["data"])
    assert isinstance(df, pd.DataFrame)
    # Expected: three rows, IDs 1..3
    expected_df_len = 3
    assert len(df) == expected_df_len
    assert set(df["id"].tolist()) == {1, 2, 3}

    snap = ctx.snapshot()
    # Two pages -> 2 requests, 2 hits
    request_count = 2
    cache_hit_count = 2
    assert snap["requests"] == request_count
    assert snap["cache_hits"] == cache_hit_count


@pytest.mark.unit
@pytest.mark.http
def test_raw_get_pagination_returns_pages(monkeypatch, tmp_path):
    monkeypatch.setenv("FF_HTTP_OFFLINE", "0")
    monkeypatch.setenv("FF_HTTP_CACHE_DIR", str(tmp_path))
    importlib.reload(http)

    calls: list[dict[str, Any]] = []

    def fake_http_request(method, u, *, params=None, headers=None, timeout=None):
        calls.append({"url": u, "params": params, "headers": headers})
        if "page=1" in u:
            body = json.dumps({"next": "https://api.example.com/users?page=2"}).encode("utf-8")
        else:
            body = json.dumps({"next": None}).encode("utf-8")
        return 200, {}, body

    monkeypatch.setattr(http, "_http_request", fake_http_request)

    def paginator(u: str, p: dict | None, payload: Any):
        nxt = payload.get("next") if isinstance(payload, dict) else None
        if not nxt:
            return None
        return {"next_request": {"url": nxt, "headers": {"X-Token": "abc"}}}

    pages = http.get("https://api.example.com/users?page=1", paginator=paginator)
    assert isinstance(pages, list)
    assert len(pages) == 2
    assert calls[0]["headers"] == {}
    assert calls[1]["headers"] == {"X-Token": "abc"}
