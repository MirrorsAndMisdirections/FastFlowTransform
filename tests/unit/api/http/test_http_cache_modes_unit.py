import importlib
import json
from pathlib import Path

import pytest

from fastflowtransform.api import http


def _seed_cache(http_mod, cache_dir: Path, url: str, params: dict | None, body_obj) -> None:
    key = http_mod._cache_key("GET", url, params, headers={})
    body = json.dumps(body_obj, ensure_ascii=False).encode("utf-8")
    http_mod._write_cache(key, 200, {}, body, url)


@pytest.mark.unit
@pytest.mark.http
def test_http_cache_mode_off_disables_cache(monkeypatch, tmp_path):
    """
    Mode=off: the cache is ignored; in offline mode this raises because
    the network is blocked and the cache is unavailable.
    """
    monkeypatch.setenv("FF_HTTP_OFFLINE", "1")
    monkeypatch.setenv("FF_HTTP_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("FF_HTTP_CACHE_MODE", "off")

    importlib.reload(http)

    # Populate the cache anyway; it must not be read
    url = "https://api.example.com/ping"
    params = None
    _seed_cache(http, Path(tmp_path), url, params, {"ok": True})

    with pytest.raises(RuntimeError) as e:
        http.get_json(url, params=params)
    assert "offline" in str(e.value).lower() and "cache miss" in str(e.value).lower()


@pytest.mark.unit
@pytest.mark.http
def test_http_cache_mode_ro_does_not_write(monkeypatch, tmp_path):
    """
    Mode=ro: reads are allowed, writes are forbidden.
    Patch the actual request and ensure no cache file is created.
    """
    monkeypatch.setenv("FF_HTTP_OFFLINE", "0")
    monkeypatch.setenv("FF_HTTP_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("FF_HTTP_CACHE_MODE", "ro")

    importlib.reload(http)

    # No cache present; the request is patched
    url = "https://api.example.com/echo"
    params = {"q": "x"}

    def fake_http_request(method, u, *, params=None, headers=None, timeout=None):
        body = json.dumps({"data": [{"id": 1}]}, ensure_ascii=False).encode("utf-8")
        return 200, {}, body

    # Monkeypatch the internal request helper
    monkeypatch.setattr(http, "_http_request", fake_http_request)

    # Before the call: no files present
    assert not any(Path(tmp_path).iterdir())

    out = http.get_json(url, params=params)
    assert out == {"data": [{"id": 1}]}

    # After the call: still no files (read-only mode blocks writes)
    assert not any(Path(tmp_path).iterdir())
