from __future__ import annotations

import hashlib
import json
import os
import random
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

import httpx as _HTTP
import pandas as pd

from fastflowtransform.api import context as _ctx
from fastflowtransform.api.rate_limit import init_rate_limiter

# ---- Config via ENV (optional; sensible defaults) -----------------------
_DEF_CACHE_DIR = os.getenv("FF_HTTP_CACHE_DIR", ".fastflowtransform/http_cache")
_DEF_TTL = int(os.getenv("FF_HTTP_TTL", "0") or 0)  # seconds; 0 = no default TTL
_DEF_TIMEOUT = float(os.getenv("FF_HTTP_TIMEOUT", "20"))
_DEF_MAX_RETRIES = int(os.getenv("FF_HTTP_MAX_RETRIES", "3"))
_DEF_RATE_RPS = float(os.getenv("FF_HTTP_RATE_LIMIT_RPS", "0") or 0)  # 0 = unlimited
_OFFLINE = os.getenv("FF_HTTP_OFFLINE", "0").lower() in {"1", "true", "yes", "on"}
_CACHE_MODE = (os.getenv("FF_HTTP_CACHE_MODE", "rw") or "rw").lower()  # "off" | "ro" | "rw"
_CAP = max(_DEF_RATE_RPS, 1.0)
_ALLOWED = {
    d.strip().lower()
    for d in (os.getenv("FF_HTTP_ALLOWED_DOMAINS", "") or "").split(",")
    if d.strip()
}


# ---- rate limiter (process-wide token bucket) ----------------------
init_rate_limiter(capacity=_CAP, rps=_DEF_RATE_RPS)


# ---- helpers ------------------------------------------------------------
_MASK_KEYS = {"authorization", "x-api-key", "apikey", "api_key", "token", "secret", "password"}


def _mask_headers(h: dict | None) -> dict:
    out: dict[str, Any] = {}
    for k, v in (h or {}).items():
        if str(k).lower() in _MASK_KEYS:
            out[k] = "***"
        else:
            out[k] = v
    return out


def _domain_ok(url: str) -> bool:
    if not _ALLOWED:
        return True
    try:
        netloc = urlparse(url).netloc.split("@")[-1].split(":")[0].lower()
        return any(netloc == d or netloc.endswith("." + d) for d in _ALLOWED)
    except Exception:
        return True


@dataclass
class _CacheEntry:
    meta_path: Path
    body_path: Path
    key: str


def _cache_key(method: str, url: str, params: dict | None, headers: dict | None) -> str:
    norm = {
        "method": method.upper(),
        "url": url,
        "params": params or {},
        "headers": _mask_headers(headers or {}),
    }
    blob = json.dumps(norm, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _cache_entry(key: str) -> _CacheEntry:
    base = Path(_DEF_CACHE_DIR)
    base.mkdir(parents=True, exist_ok=True)
    return _CacheEntry(meta_path=base / f"{key}.json", body_path=base / f"{key}.bin", key=key)


def _content_hash(b: bytes) -> str:
    return "sha256:" + hashlib.sha256(b).hexdigest()


def _read_cache(key: str, ttl: int | None) -> tuple[dict | None, bytes | None, bool]:
    if _CACHE_MODE == "off":
        return None, None, False
    ce = _cache_entry(key)
    if not ce.meta_path.exists() or not ce.body_path.exists():
        return None, None, False
    try:
        meta = json.loads(ce.meta_path.read_text(encoding="utf-8"))
        body = ce.body_path.read_bytes()
    except Exception:
        return None, None, False
    # TTL check
    if ttl and ttl > 0:
        age = int(time.time()) - int(meta.get("ts", 0))
        if age > ttl:
            return None, None, False
    return meta, body, True


def _write_cache(key: str, status: int, headers: dict, body: bytes, url: str) -> dict:
    if _CACHE_MODE == "ro":
        return {
            "ts": int(time.time()),
            "status": status,
            "headers": headers,
            "content_hash": _content_hash(body),
            "url": url,
        }
    ce = _cache_entry(key)
    meta = {
        "ts": int(time.time()),
        "status": status,
        "headers": headers,
        "content_hash": _content_hash(body),
        "url": url,
    }
    ce.meta_path.write_text(
        json.dumps(meta, sort_keys=True, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    ce.body_path.write_bytes(body)
    return meta


def _http_request(
    method: str,
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: float | None = None,
) -> tuple[int, dict, bytes]:
    """Send HTTP request using httpx/requests/urllib; return (status, headers, body)."""
    with _HTTP.Client(timeout=timeout or _DEF_TIMEOUT, follow_redirects=True) as c:
        r = c.request(method.upper(), url, params=params, headers=headers)
        return int(r.status_code), dict(r.headers), bytes(r.content)


def _backoff_sleep(i: int) -> None:
    # exponential + jitter
    base = min(2**i, 30)
    time.sleep(base + random.random() * 0.3 * base)


# ---- Public API ---------------------------------------------------------
def get(
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    ttl: int | None = None,
    paginator: Callable[[str, dict | None, dict], dict | None] | None = None,
    timeout: float | None = None,
) -> bytes:
    """
    Raw GET with optional FS cache and simple pagination.
    If paginator is provided, it should return
    {"next_request": {"url": "...", "params": {...}}} or None.
    """
    if not _domain_ok(url):
        raise RuntimeError(f"HTTP domain not allowed by FF_HTTP_ALLOWED_DOMAINS: {url}")

    ttl = _DEF_TTL if ttl is None else ttl
    headers = dict(headers or {})

    def _one(method: str, url_: str, params_: dict | None) -> tuple[bytes, dict]:
        key = _cache_key(method, url_, params_, headers)
        meta, body, hit = _read_cache(key, ttl)
        if hit:
            # meta can be None -> normalize to empty dict before accessing .get
            meta_dict = meta or {}
            _ctx.record(
                key, meta_dict.get("content_hash", ""), True, len(body or b""), used_offline=True
            )
            return body or b"", meta_dict
        if _OFFLINE:
            raise RuntimeError(f"HTTP offline mode - cache miss for {url_}")

        tries = max(_DEF_MAX_RETRIES, 1)
        for i in range(tries):
            status, resp_headers, resp_body = _http_request(
                method, url_, params=params_, headers=headers, timeout=timeout
            )
            if status in (429, 500, 502, 503, 504) and i < tries - 1:
                # honor Retry-After (seconds) if present
                ra = resp_headers.get("Retry-After")
                if ra:
                    try:
                        time.sleep(float(ra))
                    except Exception:
                        _backoff_sleep(i)
                else:
                    _backoff_sleep(i)
                continue
            # write cache for any success or 304
            http_status_200 = 200
            http_status_300 = 300
            http_status_304 = 304
            if http_status_200 <= status < http_status_300 or status == http_status_304:
                meta = _write_cache(key, status, resp_headers, resp_body, url_)
                _ctx.record(
                    key, meta.get("content_hash", ""), False, len(resp_body), used_offline=False
                )
                return resp_body, meta
            raise RuntimeError(f"HTTP {status} for {url_}")
        # should not reach
        raise RuntimeError(f"HTTP error after retries for {url_}")

    body, _ = _one("GET", url, params)
    if not paginator:
        return body

    # paginate: concatenated bytes are not helpful
    # → collect JSON pages and join later in get_json/get_df
    # Here we just return the first page; get_json/get_df implement paging across JSON.
    return body


def get_json(
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    ttl: int | None = None,
    paginator: Callable[[str, dict | None, dict], dict | None] | None = None,
    timeout: float | None = None,
) -> Any:
    """GET returning parsed JSON. If paginator is provided, it follows pages via callback."""
    ttl = _DEF_TTL if ttl is None else ttl
    headers = dict(headers or {})

    def _load_one(u: str, p: dict | None) -> tuple[Any, dict]:
        raw = get(u, params=p, headers=headers, ttl=ttl, paginator=None, timeout=timeout)
        try:
            js = json.loads(raw.decode("utf-8"))
        except Exception:
            js = json.loads(raw)  # if already str
        return js, {}

    pages: list[Any] = []
    u, p = url, params
    while True:
        js, _ = _load_one(u, p)
        pages.append(js)
        if paginator is None:
            break
        nxt = paginator(u, p, js)
        if not nxt:
            break
        req = nxt.get("next_request")
        if not req:
            break
        u = req.get("url") or u
        p = req.get("params")
    return pages[0] if paginator is None else pages


MetaEntry = str | list[str]
MetaArgIn = Sequence[str | Sequence[str]] | None
MetaParamOut = str | list[MetaEntry] | None


OutputBackend = Literal["pandas", "spark", "bigframes"]


def get_df(
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    ttl: int | None = None,
    paginator: Callable[[str, dict | None, dict], dict | None] | None = None,
    json_path: list[str] | None = None,
    record_path: Sequence[str] | None = None,
    meta: MetaArgIn | None = None,
    dtype: dict[str, str] | None = None,
    timeout: float | None = None,
    normalize: bool = False,
    output: OutputBackend = "pandas",
    session: Any | None = None,
) -> Any:
    """
    GET JSON and normalize into a DataFrame using pandas.json_normalize.
    If `paginator` is provided, concatenates pages over the same normalization logic.

    Parameters
    ----------
    record_path : Sequence[str] | None
        Path to the list in the JSON to be normalized.
    meta : Sequence[str | Sequence[str]] | None
        Columns to include as metadata (top-level keys or nested paths).
    output : {"pandas","spark","bigframes"}
        Controls the returned frame type. "pandas" (default) yields a pandas DataFrame.
        "spark" materialises a pyspark.sql.DataFrame using the provided session
        (or an active/builder session).
        "bigframes" is reserved for future integration and currently raises NotImplementedError.
    session : Any | None
        Optional backend handle. For Spark, pass a SparkSession;
        otherwise the active session or a new one is used.
    """

    def _extract(obj: Any) -> Any:
        """Follow json_path (if provided) into nested JSON."""
        cur = obj
        for k in json_path or []:
            cur = cur.get(k) if isinstance(cur, dict) else None
        return cur

    def _coerce_meta(m: MetaArgIn) -> MetaParamOut:
        """
        Return a value whose static type is exactly:
            str | list[str | list[str]] | None
        """
        if m is None:
            return None
        # Build a list whose element type is (str | list[str])
        out: list[MetaEntry] = []
        for elem in m:
            if isinstance(elem, str):
                out.append(elem)  # str
            else:
                out.append(list(elem))  # Sequence[str] -> list[str]
        return out  # list[MetaEntry] == list[str | list[str]]

    def _to_df(js: Any) -> pd.DataFrame:
        base = _extract(js)
        base = base if base is not None else js
        if record_path:
            rp = list(record_path) if record_path else None
            meta_param = _coerce_meta(meta)
            df = pd.json_normalize(base, record_path=rp, meta=meta_param)
        # if it's a list of dicts
        elif isinstance(base, list):
            df = pd.json_normalize(base, sep=".") if normalize else pd.DataFrame(base)
        else:
            df = pd.json_normalize(base, sep=".") if normalize else pd.json_normalize(base)
        if dtype:
            # Use DataFrame.astype with a mapping to avoid Series.astype overload issues.
            try:
                df = df.astype(cast(Any, dict(dtype)), copy=False)
            except Exception:
                # Best-effort fallback, still via DataFrame.astype (no Series.astype)
                for col, dt in dtype.items():
                    with suppress(Exception):
                        df = df.astype({col: cast(Any, dt)}, copy=False)
        return df

    def _finalize(pdf: pd.DataFrame) -> Any:
        mode = (output or "pandas").lower()
        if mode == "pandas":
            return pdf
        if mode == "spark":
            try:
                from pyspark.sql import SparkSession  # noqa: PLC0415
            except Exception as exc:  # pragma: no cover - pyspark optional dependency
                raise RuntimeError(
                    "get_df(..., output='spark') requires pyspark to be installed."
                ) from exc
            spark = session
            if spark is None:
                spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder.getOrCreate()
            return spark.createDataFrame(pdf)
        if mode == "bigframes":
            raise NotImplementedError(
                "get_df(..., output='bigframes') is not implemented yet. "
                "Open an issue if you need this backend."
            )
        raise ValueError(
            f"Unsupported output backend '{output}' (expected pandas|spark|bigframes)."
        )

    if paginator is None:
        js = get_json(url, params=params, headers=headers, ttl=ttl, timeout=timeout)
        return _finalize(_to_df(js))

    pages = get_json(
        url, params=params, headers=headers, ttl=ttl, paginator=paginator, timeout=timeout
    )
    frames = []
    for js in pages if isinstance(pages, list) else [pages]:
        frames.append(_to_df(js))
    if not frames:
        return _finalize(pd.DataFrame())
    return _finalize(pd.concat(frames, ignore_index=True))
