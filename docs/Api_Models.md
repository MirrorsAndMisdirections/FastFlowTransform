# API Calls in Python Models

> **Status:** Experimental but stable for demos and smaller workflows.  
> **Goal:** Query HTTP APIs from Python models, return responses as DataFrames, cache and instrument them cleanly, and support reproducible offline runs.

* [Motivation](#motivation)
* [Quickstart](#quickstart)
* [Programming API](#programming-api)
  * [`get_json`](#get_json)
  * [`get_df`](#get_df)
  * [Pagination](#pagination)
  * [Context & Telemetry](#context--telemetry)
* [CLI Flags & Environment Variables](#cli-flags--environment-variables)
* [Example Model](#example-model)
* [Artifacts](#artifacts)
* [Tests & Offline Demos](#tests--offline-demos)
* [Best Practices](#best-practices)
* [Troubleshooting](#troubleshooting)
* [Security & Compliance](#security--compliance)
* [FAQ](#faq)

---

## Motivation

Many pipelines need small, reliable API fetchers: configuration tables, miniature dimensions, feature flags, SaaS exports. This feature provides:

- Simple HTTP calls inside Python models
- File-backed cache (reproducible builds, works offline)
- Per-node telemetry (requests, hits, bytes, hashes)
- CLI switches `--offline` and `--http-cache` for reproducible runs

---

## Quickstart

1. **Optionally enable flags** (recommended):

   ```bash
   # No network - cache hits only
   fft run . --env dev --offline
   # Cache mode
   fft run . --env dev --http-cache rw   # rw|ro|off
   ```

2. **Write a Python model**:

   ```python
   # models/users_from_api.ff.py
   import pandas as pd
   from fastflowtransform.core import model
   from fastflowtransform.api.http import get_df

   @model(name="users_from_api", deps=["users.ff"])
   def fetch(_: pd.DataFrame) -> pd.DataFrame:
       df = get_df(
           url="https://api.example.com/users",
           params={"page": 1},
           record_path=["data"],        # JSON -> list -> DataFrame
       )
       return df
   ```

3. **Run it**:

   ```bash
   fft run . --env dev --select users_from_api
   ```

---

## Programming API

> Module: `fastflowtransform.api.http`

### `get_json`

```python
from fastflowtransform.api.http import get_json

data = get_json(
    url="https://api.example.com/objects",
    params={"page": 1},        # optional
    headers={"Authorization": "Bearer ..."},  # optional
    timeout=20,                # optional (seconds)
)
# -> Python dict / list
```

**Behavior**

- Reads from the local cache (when present and valid).
- Writes to the cache (`rw` mode), including the response body.
- Respects offline mode (no network traffic).

### `get_df`

```python
from fastflowtransform.api.http import get_df

df = get_df(
    url="https://api.example.com/users",
    params={"page": 1},
    record_path=["data"],     # path to the JSON list
    normalize=True,           # optional: flatten nested objects
    paginator=None,           # optional: pagination strategy (see below)
)
# -> pandas.DataFrame
```

**Conversion**

- Default: `record_path` points to the array payload (for example `["data"]`).
- `normalize=True` delegates to `json_normalize` for deeper structures.

### Pagination

For paged APIs you can describe the next request declaratively:

```python
def paginator(url: str, params: dict | None, json_obj: dict):
    next_url = json_obj.get("next")          # e.g. absolute URL
    if next_url:
        return {"next_request": {"url": next_url}}
    return None

df = get_df(
    "https://api.example.com/users?page=1",
    paginator=paginator,
    record_path=["data"],
)
```

The paginator may return the following fields:

- `{"next_request": {"url": "...", "params": {...}, "headers": {...}}}`
  (any missing field keeps its previous value)

### Context & Telemetry

During a model run the executor collects telemetry per node and writes it into `run_results.json`:

- `requests` (count)
- `cache_hits`
- `bytes` (sum of response bodies)
- `used_offline` (bool)
- `keys` (cache keys)
- `entries` (optional compact array with URL, status, content hash)

You will find these metrics under the `http` block of each node (see [Artifacts](#artifacts)).

---

## CLI Flags & Environment Variables

**CLI**

- `--offline`  
  Sets `FF_HTTP_OFFLINE=1`; network requests are blocked, **cache hits only**.
- `--http-cache {off|ro|rw}`  
  Sets `FF_HTTP_CACHE_MODE`:

  - `off`: neither read nor write.
  - `ro`: read-only (hits), **no** writes.
  - `rw`: read and write (default).

**Environment (optional to set directly)**

| Variable                 | Default                         | Effect                              |
| ------------------------ | ------------------------------- | ----------------------------------- |
| `FF_HTTP_OFFLINE`        | `0`                             | `1/true/on` -> offline mode         |
| `FF_HTTP_CACHE_MODE`     | `rw`                            | `off` / `ro` / `rw`                 |
| `FF_HTTP_CACHE_DIR`      | `.fastflowtransform/http_cache` | Cache directory                     |
| `FF_HTTP_TTL`            | `0`                             | Seconds; 0 = never expires          |
| `FF_HTTP_TIMEOUT`        | `20`                            | Request timeout (seconds)           |
| `FF_HTTP_MAX_RETRIES`    | `3`                             | Basic retry count                   |
| `FF_HTTP_RATE_LIMIT_RPS` | `0`                             | Requests per second (0 = unlimited) |

---

## Example Model

```python
# models/dim_countries_from_api.ff.py
import pandas as pd
from fastflowtransform.core import model
from fastflowtransform.api.http import get_df

@model(name="dim_countries_from_api", deps=["users.ff"])
def countries(_: pd.DataFrame) -> pd.DataFrame:
    def pager(u, p, js):
        nxt = js.get("paging", {}).get("next")
        return {"next_request": {"url": nxt}} if nxt else None

    df = get_df(
        url="https://api.example.com/countries?page=1",
        paginator=pager,
        record_path=["data"],
        normalize=True,
    )
    # lightweight post-processing
    if "code" in df.columns:
        df["code"] = df["code"].str.upper()
    return df
```

Run:

```bash
fft run . --env dev --select dim_countries_from_api --http-cache ro
```

---

## Artifacts

`<project>/.fastflowtransform/target/run_results.json` (excerpt):

```json
{
  "results": [
    {
      "name": "dim_countries_from_api",
      "status": "success",
      "duration_ms": 153,
      "http": {
        "requests": 2,
        "cache_hits": 2,
        "bytes": 1842,
        "used_offline": true,
        "keys": ["GET:https://api.example.com/countries?page=1|{}|{}", "..."],
        "entries": [
          {"url": "https://api.example.com/countries?page=1", "status": 200, "content_hash": "sha256:..."},
          {"url": "https://api.example.com/countries?page=2", "status": 200, "content_hash": "sha256:..."}
        ]
      }
    }
  ]
}
```

> Note: When a node is **skipped** (fingerprint cache hit), no new `http` block is emitted - the model did not run.

---

## Tests & Offline Demos

- Place unit tests under `tests/api/...` and seed the cache directly (no real HTTP calls).
- Suggested scenarios:

  - **Offline hit:** set `FF_HTTP_OFFLINE=1`, seed the cache, `get_json/get_df` must succeed.
  - **Cache mode `off`:** even with cache entries, **no** reads; expect a failure in offline mode.
  - **`ro`:** allow read hits; **no** cache writes after a real or mocked request.
  - **Pagination:** stitch several pages from offline fixtures; telemetry should count requests/hits.

---

## Best Practices

- **Stable URLs and parameter order** produce identical cache keys and reproducible builds.
- **Keep `record_path` shallow**; use `normalize=True` only when necessary (performance).
- **Never cache secrets:** provide tokens via headers; the response body and metadata are cached.
- **Use `--offline` in CI** for deterministic tests with a pre-seeded cache.
- **Set TTL intentionally** when APIs change frequently.

---

## Troubleshooting

- **“offline + cache miss”**  
  Seed the cache (see tests) or disable offline mode.
- **“Schema mismatch”**  
  Harmonize columns after `get_df` (types, missing keys).
- **“Too many requests”**  
  Configure `FF_HTTP_RATE_LIMIT_RPS`; make pagination more efficient (larger `page_size`).
- **“No http block”**  
  Was the node **skipped** (fingerprint cache)? Or did the model avoid HTTP calls altogether?

---

## Security & Compliance

- **Do not commit secrets** - use environment variables or a secret manager.
- **PII/GDPR:** verify whether the API returns personal data; minimise retention.
- **Cache directory:** keep it in `.gitignore`; encrypt or isolate it if necessary.

---

## FAQ

**Q:** Can I call other libraries (for example `requests`, `httpx`) directly?  
**A:** Yes, but you lose telemetry and caching. The recommended entrypoint is `fastflowtransform.api.http`.

**Q:** How do I add custom headers (for example OAuth)?  
**A:** Pass `headers={...}`. Store sensitive values in env vars and inject them into your models.

**Q:** Does this work for POST requests?  
**A:** Release R1 focuses on GET. Please open an issue for POST/PUT support; the design can be extended.

---

**See also:**

- Technical guide: *Developer Guide – Architecture & Internals*
- Unit tests: `tests/api/test_http_*.py`
- Runtime & cache: *Parallelism & Cache (v0.3)*
