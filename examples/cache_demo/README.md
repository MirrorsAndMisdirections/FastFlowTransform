# Cache Demo

This demo shows:
- Build cache skip/hit via fingerprints
- Downstream invalidation (seed → staging → mart)
- Environment-driven invalidation (only `FF_*`)
- Parallelism within levels (`--jobs`)
- HTTP response cache + offline mode

## Quickstart

```bash
cd examples/cache_demo
make cache_first      # builds and writes cache
make cache_second     # should SKIP everything
make change_sql       # touch SQL → mart rebuilds
make change_seed      # add a seed row → staging + mart rebuild
make change_env       # FF_* env change → full rebuild
make change_py        # edit constant in py_constants.ff.py → it rebuilds

make http_first       # warms HTTP cache
make http_offline     # reuses HTTP cache without network
make http_cache_clear # clears HTTP response cache
Inspect:

site/dag/index.html

.fastflowtransform/target/run_results.json (HTTP stats, results)

markdown
Code kopieren

---

## What this demo proves (in a minute)

- **Cache hit/skip:** `make cache_second` should skip everything (if nothing changed).
- **Upstream invalidation:** `make change_seed` rebuilds staging **and** the mart.
- **Env invalidation:** `make change_env` (because `FF_*` is part of the fingerprint).
- **Python source sensitivity:** `py_constants` rebuilds only when its code changes.
- **HTTP cache:** `http_first` fetches; `http_offline` runs fully offline using cached responses.
