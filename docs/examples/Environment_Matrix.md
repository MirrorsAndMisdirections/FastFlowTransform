# Environment Matrix (DuckDB-only) — Example

This tiny project demonstrates **per-environment configuration** (dev / stg / prod) while keeping everything on **DuckDB**.
Each environment uses its **own DuckDB file**, so you can switch environments without changing code.

It also includes a **seed step** (CSV → table) and two minimal models:

* `env_vars.ff` (Python) — echoes which env is active and which DuckDB file is used
* `hello.ff` (SQL view) — shows how `{{ this.* }}` resolves from the active profile
* `users.ff` (SQL table) — reads from the seeded CSV table to prove seeding works

---

## What this shows

* Layered environment files: `.env.dev`, `.env.stg`, `.env.prod` (+ optional `*.local` overrides)
* `profiles.yml` that reads from `env('…')` so connection details live in env files
* All environments use **DuckDB**, but **different DB files** (e.g. `.local/dev.duckdb`, `.local/stg.duckdb`, …)
* Seeding CSV → `seed_users` table, then a simple model consuming it

---

## Project layout

```
examples/env_matrix/
├─ models/
│  ├─ env_vars.ff.py          # Python model: shows env + DuckDB file info
│  └─ users.ff.sql            # SQL table: reads from seeded 'seed_users'
├─ seeds/
│  └─ users.csv               # sample data for seeding (-> seed_users)
├─ profiles.yml               # all envs = DuckDB, different paths
├─ .env                       # shared defaults (optional)
├─ .env.dev                   # dev environment vars
├─ .env.stg                   # stg environment vars
├─ .env.prod                  # prod environment vars
├─ .env.dev.local             # private overrides (gitignored; optional)
├─ .env.stg.local             # private overrides (gitignored; optional)
├─ .env.prod.local            # private overrides (gitignored; optional)
└─ Makefile                   # convenience targets (run, seed, dag)
```

---

## Environment files

Each env file sets a different DuckDB path:

* `.env.dev`

  ```
  FFT_ACTIVE_ENV=dev
  FF_ENGINE=duckdb
  FF_DUCKDB_PATH=.local/env_matrix.dev.duckdb
  ```

* `.env.stg`

  ```
  FFT_ACTIVE_ENV=stg
  FF_ENGINE=duckdb
  FF_DUCKDB_PATH=.local/env_matrix.stg.duckdb
  ```

* `.env.prod`

  ```
  FFT_ACTIVE_ENV=prod
  FF_ENGINE=duckdb
  FF_DUCKDB_PATH=.local/env_matrix.prod.duckdb
  ```

> You can place secrets or machine-local tweaks in `.env.<env>.local` (ignored by git).
> Optional toggles (if you want verbose SQL logs):
> `FFT_SQL_DEBUG=1`, `FFT_LOG_JSON=1`

---

## `profiles.yml` (DuckDB for all envs)

```yaml
default:
  dev:
    engine: "{{ env('FF_ENGINE', 'duckdb') }}"
    duckdb:
      path: "{{ env('FF_DUCKDB_PATH', ':memory:') }}"

  stg:
    engine: "{{ env('FF_ENGINE', 'duckdb') }}"
    duckdb:
      path: "{{ env('FF_DUCKDB_PATH', ':memory:') }}"

  prod:
    engine: "{{ env('FF_ENGINE', 'duckdb') }}"
    duckdb:
      path: "{{ env('FF_DUCKDB_PATH', ':memory:') }}"
```

---

## Models

### `models/env_vars.ff.py` (Python)

Returns one row with:

* `active_env_hint` (from `.env.*`),
* `ff_engine` (should be `duckdb` here),
* `duckdb_path`, `duckdb_exists`, `duckdb_size_bytes`.

### `models/hello.ff.sql` (SQL view)

Uses `{{ this.materialized }}`, `{{ this.schema }}`, `{{ this.database }}` so you can see what the active profile provides. (The simple `SELECT` is compatible with DuckDB; if you added casts like `::text`, they’re fine in DuckDB too.)

### `models/users.ff.sql` (SQL table)

Reads from the seeded table `seed_users`:

```sql
{{ config(materialized='table', tags=['demo', 'seed']) }}

select
  id,
  email
from "seed_users";
```

> If you see an error “table seed_users does not exist”, you **haven’t run `fft seed`** for that environment yet.

---

## Seeds

`seeds/users.csv` is loaded by `fft seed` into a table named `seed_users`.
(That’s the default naming convention: `users.csv` → `seed_users`.)

---

## Running it

From the repo root:

### Using `uv` directly

**Dev**

```bash
uv run fft seed examples/env_matrix --env dev
uv run fft run  examples/env_matrix --env dev
uv run fft dag  examples/env_matrix --env dev --html
```

**Staging**

```bash
uv run fft seed examples/env_matrix --env stg
uv run fft run  examples/env_matrix --env stg
```

**Prod**

```bash
uv run fft seed examples/env_matrix --env prod
uv run fft run  examples/env_matrix --env prod
```

### Using the Makefile (inside `examples/env_matrix/`)

```bash
make run-dev     # runs the DAG on dev
make run-stg
make run-prod

make seed-dev    # seed only (dev)
make seed-stg
make seed-prod

make dag-dev     # generate HTML DAG for dev
make clean       # remove .local/, docs/, site/, .fastflowtransform/
```

> Tip: re-run `fft seed` whenever you switch environments or change `seeds/*.csv`.

---

## Inspecting results

* The **HTML DAG** (after `make dag-dev`) will be at:

  ```
  examples/env_matrix/site/dag/index.html
  ```
* The **artifacts** are under:

  ```
  examples/env_matrix/.fastflowtransform/target/{manifest.json, run_results.json, catalog.json}
  ```
* Query the DuckDB files directly with `duckdb` CLI or `python` + `duckdb` module if you want to peek inside.

---

## Troubleshooting

* **`seed_users` not found**
  Run `fft seed` for the same environment:
  `uv run fft seed examples/env_matrix --env dev`

* **No logs showing**
  Use `-v`/`-vv` and/or `--sql-debug` on the CLI, or set:

  ```
  FFT_SQL_DEBUG=1
  FFT_LOG_JSON=1  # optional JSON logs
  ```

* **Wrong environment picked**
  Double-check the `--env` flag in your CLI call and ensure the `.env.<env>` file exists.

---

## Clean up

```bash
make clean              # from examples/env_matrix/
# or manually:
rm -rf examples/env_matrix/.local examples/env_matrix/site examples/env_matrix/docs
rm -rf examples/env_matrix/.fastflowtransform
```
