# API Demo Project

The `examples/api_demo` scenario demonstrates how FastFlowTransform blends local data, external APIs, and multiple execution engines. It highlights:

- **Hybrid data model**: joins a local seed (`crm.users`) with live user data from JSONPlaceholder.
- **Multiple environments**: switch between DuckDB, Postgres, and Databricks Spark using `profiles.yml` + `.env.*`.
- **HTTP integration**: compare the built-in FastFlowTransform HTTP client (`api_users_http`) with a plain `requests` implementation (`api_users_requests`).
- **Offline caching & telemetry**: inspect HTTP snapshots via `run_results.json`.

## Data Model

1. **Seed staging** – `models/common/users.ff.sql`
   ```sql
   {{ config(
       materialized='table',
       tags=[
           'example:api_demo',
           'scope:common',
           'kind:seed-consumer',
           'engine:duckdb',
           'engine:postgres',
           'engine:databricks_spark'
       ]
   ) }}
   select id, email
   from {{ source('crm', 'users') }};
   ```
   Consumes `sources.yml → crm.users` (seeded from `seeds/seed_users.csv`).

2. **API enrichment** – two Python implementations under `models/engines/duckdb/`:
   - `api_users_http.ff.py` uses the built-in HTTP wrapper (`fastflowtransform.api.http.get_df`) with cache/offline support.
   - `api_users_requests.ff.py` uses raw `requests` for maximum flexibility.

3. **Mart join** – `models/common/mart_users_join.ff.sql`
   ```sql
  {% set api_users_model = var('api_users_model', 'api_users_http') %}
   with a as (
     select u.id as user_id, u.email from {{ ref('users.ff') }} u
   ),
   b as (
     select * from {{ ref(api_users_model) }}
   )
   select ...
   ```
   Ties everything together and exposes the `var('api_users_model')` hook to choose the HTTP implementation.

## Profiles & Secrets

`profiles.yml` defines per-engine profiles that reference environment variables:

```yaml
dev_duckdb:
  engine: duckdb
  duckdb:
    path: "{{ env('FF_DUCKDB_PATH', '.local/api_demo.duckdb') }}"

dev_postgres:
  engine: postgres
  postgres:
    dsn: "{{ env('FF_PG_DSN') }}"
    db_schema: "{{ env('FF_PG_SCHEMA', 'public') }}"
```

`.env.dev_*` files supply the actual values. `_load_dotenv_layered()` loads them in priority order: repo `.env` → project `.env` → `.env.<env>` → shell overrides (highest priority). Secrets stay out of version control.


## Makefile Workflow

`Makefile` chooses the profile via `ENGINE` (`duckdb`/`postgres`/`databricks_spark`) and wraps the main commands:

```make
ENGINE ?= duckdb

ifeq ($(ENGINE),duckdb)
  PROFILE_ENV = dev_duckdb
endif
...

seed:
	uv run fft seed "$(PROJECT)" --env $(PROFILE_ENV)
run:
	env FFT_ACTIVE_ENV=$(PROFILE_ENV) ... uv run fft run ...
```

Common targets:

| Target                   | Description |
|--------------------------|-------------|
| `make ENGINE=duckdb seed`| Materialize seeds into DuckDB. |
| `make ENGINE=postgres run`| Execute the full pipeline against Postgres. |
| `make dag`               | Render documentation (`site/dag/`). |
| `make api-run`           | Run only API models (uses HTTP cache). |
| `make api-offline`       | Force offline mode (`FF_HTTP_OFFLINE=1`). |
| `make api-show-http`     | Display HTTP snapshot metrics via `jq`. |

HTTP tuning parameters (`FF_HTTP_ALLOWED_DOMAINS`, cache dir, timeouts) live in `.env` and are appended via `HTTP_ENV` when running commands.

## End-to-End Demo

1. **Select engine**: `make ENGINE=duckdb` (default). Set `ENGINE=postgres` or `ENGINE=databricks_spark` to switch.
2. **Seed data**: `make seed`
3. **Run pipeline**: `make run`
4. **Explore docs**: `make dag` → open `examples/api_demo/site/dag/index.html`
5. **Inspect HTTP usage**: `make api-show-http`

This example demonstrates multi-engine configuration, environment-driven secrets, and API enrichment within FastFlowTransform.
