# API Demo Project

The `examples/api_demo` scenario demonstrates how FastFlowTransform blends local data, external APIs, and multiple execution engines. It highlights:

- **Hybrid data model**: joins a local seed (`crm.users`) with live user data from JSONPlaceholder.
- **Multiple environments**: switch between DuckDB, Postgres, Databricks Spark, BigQuery (pandas or BigFrames client), and Snowflake (Snowpark) using `profiles.yml` + `.env.*`.
- **HTTP integration**: compare the built-in FastFlowTransform HTTP client (`api_users_http`) with a plain `requests` implementation (`api_users_requests`).
- **Offline caching & telemetry**: inspect HTTP snapshots via `run_results.json`.
- **Engine-aware registration**: scope Python models via `engine_model` and SQL models via `config(engines=[...])` so only the active engine’s nodes load.

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
           'engine:databricks_spark',
           'engine:bigquery',
           'engine:snowflake_snowpark'
       ]
   ) }}
   select id, email
   from {{ source('crm', 'users') }};
   ```
   Consumes `sources.yml → crm.users` (seeded from `seeds/seed_users.csv`).

2. **API enrichment** – engine-specific Python implementations under `models/engines/<engine>/`:
   - `api_users_http.ff.py` uses the built-in HTTP wrapper (`fastflowtransform.api.http.get_df`) with cache/offline support.
   - `api_users_requests.ff.py` uses raw `requests` for maximum flexibility.
- Engine-specific callables are scoped with `engine_model(only=...)` (DuckDB/Postgres/Spark/Snowflake) or `env_match={"FF_ENGINE": "bigquery", "FF_ENGINE_VARIANT": ...}` (BigQuery pandas/BigFrames) to stay isolated per engine.

3. **Mart join** – `models/common/mart_users_join.ff.sql`
   ```sql
   {{ config(engines=['duckdb','postgres','databricks_spark','bigquery','snowflake_snowpark']) }}
   {% set api_users_model = var('api_users_model', 'api_users_http') %}
   {% set api_users_refs = {
       'api_users_http': ref('api_users_http'),
       'api_users_requests': ref('api_users_requests')
   } %}
   {% set api_users_relation = api_users_refs.get(api_users_model, api_users_refs['api_users_http']) %}
   with a as (
     select u.id as user_id, u.email from {{ ref('users.ff') }} u
   ),
   b as (
     select * from {{ api_users_relation }}
   )
   select ...
   ```
   Ties everything together and exposes the `var('api_users_model')` hook to choose the HTTP implementation while still keeping literal `ref('…')` calls in the template (required for DAG detection). `config(engines=[...])` keeps the SQL node registered only for the engines you list, preventing duplicate names across engine-specific folders.

   > **Warning:** The DAG builder only detects dependencies from literal `ref('model_name')` strings. A pure `ref(api_users_model)` (without the mapping shown above) compiles, but the graph would miss the edge to `api_users_http`/`api_users_requests`.

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

dev_bigquery_bigframes:
  engine: bigquery
  bigquery:
    project: "{{ env('FF_BQ_PROJECT') }}"
    dataset: "{{ env('FF_BQ_DATASET', 'api_demo') }}"
    location: "{{ env('FF_BQ_LOCATION', 'EU') }}"
    use_bigframes: true

dev_snowflake:
  engine: snowflake_snowpark
  snowflake_snowpark:
    account: "{{ env('FF_SF_ACCOUNT') }}"
    user: "{{ env('FF_SF_USER') }}"
    password: "{{ env('FF_SF_PASSWORD') }}"
    warehouse: "{{ env('FF_SF_WAREHOUSE', 'COMPUTE_WH') }}"
    database: "{{ env('FF_SF_DATABASE', 'API_DEMO') }}"
    schema: "{{ env('FF_SF_SCHEMA', 'API_DEMO') }}"
    role: "{{ env('FF_SF_ROLE', '') }}"
    allow_create_schema: true
```

`.env.dev_*` files supply the actual values (including `.env.dev_snowflake` for Snowflake credentials). `_load_dotenv_layered()` loads them in priority order: repo `.env` → project `.env` → `.env.<env>` → shell overrides (highest priority). Secrets stay out of version control.

### BigQuery specifics

- Set `ENGINE=bigquery` in the Makefile targets and choose a client via `BQ_FRAME=pandas` or `BQ_FRAME=bigframes` (default).
- Required env vars: `FF_BQ_PROJECT`, `FF_BQ_DATASET` (defaults to `api_demo`), and optionally `FF_BQ_LOCATION`. Uncomment `allow_create_dataset` in `profiles.yml` for first-run convenience.
- BigFrames variants ingest the HTTP payload into a pandas DataFrame, then wrap it as a BigFrames DataFrame (FFT’s `get_df(..., output="bigframes")` is not implemented yet).


## Makefile Workflow

`Makefile` chooses the profile via `ENGINE` (`duckdb`/`postgres`/`databricks_spark`/`bigquery`/`snowflake_snowpark`) and wraps the main commands. For BigQuery, set `BQ_FRAME=pandas|bigframes`:

```make
ENGINE ?= duckdb

ifeq ($(ENGINE),duckdb)
  PROFILE_ENV = dev_duckdb
endif
...
ifeq ($(ENGINE),bigquery)
  ENGINE_TAG = engine:bigquery
  ifeq ($(BQ_FRAME),pandas)
    PROFILE_ENV = dev_bigquery_pandas
  else
    PROFILE_ENV = dev_bigquery_bigframes
  endif
endif
ifeq ($(ENGINE),snowflake_snowpark)
  PROFILE_ENV = dev_snowflake
endif

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
| `make ENGINE=bigquery run BQ_FRAME=bigframes`| Run against BigQuery (default BigFrames client; set `BQ_FRAME=pandas` to switch). |
| `make ENGINE=snowflake_snowpark run`| Execute the API demo on Snowflake via Snowpark (install `fastflowtransform[snowflake]`). |
| `make dag`               | Render documentation (`site/dag/`). |
| `make api-run`           | Run only API models (uses HTTP cache). |
| `make api-offline`       | Force offline mode (`FF_HTTP_OFFLINE=1`). |
| `make api-show-http`     | Display HTTP snapshot metrics via `jq`. |

HTTP tuning parameters (`FF_HTTP_ALLOWED_DOMAINS`, cache dir, timeouts) live in `.env` and are appended via `HTTP_ENV` when running commands.

## End-to-End Demo

1. **Select engine**: `make ENGINE=duckdb` (default). Set `ENGINE=postgres`, `ENGINE=databricks_spark`, `ENGINE=bigquery BQ_FRAME=<pandas|bigframes>`, or `ENGINE=snowflake_snowpark` to switch.
2. **Seed data**: `make seed`
3. **Run pipeline**: `make run`
4. **Explore docs**: `make dag` → open `examples/api_demo/site/dag/index.html`
5. **Inspect HTTP usage**: `make api-show-http`

This example demonstrates multi-engine configuration, environment-driven secrets, and API enrichment within FastFlowTransform.
