# Profiles Configuration

FastFlowTransform uses `profiles.yml` to describe how each environment connects to the execution engine (DuckDB, Postgres, BigQuery, Databricks Spark, Snowflake Snowpark, …). This document covers file layout, supported features, environment overrides, and loading precedence.

## File Location

`profiles.yml` lives at the project root (same level as `models/`, `project.yml`). The CLI loads it whenever you run `fft` commands (seed/run/test/dag/utest/docgen …).

```
project/
├── models/
├── project.yml
└── profiles.yml
```

## Basic Structure

The file is parsed as YAML after optional Jinja rendering. Top-level keys represent profile “names” (e.g. `dev`, `prod`, `dev_postgres`). Each profile must include an `engine` plus engine-specific configuration.

```yaml
dev:
  engine: duckdb
  duckdb:
    path: "{{ env('FF_DUCKDB_PATH', '.local/dev.duckdb') }}"

stg:
  engine: postgres
  postgres:
    dsn: "{{ env('FF_PG_DSN') }}"
    db_schema: "{{ env('FF_PG_SCHEMA', 'public') }}"

prod:
  engine: bigquery
  bigquery:
    project: "{{ env('FF_BQ_PROJECT') }}"
    dataset: "{{ env('FF_BQ_DATASET') }}"
    location: EU

default:
  engine: duckdb
  duckdb:
    path: ":memory:"
```

### Engines and Sections

Supported engines and their expected sections:

| Engine               | Section            | Key Fields                                        |
|----------------------|--------------------|---------------------------------------------------|
| `duckdb`             | `duckdb`           | `path` (file path or `:memory:`)                  |
| `postgres`           | `postgres`         | `dsn`, `db_schema`                                |
| `bigquery`           | `bigquery`         | `project` (optional), `dataset`, `location`       |
| `databricks_spark`   | `databricks_spark` | `master`, `app_name`, optional `extra_conf`, `warehouse_dir`, `use_hive_metastore`, `database`, `table_format`, `table_options` |
| `snowflake_snowpark` | `snowflake_snowpark`| `account`, `user`, `password`, `warehouse`, `database`, `db_schema`, optional `role` |

Each profile can define its own `vars:` block (values exposed via `var('key')` inside templates).

## Environment Variables

`profiles.yml` supports Jinja expressions. The helper `env('FF_VAR', 'fallback')` reads process environment variables and substitutes the default if unset. Examples:

```yaml
dev_postgres:
  engine: postgres
  postgres:
    dsn: "{{ env('FF_PG_DSN') }}"
    db_schema: "{{ env('FF_PG_SCHEMA', 'analytics') }}"
```

These expressions are rendered *before* YAML parsing. If the environment variable is missing and no default is provided, the expression resolves to an empty string and validation will fail with a clear error message.

## Loading Order & Precedence

When running `fft` commands, `_load_dotenv_layered()` loads `.env` files in ascending precedence:

1. `<repo>/.env`
2. `<project>/.env`
3. `<project>/.env.local`
4. `<project>/.env.<env_name>`
5. `<project>/.env.<env_name>.local`

Earlier values fill defaults; later files override earlier ones *only for keys that are not already defined*. **Values set in the shell (e.g. via `FF_ENGINE=duckdb fft run …`) have highest priority**—they remain untouched, even if `.env` files define the same key.

After `.env` loading, `profiles.yml` is rendered with Jinja (using the current `os.environ`) and parsed by Pydantic. Validation ensures required fields are present for each engine and produces human-readable errors for missing DSNs, schemas, etc.

## Selecting Profiles

- **Via `--env` flag**: `fft run . --env dev_postgres`
- **Via `FFT_ACTIVE_ENV`**: set in shell or `.env` to choose the active profile name.
- **Legacy `FF_ENGINE`** (overrides `engine` field post-parse): useful for quick experiments but explicit `profiles.yml` entries are preferred.

Example Makefile snippet that switches profiles without exposing secrets:

```make
ENGINE ?= duckdb

ifeq ($(ENGINE),duckdb)
  PROFILE_ENV = dev_duckdb
endif
ifeq ($(ENGINE),postgres)
  PROFILE_ENV = dev_postgres
endif

seed:
	FFT_ACTIVE_ENV=$(PROFILE_ENV) uv run fft seed . --env $(PROFILE_ENV)
```

## Using `.env` for Secrets

Keep sensitive credentials out of VCS by storing them in `.env` files referenced above:

```
examples/api_demo/
├── .env.dev_duckdb        # FF_DUCKDB_PATH=...
├── .env.dev_postgres      # FF_PG_DSN=..., FF_PG_SCHEMA=...
├── .env.dev_databricks    # FF_SPARK_MASTER=..., FF_SPARK_APP_NAME=...
└── profiles.yml
```

These files stay out of git (via `.gitignore`), while `profiles.yml` contains only non-sensitive wiring.

## Summary of Features

- Multiple profiles in a single YAML file.
- Jinja templating with `env()` helper for dynamic values.
- `.env` layered loading with shell overrides taking precedence.
- Validation for engine-specific parameters (clear error messages).
- Profile-specific `vars` exposed to Jinja `var()` function in models.
- Works seamlessly across CLI commands: seed, run, dag, test, docgen, utest.

Keep `profiles.yml` declarative, `.env` files secret, and use CLI or Makefiles to select the active profile per run. This pattern scales from local DuckDB demos to production Postgres/BigQuery/Snowflake deployments.
