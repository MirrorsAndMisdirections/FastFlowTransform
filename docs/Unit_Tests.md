# Model Unit Tests (`fft utest`)

`fft utest` executes a single model in isolation, loading only the inputs you provide and comparing the result to an expected dataset. It works for SQL and Python models and runs against DuckDB or Postgres by default.

## Cache Modes

`fft utest --cache {off|ro|rw}` (default: `off`)

- `off`: deterministic, never skips.
- `ro`: skip on cache hit; on miss, build but **do not write** cache.
- `rw`: skip on hit; on miss, build **and write** fingerprint.

Notes:

- UTests key the cache with `profile="utest"`.
- Fingerprints include case inputs (CSV content hash / inline rows), so changing inputs invalidates the cache.
- `--reuse-meta` is currently a reserved flag: exposed in the CLI, acts as a no-op today, and will enable future meta-table optimizations.

## Why Use UTests?

- Fast feedback on transformation logic without full DAG runs.
- Small, reproducible fixtures (rows inline or external CSV).
- Engine-agnostic: swap DuckDB/Postgres to spot dialect differences.

## Folder Layout

Specs live under `<project>/tests/unit/*.yml` relative to the project root (the directory passed to the CLI that contains `models/`):

```
your-project/
├── models/
│   ├── users.ff.sql
│   ├── users_enriched.ff.py
│   └── mart_users.ff.sql
└── tests/
    └── unit/
        ├── users_enriched.yml
        └── mart_users.yml
```

## YAML DSL (with `defaults`)

Each file targets one logical node (the DAG name). Defaults are deep-merged into every case so you can share inputs/expectations and override per scenario.

```yaml
# tests/unit/users_enriched.yml
model: users_enriched

defaults:
  inputs:
    users:
      rows:
        - {id: 1, email: "a@example.com"}
        - {id: 2, email: "b@gmail.com"}
  expect:
    relation: users_enriched
    order_by: [id]

cases:
  - name: basic_gmail_flag
    expect:
      rows:
        - {id: 1, email: "a@example.com", is_gmail: false}
        - {id: 2, email: "b@gmail.com",   is_gmail: true}

  - name: override_inputs
    inputs:
      users:
        rows:
          - {id: 3, email: "c@hotmail.com"}
          - {id: 4, email: "d@gmail.com"}
    expect:
      rows:
        - {id: 3, email: "c@hotmail.com", is_gmail: false}
        - {id: 4, email: "d@gmail.com",   is_gmail: true}
```

SQL models use the file stem (including `.ff`) as `model`. Provide expected relation names that match the materialized table/view:

```yaml
# tests/unit/mart_users.yml
model: mart_users.ff

defaults:
  inputs:
    users_enriched:
      rows:
        - {id: 1, email: "a@example.com", is_gmail: false}
        - {id: 2, email: "b@gmail.com",   is_gmail: true}
  expect:
    relation: mart_users
    order_by: [id]

cases:
  - name: passthrough_columns
    expect:
      rows:
        - {id: 1, email: "a@example.com", is_gmail: false}
        - {id: 2, email: "b@gmail.com",   is_gmail: true}
```

For multi-dependency models, include every physical relation name (what `relation_for(dep)` returns):

```yaml
model: mart_orders_enriched
defaults:
  inputs:
    users_enriched:
      rows:
        - {id: 1, email: "x@gmail.com", is_gmail: true}
    orders:
      rows:
        - {order_id: 10, user_id: 1, amount: 19.9}
        - {order_id: 11, user_id: 1, amount: -1.0}
cases:
  - name: join_and_flag
    expect:
      any_order: true
      rows:
        - {order_id: 10, user_id: 1, email: "x@gmail.com", is_gmail: true, amount: 19.9, valid_amt: true}
        - {order_id: 11, user_id: 1, email: "x@gmail.com", is_gmail: true, amount: -1.0, valid_amt: false}
```

## Input Formats

- `rows`: inline dictionaries per row.
- `csv`: reference a CSV file (relative paths allowed).

Keys under `inputs` are physical relations; use `relation_for('users.ff')` if unsure.

## Expected Output & Comparison

- `relation`: actual table/view name produced by the model (defaults to `relation_for(model)`).
- Ordering: `order_by: [...]` or `any_order: true`.
- Columns: `ignore_columns: [...]`, `subset: true`.
- Numeric tolerance: `approx: true` or `approx: { col: 1e-9, other_col: 0.01 }`
  (numbers can be plain `1e-9` or quoted; they are cast to float).

## Running UTests

```bash
fft utest .                      # discover all specs
fft utest . --env dev            # use a specific profile
fft utest . --model users_enriched
fft utest . --model mart_orders_enriched --case join_and_flag
fft utest . --path tests/unit/users_enriched.yml
```

Override the executor for all specs (ensure credentials/DSNs are set):

```bash
export FF_PG_DSN="postgresql+psycopg://postgres:postgres@localhost:5432/ffdb"
export FF_PG_SCHEMA="public"
fft utest . --engine postgres
```

Executor precedence (highest → lowest): CLI `--engine`, YAML `engine:` (optional), `profiles.yml`, environment overrides.

## Design Notes

- Only the target model runs; supply all upstream relations the model expects.
- `defaults` deep-merge: dicts merge, lists/scalars overwrite.
- Results compare as DataFrames with configurable order, subsets, ignored columns, and numeric tolerances.
- Exit codes: `0` for success, `2` when at least one case fails (compact CSV-style diff is printed).

## CI Example

```yaml
name: utests
on: [push, pull_request]
jobs:
  duckdb:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -e .
      - run: fft utest . --env dev
```

For Postgres, add a service container and run `fft utest . --engine postgres` with `FF_PG_DSN` / `FF_PG_SCHEMA`.
