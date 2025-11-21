# Snapshot Demo Project

The `examples/snapshot_demo` project shows how to build **history-aware tables** with FastFlowTransform snapshots. It reuses the small users pipeline from the basic demo and adds a `users_clean_snapshot` model that captures **row-versioned history** over time.

## Why it exists

* **Show snapshot semantics** – demonstrate `materialized='snapshot'` with `strategy='timestamp'` on a simple dataset.
* **Separate runs** – illustrate why snapshots are executed via `fft snapshot run` instead of the regular `fft run`.
* **Engine parity** – keep the snapshot demo portable across DuckDB, Postgres, Databricks Spark (parquet / Delta Lake / Iceberg), and BigQuery (once engines are implemented).
* **Understand the shape of a snapshot table** – see how FFT adds validity columns on top of your source columns.

Use it as a sandbox before adding snapshots to your own marts and dimensions.

## Project layout

The snapshot demo is intentionally tiny and mirrors the basic demo structure:

| Path                                           | Purpose                                                                                             |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `seeds/seed_users.csv`                         | Sample CRM-style user data. `fft seed` materializes it as a physical `seed_users` table.            |
| `models/staging/users_clean.ff.sql`            | Same as in the basic demo: cleans emails, casts types, derives `email_domain`.                      |
| `models/marts/mart_users_by_domain.ff.sql`     | Same as in the basic demo: aggregates users per email domain.                                       |
| `models/snapshots/users_clean_snapshot.ff.sql` | **New:** snapshot model that captures slowly changing history of `users_clean.ff`.                  |
| `profiles.yml`                                 | Reused from the basic demo: defines `dev_duckdb`, `dev_postgres`, `dev_databricks_parquet`, `dev_databricks_delta`, `dev_databricks_iceberg`, `dev_bigquery`. |
| `.env.dev_*`                                   | Engine-specific environment files (`.env.dev_duckdb`, `.env.dev_postgres`, `.env.dev_databricks_parquet`, `.env.dev_databricks_delta`, `.env.dev_databricks_iceberg`). |
| `Makefile`                                     | Adds snapshot-aware targets on top of the usual `seed` / `run` / `test` / `dag`.                    |

### The snapshot model

The core of the demo is `models/snapshots/users_clean_snapshot.ff.sql`:

```sql
{{ config(
    materialized='snapshot',
    snapshot={
        'strategy': 'timestamp',   -- or 'check' (not used in this demo)
    },
    unique_key='user_id',
    updated_at='signup_date',
    tags=[
        'example:snapshot_demo',
        'scope:snapshot',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery'
    ],
) }}

select
    user_id,
    email,
    email_domain,
    signup_date
from {{ ref('users_clean.ff') }};
```

Key points:

* `materialized='snapshot'` marks this as a **snapshot model**.
* `snapshot.strategy='timestamp'` means:

  * FFT uses `updated_at='signup_date'` to detect changed rows.
  * When a row changes, the old version is **closed** and a new version is **opened**.
* `unique_key='user_id'` defines the **business key** used to match records between runs.
* The *body* is a normal `SELECT` from the cleaned staging model; FFT takes care of the history logic.

On physical storage, FFT keeps:

* All columns from the select (`user_id`, `email`, `email_domain`, `signup_date`)
* Plus engine-agnostic snapshot metadata columns (names depending on your implementation), typically:

  * a **valid-from** timestamp
  * a **valid-to** timestamp (nullable/open ended)
  * an **is_current** flag

So a given `user_id` may appear multiple times with different validity ranges.

## Running the snapshot demo

Assuming you’ve already wired `examples/snapshot_demo/Makefile` similarly to the basic demo (with `snapshot` / `snapshot_demo` targets):

1. Change into the project:

   ```bash
   cd examples/snapshot_demo
   ```

2. Choose an engine and export the environment (example: DuckDB):

   ```bash
   # DuckDB
   set -a; source .env.dev_duckdb; set +a

   # Or Postgres
   # set -a; source .env.dev_postgres; set +a

   # Or Databricks
   # Parquet: set -a; source .env.dev_databricks_parquet; set +a
   # Delta:   set -a; source .env.dev_databricks_delta; set +a
   # Iceberg: set -a; source .env.dev_databricks_iceberg; set +a
   # (optionally export FF_DBR_TABLE_FORMAT=delta|iceberg to override the table format)

   # Or BigQuery (requires GCP setup)
   # set -a; source .env.dev_bigquery_pandas; set +a
   # set -a; source .env.dev_bigquery_bigframes; set +a
   ```

3. Run the full snapshot demo for the selected engine:

   ```bash
   # One-shot: clean → seed → run (pipeline) → snapshot → dag → test
   make snapshot_demo ENGINE=duckdb
   # make snapshot_demo ENGINE=postgres
   # make snapshot_demo ENGINE=databricks_spark DBR_TABLE_FORMAT=delta
   # make snapshot_demo ENGINE=databricks_spark DBR_TABLE_FORMAT=iceberg
   # make snapshot_demo ENGINE=bigquery BQ_FRAME=bigframes
   ```

   Under the hood this will typically do:

   * `fft seed` – materialize `seed_users`
   * `fft run` – build staging/mart views/tables (excluding snapshot models)
   * `fft snapshot run` – apply snapshot logic to `users_clean_snapshot`
   * `fft dag` – generate the DAG/site
   * `fft test` – run any configured DQ tests

### Databricks table formats (parquet / Delta / Iceberg)

Just like the incremental demo, the snapshot project lets you flip Spark table formats without
editing models. Pass `DBR_TABLE_FORMAT=parquet|delta|iceberg` to `make snapshot_demo` or export
`FF_DBR_TABLE_FORMAT` when invoking `fft` directly. `dev_databricks_parquet`,
`dev_databricks_delta`, and `dev_databricks_iceberg` each point to their own managed database /
warehouse (`snapshot_demo_parquet`, `snapshot_demo_delta`, `snapshot_demo_iceberg`), so switching
formats never reuses stale Hive metadata. The Iceberg profile wires in the catalog via
`spark.sql.catalog.iceberg.*`; Delta still requires the `delta-spark` package.

Manual CLI examples:

```bash
# Parquet snapshots
FF_DBR_TABLE_FORMAT=parquet \
  FFT_ACTIVE_ENV=dev_databricks_parquet FF_ENGINE=databricks_spark \
  fft snapshot run . --select tag:example:snapshot_demo --select tag:engine:databricks_spark

# Delta Lake snapshots
FF_DBR_TABLE_FORMAT=delta \
  FFT_ACTIVE_ENV=dev_databricks_delta FF_ENGINE=databricks_spark \
  fft snapshot run . --select tag:example:snapshot_demo --select tag:engine:databricks_spark

# Iceberg snapshots
FF_DBR_TABLE_FORMAT=iceberg \
  FFT_ACTIVE_ENV=dev_databricks_iceberg FF_ENGINE=databricks_spark \
  fft snapshot run . --select tag:example:snapshot_demo --select tag:engine:databricks_spark
```

4. Or run only the snapshot step (after a normal `fft run`):

   ```bash
   # DuckDB example
   make run ENGINE=duckdb              # builds users_clean etc.
   make snapshot ENGINE=duckdb         # runs only snapshot models
   ```

   Or directly with `fft`:

   ```bash
   # Only snapshot models (tagged example:snapshot_demo)
   fft snapshot run . \
     --env dev_duckdb \
     --select tag:example:snapshot_demo --select tag:engine:duckdb
   ```

   If your selection includes non-snapshot models, FFT will ignore them for the snapshot run.

## Inspecting the snapshot table

After a couple of runs with changed data, use your engine to inspect `users_clean_snapshot`:

* **DuckDB** (from the project root):

  ```sql
  select *
  from users_clean_snapshot
  order by user_id, _ff_valid_from;  -- adjust column names to what you implement
  ```

* **Postgres / BigQuery / Databricks**: the table name is the same; the schema/database/dataset follows the profile.

Typical patterns to explore:

* **Current records only** (one row per `user_id`):

  ```sql
  select *
  from users_clean_snapshot
  where _ff_is_current = true;
  ```

* **History of a single user**:

  ```sql
  select *
  from users_clean_snapshot
  where user_id = 42
  order by _ff_valid_from;
  ```

This makes it easy to answer questions like “what did we know about this user on date X?”.

## Snapshot CLI & retention

The snapshot demo uses the dedicated entry point:

```bash
fft snapshot run . --env dev_duckdb --select tag:example:snapshot_demo
```

In addition, the CLI supports retention and pruning flags (once implemented in your code base):

* `--prune` – enables pruning of old snapshot rows.
* `--keep-last N` – when used with `--prune`, keeps only the last `N` versions per key.
* `--dry-run` – shows which rows would be pruned without actually deleting anything.

Example:

```bash
# Keep only the last 3 versions per user_id; just show the plan
fft snapshot run . \
  --env dev_duckdb \
  --select tag:example:snapshot_demo \
  --prune --keep-last 3 --dry-run

# Apply the pruning for real
fft snapshot run . \
  --env dev_duckdb \
  --select tag:example:snapshot_demo \
  --prune --keep-last 3
```

This is especially useful when snapshot tables grow large and you only care about a bounded history window for most use cases.

## Interaction with regular runs

Two important rules:

1. **Snapshot models are not part of `fft run`**
   They’re intentionally excluded to keep regular pipeline runs stateless and predictable. If a snapshot model is accidentally selected in `fft run`, FFT surfaces a clear error:

   > Snapshot models cannot be executed via 'fft run'. Use 'fft snapshot run' instead.

2. **Snapshots depend on upstream models**
   In the demo, `users_clean_snapshot` depends on `users_clean.ff`. The typical flow is:

   ```bash
   fft run . --env dev_duckdb --select tag:example:basic_demo
   fft snapshot run . --env dev_duckdb --select tag:example:snapshot_demo
   ```

   * `fft run` ensures `users_clean` is fresh.
   * `fft snapshot run` compares the new `users_clean` rows with the existing snapshot table and writes history changes.
