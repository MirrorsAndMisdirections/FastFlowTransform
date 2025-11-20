# Snapshots

Snapshots are **history-aware tables** that track how a row changes over time.

Unlike regular `table` / `view` / `incremental` models, which only ever expose the *current* state, a snapshot keeps **multiple versions** of each business key, with validity ranges and a “current” flag.

FastFlowTransform implements snapshots as a dedicated materialization:

```sql
{{ config(
    materialized='snapshot',
    snapshot={
        'strategy': 'timestamp',    -- or 'check'
    },
    unique_key='id',
    updated_at='updated_at',
) }}

select
    id,
    ...
from {{ ref('some_model.ff') }};
````

You run snapshot models via a **separate CLI entrypoint**:

```bash
fft snapshot run . --env dev_duckdb
```

Regular `fft run` does *not* execute snapshot models.

---

## When to use snapshots

Use snapshots when you need to:

* Answer **“what did we know back then?”** questions
  e.g. “What was the user’s email on 2024-03-01?”
* Implement **type-2 slowly changing dimensions (SCD2)** for dimensions like users, customers, products, or feature flags.
* Preserve a **temporal audit trail** of important entities without hand-rolling history tables and merge logic.

You typically place snapshot models near your **cleaned dimensions**, e.g.:

* `staging/users_clean.ff.sql`
* `snapshots/users_clean_snapshot.ff.sql`  ⟵ snapshot over the staging model
* `marts/dim_users.ff.sql`  ⟵ reads from the snapshot’s “current” rows

---

## Conceptual model

A snapshot is defined by:

1. **Business key**:
   `unique_key` / `primary_key`

   > “Which column(s) identify a logical entity?”

2. **Change detection strategy** (required for snapshots):

   * `strategy='timestamp'`
     Use a **monotonic timestamp column** to detect new versions, e.g. `updated_at`, `signup_date`.
   * `strategy='check'`
     Compare a set of **“interesting” columns** (`check_cols`) between runs and open a new version when any of them changes.

3. **Source query**:
   A normal `SELECT` that produces the *current* state of your entities.

On disk, each snapshot table contains:

* All columns produced by your `SELECT`
  (e.g. `user_id`, `email`, `email_domain`, `signup_date`)
* Plus a set of **snapshot metadata columns**, typically:

  * ` _ff_valid_from` – when this version became active
  * ` _ff_valid_to` – when this version stopped being active (`NULL` for open/current)
  * ` _ff_is_current` – boolean flag marking the current row for each key

Exact column names may vary per implementation, but the pattern is always:

> “Multiple rows per business key, each with a validity range, and exactly one current row.”

---

## Snapshot configuration

Snapshot behavior is configured via `config(...)` at the top of a model.

### Minimal timestamp snapshot

```sql
{{ config(
    materialized='snapshot',
    snapshot={
        'strategy': 'timestamp',
    },
    unique_key='user_id',
    updated_at='signup_date',
) }}

select
    user_id,
    email,
    email_domain,
    signup_date
from {{ ref('users_clean.ff') }};
```

Key pieces:

* `materialized='snapshot'`
  Enables snapshot semantics for this model.

* `snapshot.strategy='timestamp'`
  Use a timestamp column to detect new versions.

* `unique_key='user_id'`
  Business key; you can also pass a list: `['user_id', 'country']`.

* `updated_at='signup_date'`
  Column used as the **freshness indicator**. When a new run sees a `signup_date` that is greater than the existing version’s, a new version is opened.

> **Validation rules**
>
> * Snapshots require a `unique_key` (or `primary_key`).
> * `strategy` must be `'timestamp'` or `'check'`.
> * For `'timestamp'`, you must provide `updated_at` / `updated_at_column`.
> * For `'check'`, you must provide `check_cols`.

### Check strategy with `check_cols`

Use this when you **don’t have** a reliable `updated_at` column and instead want to compare a list of columns:

```sql
{{ config(
    materialized='snapshot',
    snapshot={
        'strategy': 'check',
        'check_cols': ['email', 'email_domain', 'status'],
    },
    unique_key='user_id',
) }}

select
    user_id,
    email,
    email_domain,
    status,
    signup_date
from {{ ref('users_clean.ff') }};
```

Here:

* The engine joins **current source rows** with **current snapshot rows** on `unique_key`.
* It recomputes a hash over `check_cols`. When the hash changes, a new version is opened.

This is convenient for:

* Entities with **many changing attributes**.
* Sources where `updated_at` is unreliable or missing.

### Shorthands and normalization

FastFlowTransform’s config layer normalizes snapshot config so you can:

* Pass a single string or list for `unique_key`, `check_cols`, `updated_at_columns`, etc.
* Use `updated_at` or `updated_at_column` interchangeably (they are validated to be consistent).
* Optionally keep snapshot settings nested under `snapshot={...}` while still accessing the top-level shortcuts (`unique_key`, `updated_at`, `check_cols`) in executors.

---

## Runtime behavior

### First snapshot run

On the **first** `fft snapshot run`:

* FFT executes the snapshot’s `SELECT`.
* For each row, it writes:

  * One row per `unique_key`.
  * `valid_from = run_timestamp`
  * `valid_to   = NULL`
  * `is_current = TRUE`

No comparison with previous data (there is none yet).

### Subsequent runs (timestamp strategy)

On each subsequent run (`strategy='timestamp'`):

1. **Load current version** per `unique_key` from the snapshot table.
2. **Load current source rows** from the snapshot model’s `SELECT`.
3. For each key:

   * If the key **did not exist** before → **insert** new open-ended version.
   * If the key existed, and the source row’s `updated_at` is **greater** than the snapshot’s latest version:

     * Compare row values (implementation detail; often just “trust” updated_at).
     * If considered changed:

       * **Close** the current version: set `valid_to = run_timestamp`, `is_current = FALSE`.
       * **Open** a new version with `valid_from = run_timestamp`, `valid_to = NULL`, `is_current = TRUE`.
   * If the key existed, and `updated_at` is **not greater** (or row unchanged) → no-op.

> **Deletes**
> By design, snapshots focus on changes in the **source-of-truth rows**. When a row disappears from the source, it is treated as **no change** for snapshot purposes (the last known version remains current). If you need delete tracking, model a soft-delete flag and include it in `check_cols`.

### Subsequent runs (check strategy)

For `strategy='check'`:

1. **Load current version** per `unique_key`.
2. **Load current source rows**.
3. Compute a **hash** (or equivalent) over the configured `check_cols` for both.
4. If the hash differs → treat it as a change:

   * Close old version (`valid_to = run_timestamp`, `is_current = FALSE`).
   * Insert new version (`valid_from = run_timestamp`, `valid_to = NULL`, `is_current = TRUE`).

This strategy is usually more robust when:

* Your source doesn’t maintain an updated timestamp.
* You care about a specific subset of columns only.

---

## Snapshot table schema

A snapshot table contains:

* **Business columns**: whatever your `SELECT` produces.
* **Snapshot columns** (typical pattern):

  ```text
  _ff_valid_from   TIMESTAMP  -- when this version became active
  _ff_valid_to     TIMESTAMP  -- when this version ended (NULL = still active)
  _ff_is_current   BOOLEAN    -- TRUE exactly for the current version
  ```

Common query patterns:

### Current version per key

```sql
select *
from users_clean_snapshot
where _ff_is_current = true;
```

### History of a single key

```sql
select *
from users_clean_snapshot
where user_id = 42
order by _ff_valid_from;
```

### Point-in-time view

“What did we know on 2024-03-01?”

```sql
select *
from users_clean_snapshot
where
  _ff_valid_from <= timestamp '2024-03-01'
  and ( _ff_valid_to is null or _ff_valid_to > timestamp '2024-03-01' );
```

These patterns work uniformly across engines.

---

## CLI: `fft snapshot run`

Snapshots are run via a dedicated CLI subcommand:

```bash
fft snapshot run <project> [options]
```

Key properties:

* Only models with `materialized='snapshot'` are eligible.
* If your selector (`--select/--exclude`) matches non-snapshot models, they are ignored or explicitly rejected with a clear error.
* You can combine all the usual selection patterns: `tag:...`, `path:...`, `name:...`, etc.

Example:

```bash
# Run only snapshot models that belong to the snapshot demo and the DuckDB engine
fft snapshot run . \
  --env dev_duckdb \
  --select tag:example:snapshot_demo \
  --select tag:engine:duckdb
```

### Retention & pruning

To avoid unbounded growth, the snapshot CLI supports **retention** flags:

* `--prune`
  Enable pruning of old versions.
* `--keep-last N`
  Keep only the last `N` versions per `unique_key`.
* `--dry-run`
  Show what *would* be pruned without actually deleting anything.

Examples:

```bash
# Show which rows would be removed, but do not delete
fft snapshot run . \
  --env dev_duckdb \
  --select tag:example:snapshot_demo \
  --prune --keep-last 3 --dry-run

# Apply pruning for real
fft snapshot run . \
  --env dev_duckdb \
  --select tag:example:snapshot_demo \
  --prune --keep-last 3
```

Retention is applied **after** the snapshot update, so the most recent `N` versions are always preserved.

---

## Interaction with regular runs

Snapshots are intentionally **decoupled** from `fft run`:

* `fft run` builds your **current-state pipeline** (seeds, staging, marts, incremental models, etc.).
* `fft snapshot run` builds and updates **history tables** based on the current state produced by `fft run`.

Typical workflow:

```bash
# 1) Rebuild main pipeline
fft run . --env dev_duckdb

# 2) Update snapshots based on the new state
fft snapshot run . --env dev_duckdb
```

If you accidentally try to run a snapshot via `fft run`, FFT will raise an error such as:

> Snapshot models cannot be executed via 'fft run'. Use 'fft snapshot run' instead.

This separation keeps regular DAG runs **predictable and stateless**, while giving you a powerful, focused tool for history tracking.

---

## Best practices

* Put snapshots in a dedicated folder, e.g. `models/snapshots/`, and tag them:

  ```sql
  tags=['scope:snapshot', 'engine:duckdb', 'example:snapshot_demo']
  ```

* Always configure a **stable, business-level `unique_key`**. Avoid transient IDs that might be re-used.

* Prefer `strategy='timestamp'` when you have a trustworthy monotonic timestamp.

* Prefer `strategy='check'` when:

  * timestamps are unreliable, or
  * you care about specific columns only.

* Build point-in-time marts by **reading from the snapshot** rather than the raw staging table when you need historical correctness.

Snapshots give you a clean, structured way to get SCD2-style history without hand-writing merge logic for every table.
