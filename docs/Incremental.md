# Incremental Models (R1)

This guide explains how to configure incremental models, use `is_incremental()` in SQL, engine compatibility, and schema change policies.

## Quick Start

A minimal incremental model:

```sql
-- examples/r1_demo/models/fct_events_inc.ff.sql
{{ config(
  materialized='incremental',
  unique_key=['event_id'],
  on_schema_change='append_new_columns'  -- or 'sync_all_columns'
) }}
with src as (
  select * from {{ source('app', 'events') }}
  {% if is_incremental() %}
    where ingested_at > (select coalesce(max(ingested_at), timestamp '1970-01-01') from {{ this.name }})
  {% endif %}
)
select
  event_id,
  user_id,
  event_type,
  ingested_at,
  -- evolving column: will appear later
  meta_json
from src;
````

### `is_incremental()`

* Available in SQL templates during rendering.
* Returns `true` when the model exists and the current `materialized='incremental'` run chooses an incremental path (insert/merge) instead of full rebuild.
* Typical usage: filter the source to “new” rows only.

### Engine Matrix (MVP)

| Engine             | Incremental Insert | Merge/Upsert | Schema Change Policy |
| ------------------ | ------------------ | ------------ | -------------------- |
| DuckDB             | ✅ insert           | 🚧 fallback* | ✅ append new cols    |
| Postgres           | ✅ insert           | 🚧 fallback* | ✅ append new cols    |
| BigQuery (classic) | ✅ insert           | 🚧 fallback* | 🚧 best-effort       |
| BigQuery BigFrames | ✅ insert           | 🚧 fallback* | 🚧 best-effort       |
| Databricks Spark   | ✅ insert           | 🚧 fallback* | 🚧 best-effort       |
| Snowflake Snowpark | ✅ insert           | 🚧 fallback* | 🚧 best-effort       |

* Fallback strategy merges by delete-on-keys + insert (best effort) if native merge isn’t wired.

### Schema Change Policies

* `append_new_columns` (default): new columns appear in target if they show up in the select.
* `sync_all_columns` (planned): attempt to keep type/nullable alignment. Currently not enforced; prefer append in R1.

### End-to-End

```bash
# Seeds → initial incremental build → run again with filter
fft seed examples/r1_demo --env dev
fft run  examples/r1_demo --env dev --select fct_events_inc.ff
# simulate new data (re-seed or append), then:
fft run  examples/r1_demo --env dev --select fct_events_inc.ff
```

**Artifacts:** see `.fastflowtransform/target/{manifest.json, run_results.json, catalog.json}`.
