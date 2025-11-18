# Data Quality Demo Project

The **Data Quality Demo** shows how to use **all built-in FFT data quality tests** on a small, understandable model:

* Column checks:

  * `not_null`
  * `unique`
  * `accepted_values`
  * `greater_equal`
  * `non_negative_sum`
  * `row_count_between`
  * `freshness`
* Cross-table reconciliations:

  * `reconcile_equal`
  * `reconcile_ratio_within`
  * `reconcile_diff_within`
  * `reconcile_coverage`

It uses a simple **customers / orders / mart** setup so you can see exactly what each test does and how it fails when something goes wrong.

---

## What this example demonstrates

1. **Basic column checks** on staging tables
   Ensure IDs are present and unique, amounts are non-negative, and status values are valid.

2. **Freshness** on a timestamp column
   Check that the most recent order in your mart is not “too old”, using `last_order_ts`.

3. **Row count sanity checks**
   Guard against empty tables and unexpectedly large row counts.

4. **Cross-table reconciliations** between staging and mart
   Verify that sums and counts match between `orders` and the aggregated `mart_orders_agg`, and that every customer has a corresponding mart row.

5. **Tagged tests and selective execution**
   All tests are tagged (e.g. `example:dq_demo`, `reconcile`) so you can run exactly the subset you care about.

---

## Project layout (example)

```text
examples/dq_demo/
  .env
  .env.dev_duckdb
  .env.dev_postgres
  .env.dev_databricks
  .env.dev_bigquery_pandas
  .env.dev_bigquery_bigframes
  Makefile                  # optional, convenience wrapper around fft commands
  profiles.yml
  project.yml
  sources.yml

  seeds/
    customers.csv
    orders.csv

  models/
    staging/
      customers.ff.sql
      orders.ff.sql
    marts/
      mart_orders_agg.ff.sql
```

### Seeds

* `seeds/customers.csv`
  Simple customer dimension (e.g. `customer_id`, `name`, `status`).

* `seeds/orders.csv`
  Order fact data (e.g. `order_id`, `customer_id`, `amount`, `order_ts` as a string).

### Models

**1. Staging: `customers.ff.sql`**

* Materialized as a table.
* Casts IDs and other fields into proper types.
* Used as the “clean” customer dimension for downstream checks.

**2. Staging: `orders.ff.sql`**

* Materialized as a table.
* Casts fields to proper types so DQ tests work reliably:

  ```sql
  {{ config(
      materialized='table',
      tags=[
          'example:dq_demo',
          'scope:staging',
          'engine:duckdb',
          'engine:postgres',
          'engine:databricks_spark'
      ],
  ) }}

  select
    cast(order_id    as int)        as order_id,
    cast(customer_id as int)        as customer_id,
    cast(amount      as double)     as amount,
    cast(order_ts    as timestamp)  as order_ts
  from {{ source('crm', 'orders') }};
  ```

  This is important for:

  * numeric checks (`greater_equal`, `non_negative_sum`)
  * timestamp-based `freshness` checks

**3. Mart: `mart_orders_agg.ff.sql`**

Aggregates orders per customer and prepares data for reconciliation + freshness:

```sql
{{ config(
    materialized='table',
    tags=[
        'example:dq_demo',
        'scope:mart',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark'
    ],
) }}

-- Aggregate orders per customer for DQ & reconciliation tests
with base as (
  select
    o.order_id,
    o.customer_id,
    -- Ensure numeric and timestamp types for downstream DQ checks
    cast(o.amount   as double)    as amount,
    cast(o.order_ts as timestamp) as order_ts,
    c.name   as customer_name,
    c.status as customer_status
  from {{ ref('orders.ff') }} o
  join {{ ref('customers.ff') }} c
    on o.customer_id = c.customer_id
)
select
  customer_id,
  customer_name,
  customer_status as status,
  count(*)        as order_count,
  sum(amount)     as total_amount,
  min(order_ts)   as first_order_ts,
  max(order_ts)   as last_order_ts
from base
group by customer_id, customer_name, customer_status;
```

The important columns for DQ tests are:

* `status` → used for `accepted_values`
* `order_count` and `total_amount` → used for numeric and reconciliation tests
* `last_order_ts` → used for `freshness`

---

## Data quality configuration (`project.yml`)

All tests live under `project.yml → tests:`.
This example uses the tag `example:dq_demo` for easy selection.

### Column-level checks

```yaml
tests:
  # 1) IDs must be present and unique
  - type: not_null
    table: customers
    column: customer_id
    tags: [example:dq_demo, batch]

  - type: unique
    table: customers
    column: customer_id
    tags: [example:dq_demo, batch]

  # 2) Order amounts must be >= 0
  - type: greater_equal
    table: orders
    column: amount
    threshold: 0
    tags: [example:dq_demo, batch]

  # 3) Total sum of amounts must not be negative
  - type: non_negative_sum
    table: orders
    column: amount
    tags: [example:dq_demo, batch]

  # 4) Customer status values must be within a known set
  - type: accepted_values
    table: mart_orders_agg
    column: status
    values: ["active", "churned", "prospect"]
    severity: warn         # show as warning, not hard failure
    tags: [example:dq_demo, batch]

  # 5) Row count sanity check on mart
  - type: row_count_between
    table: mart_orders_agg
    min_rows: 1
    max_rows: 100000
    tags: [example:dq_demo, batch]

  # 6) Freshness: last order in the mart must not be "too old"
  - type: freshness
    table: mart_orders_agg
    column: last_order_ts
    max_delay_minutes: 100000000
    tags: [example:dq_demo, batch]
```

### Cross-table reconciliations

```yaml
  # 7) Reconcile total revenue between orders and mart
  - type: reconcile_equal
    name: total_amount_orders_vs_mart
    tags: [example:dq_demo, reconcile]
    left:
      table: orders
      expr: "sum(amount)"
    right:
      table: mart_orders_agg
      expr: "sum(total_amount)"
    abs_tolerance: 0.01

  # 8) Ratio of sums should be ~1 (within tight bounds)
  - type: reconcile_ratio_within
    name: total_amount_ratio
    tags: [example:dq_demo, reconcile]
    left:
      table: orders
      expr: "sum(amount)"
    right:
      table: mart_orders_agg
      expr: "sum(total_amount)"
    min_ratio: 0.999
    max_ratio: 1.001

  # 9) Row count diff between orders and mart should be bounded
  - type: reconcile_diff_within
    name: order_count_diff
    tags: [example:dq_demo, reconcile]
    left:
      table: orders
      expr: "count(*)"
    right:
      table: mart_orders_agg
      expr: "sum(order_count)"
    max_abs_diff: 0

  # 10) Coverage: every customer should appear in the mart
  - type: reconcile_coverage
    name: customers_covered_in_mart
    tags: [example:dq_demo, reconcile]
    source:
      table: customers
      key: "customer_id"
    target:
      table: mart_orders_agg
      key: "customer_id"
```

This set of tests touches **all available test types** and ties directly back to the simple data model.

---

## Running the demo

Assuming you are in the repo root and using DuckDB as a starting point:

### 1. Seed the data

```bash
fft seed examples/dq_demo --env dev_duckdb
```

This reads `seeds/customers.csv` and `seeds/orders.csv` and materializes them as tables referenced by `sources.yml`.

### 2. Run the models

```bash
fft run examples/dq_demo --env dev_duckdb
```

This builds:

* `customers` (staging)
* `orders` (staging)
* `mart_orders_agg` (mart)

### 3. Run all DQ tests

```bash
fft test examples/dq_demo --env dev_duckdb --select tag:example:dq_demo
```

You should see a summary like:

```text
Data Quality Summary
────────────────────
✅ not_null           customers.customer_id
✅ unique             customers.customer_id
✅ greater_equal      orders.amount
✅ non_negative_sum   orders.amount
❕ accepted_values    mart_orders_agg.status
✅ row_count_between  mart_orders_agg
✅ freshness          mart_orders_agg.last_order_ts
✅ reconcile_equal    total_amount_orders_vs_mart
✅ reconcile_ratio_within total_amount_ratio
✅ reconcile_diff_within  order_count_diff
✅ reconcile_coverage customers_covered_in_mart

Totals
──────
✓ passed: 10
! warnings: 1
```

(Exact output will differ, but you’ll see pass/failed/warned checks listed.)

### 4. Run only reconciliation tests

```bash
fft test examples/dq_demo --env dev_duckdb --select tag:reconcile
```

This executes just the cross-table checks, which is handy when you’re iterating on a mart.

---

## BigQuery variant (pandas or BigFrames)

To run the same demo on BigQuery:

1. Copy `.env.dev_bigquery_pandas` or `.env.dev_bigquery_bigframes` to `.env` and fill in:
   ```bash
   FF_BQ_PROJECT=<your-project-id>
   FF_BQ_DATASET=dq_demo
   FF_BQ_LOCATION=<region>   # e.g., EU or US
   GOOGLE_APPLICATION_CREDENTIALS=../secrets/<service-account>.json  # or rely on gcloud / WIF
   ```
2. Run via the Makefile from `examples/dq_demo`:
   ```bash
   make demo ENGINE=bigquery BQ_FRAME=pandas      # or bigframes
   ```

Both profiles accept `allow_create_dataset` in `profiles.yml` if you want the example to create the dataset automatically.

## Things to experiment with

To understand the tests better, intentionally break the data and re-run `fft test`:

* Set one `customers.customer_id` to `NULL` → watch `not_null` fail.
* Duplicate a `customer_id` → watch `unique` fail.
* Put a negative `amount` in `orders.csv` → `greater_equal` and `non_negative_sum` fail.
* Add a new `status` value (e.g. `"paused"`) → `accepted_values` warns.
* Drop a customer from `mart_orders_agg` manually (or filter it out in SQL) → `reconcile_coverage` fails.
* Change an amount in the mart only → reconciliation tests fail.

This makes it very clear what each test guards against.

---

## Summary

The Data Quality Demo is designed to be:

* **Small and readable** – customers, orders, and a single mart.
* **Complete** – exercises every built-in FFT DQ test type.
* **Practical** – real-world patterns like:

  * typing in staging models,
  * testing freshness on a mart timestamp,
  * reconciling sums and row counts across tables.

Once you’re comfortable with this example, you can copy the patterns into your real project: start with staging-level checks, then layer in reconciliations and freshness on your most important marts.
