# Packages Demo

The **packages demo** shows how to split FastFlowTransform logic into a **reusable package**
and a **consumer project** that imports it via `packages.yml`.

It answers:

- How do I **share staging models** and macros across multiple projects?
- How does `packages.yml` work?
- How do I `ref()` a model that physically lives in another directory/tree?

Use this as a template for building your own internal “FFT packages” repo.

---

## Layout

The example lives under:

```text
examples/packages_demo/
  shared_package/      # reusable package
  main_project/        # normal FFT project that consumes the package
````

### `shared_package` – reusable code

This folder behaves like a **mini FFT project**:

```text
shared_package/
  project.yml
  models/
    README.md
    macros/
      shared_utils.sql
    staging/
      users_base.ff.sql
```

It includes:

* `project.yml` – minimal config so FFT knows how to load its models.
* `models/macros/shared_utils.sql` – shared SQL macros (e.g. `email_domain(expr)`).
* `models/staging/users_base.ff.sql` – a reusable staging model that:

  * reads `source('crm', 'users')`
  * normalizes emails
  * derives `email_domain`.

You **do not** call `fft run shared_package` directly in this demo.
Instead, `shared_package` is loaded by the consumer project via `packages.yml`.

### `main_project` – normal FFT project

```text
main_project/
  .env.dev_duckdb
  Makefile
  README.md
  profiles.yml
  project.yml
  packages.yml
  sources.yml
  models/
    README.md
    marts/
      mart_users_from_package.ff.sql
  seeds/
    README.md
    seed_users.csv
  tests/
    unit/
      README.md
```

This is a regular project:

* `profiles.yml` – DuckDB connection profile (`dev_duckdb`).
* `.env.dev_duckdb` – points DuckDB at `.local/packages_demo.duckdb`.
* `seeds/seed_users.csv` + `sources.yml` – define `source('crm','users')`.
* `packages.yml` – declares the dependency on `../shared_package`.
* `models/marts/mart_users_from_package.ff.sql` – a local mart that does:

  ```jinja
  from {{ ref('users_base.ff') }}
  ```

  where `users_base.ff` is defined in the **package**, not in `main_project`.

---

## Key concepts

### 1. Declaring packages – `packages.yml`

In `main_project/packages.yml`:

```yaml
packages:
  - name: shared_package
    path: "../shared_package"
    models_dir: "models"
```

* `name` – logical package name (for logging / internal bookkeeping).
* `path` – where to find the package directory, relative to `packages.yml`.
* `models_dir` – where to look for models inside the package (defaults to `models` if omitted).

At load time, the core:

1. Reads `packages.yml`.
2. For each entry, loads its `project.yml` (if present) and its `models/`.
3. Registers all models/macros from the package **into the same namespace** as local models.

From the perspective of `main_project`, `users_base.ff` looks just like any other model —
you can `ref('users_base.ff')` without caring that it physically lives in `../shared_package`.

### 2. Referencing package models with `ref()`

In `main_project/models/marts/mart_users_from_package.ff.sql`:

```jinja
with base as (
    select
        email_domain,
        signup_date
    from {{ ref('users_base.ff') }}
)
select
    email_domain,
    count(*) as user_count,
    min(signup_date) as first_signup,
    max(signup_date) as last_signup
from base
group by email_domain
order by email_domain;
```

* `users_base.ff` is defined in `shared_package/models/staging/users_base.ff.sql`.
* Because the package is registered, `ref('users_base.ff')` resolves correctly.
* The DAG includes both the package model and the local mart.

**Important:** model names must still be globally unique. If you define a model with the same name
in both the package and the project, you’ll get a conflict (which is what you want).

### 3. Shared macros from a package

The package ships a simple macro file:

```jinja
-- shared_package/models/macros/shared_utils.sql
{%- macro email_domain(expr) -%}
  lower(regexp_replace({{ expr }}, '^.*@', ''))
{%- endmacro -%}
```

`users_base.ff` uses it:

```jinja
select
  user_id,
  email,
  {{ email_domain("email") }} as email_domain,
  signup_date
from raw_users;
```

Because macros are loaded from both the main project and all packages into the same Jinja environment:

* models in the **package** can use macros from the package,
* models in the **main project** can also use those macros if you want, subject to naming rules.

---

## Data flow

The demo intentionally mirrors the basic_demo pipeline but splits staging into a package:

```text
(main_project) seeds/seed_users.csv
       │
       ├─ fft seed
       ▼
   seed_users (DuckDB table via sources.yml → crm.users)
       │
       ├─ shared_package/models/staging/users_base.ff.sql
       │   (materialized view)
       ▼
   users_base
       │
       ├─ main_project/models/marts/mart_users_from_package.ff.sql
       ▼
   mart_users_from_package
```

The DAG (after a run) will roughly show:

```text
crm.users (source) → users_base.ff (package) → mart_users_from_package.ff (main_project)
```

---

## Running the demo

From the repo root:

```bash
cd examples/packages_demo/main_project
```

### 1. Configure DuckDB env

```bash
set -a; source .env.dev_duckdb; set +a
# This sets:
#   FF_DUCKDB_PATH=.local/packages_demo.duckdb
#   FFT_ACTIVE_ENV=dev_duckdb
#   FF_ENGINE=duckdb
```

### 2. Run the full demo

```bash
make demo ENGINE=duckdb
```

This will:

1. **clean** – drop local artifacts and DuckDB file via `cleanup_env.py`.

2. **seed** – `fft seed . --env dev_duckdb`:

   * loads `seeds/seed_users.csv` into DuckDB as `seed_users`.

3. **run** – `fft run . --env dev_duckdb` with:

   ```bash
   --select tag:example:packages_demo --select tag:engine:duckdb
   ```

   Only models tagged for this example are built:

   * `users_base.ff` (from `shared_package`)
   * `mart_users_from_package.ff` (from `main_project`)

4. **dag** – `fft dag . --env dev_duckdb --html`:

   * writes HTML docs to `main_project/site/dag/index.html`.

5. **test** – `fft test . --env dev_duckdb`:

   * runs DQ tests from `project.yml` (`not_null`, `unique`, `greater_equal`).

6. **artifacts** – prints paths to `manifest.json`, `run_results.json`, `catalog.json`.

### 3. Inspect results

* DAG HTML:

  ```text
  examples/packages_demo/main_project/site/dag/index.html
  ```

* Artifacts:

  ```text
  examples/packages_demo/main_project/.fastflowtransform/target/manifest.json
  examples/packages_demo/main_project/.fastflowtransform/target/run_results.json
  examples/packages_demo/main_project/.fastflowtransform/target/catalog.json
  ```

* DuckDB file (if you want to open it manually):

  ```text
  examples/packages_demo/main_project/.local/packages_demo.duckdb
  ```

---

## What this demo demonstrates

1. **Package loading**

   `packages.yml` allows you to point at **another tree of models** and macros and load them as if they were local.

2. **Shared staging layers**

   You can move “standardized” staging code (sources, cleaning, type-casting, email normalization, etc.)
   into a central `shared_package` and reuse it from multiple projects.

3. **Consistent naming**

   Since packaged models live in the same logical namespace, you get early feedback if two projects try to
   define a model with the same name.

4. **Separation of concerns**

   * Package: stable, reused logic (e.g. `users_base`).
   * Main project: business-specific marts and reporting (`mart_users_from_package`).

---

## Things to try

To understand packages better, experiment with:

1. **Breaking the shared model**

   * Edit `shared_package/models/staging/users_base.ff.sql` (e.g. remove `email_domain`).
   * Re-run `make demo`.
   * Watch `mart_users_from_package` fail because the column is missing — proving the dependency goes through the package.

2. **Adding a new shared macro**

   * Add a `country_label(expr)` macro in `shared_utils.sql`.
   * Use it in a *local* model inside `main_project` to see that macros from the package are visible in the consumer.

3. **Adding another package**

   * Create `examples/packages_demo/another_package` with its own `project.yml` and models.
   * Extend `main_project/packages.yml` with a second entry and confirm both package’s models appear in the DAG.

4. **Introducing a naming conflict**

   * Define a model named `users_base.ff` inside `main_project/models/staging`.
   * Reload the project; you should get a clear error about duplicate model names, which is your cue to rename or explicitly choose one.

---

## Summary

The packages demo is a minimal, concrete example of:

* Defining a reusable FastFlowTransform **package** (`shared_package`).
* Wiring it into a **consumer project** (`main_project`) via `packages.yml`.
* Building a mart that depends on a model defined outside of the project tree.
* Running everything through the normal `fft seed`, `fft run`, `fft dag`, and `fft test` workflow.

You can adopt the same pattern to share:

* Standard staging layers (CRM / ERP / web analytics),
* Macro libraries (date helpers, casting utilities),
* Even entire mini-marts that represent common dimensional models across teams.
