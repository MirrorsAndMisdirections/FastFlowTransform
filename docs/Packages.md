# Packages

FastFlowTransform **packages** let you reuse models and macros across projects.

A *package* is just another FFT project (or mini-project) whose `models/` you want to treat as if they were part of your current project. You wire it in via a `packages.yml` file in your project root.

Typical use cases:

* A shared **staging layer** (e.g. CRM / ERP cleaning) used by multiple teams.
* A central **macro library** (casting helpers, email parsing, date tricks).
* A “starter kit” of **canonical marts** that downstream projects can add on top of.

---

## High-level behavior

When you declare packages in `packages.yml`:

1. FFT loads your **main project** as usual.
2. For each entry in `packages.yml`, FFT:

   * resolves the path on disk,
   * reads that package’s `project.yml` (if present),
   * loads its `models/` and macros.
3. All package models and macros are registered into the **same namespace** as your own.

From inside your project you can:

* `ref('users_base.ff')` even if `users_base.ff.sql` physically lives in `../shared_package/models/…`.
* Use macros defined under `shared_package/models/macros/*.sql` in your own models.

> There is no special syntax for package references; once loaded, package models look like any other model.

---

## 1. Minimal setup

### 1.1. Create a reusable package

A package looks like a regular FFT project, but you mainly care about its `models/` and macros.

```text
shared_package/
  project.yml
  models/
    macros/
      shared_utils.sql
    staging/
      users_base.ff.sql
```

Example `project.yml` in the package:

```yaml
name: shared_package
version: "0.1"
models_dir: models

# (Optional) tests/docs/etc. are allowed here but are not special in the consumer.
vars: {}
tests: []
```

Example macro (`models/macros/shared_utils.sql`):

```jinja
{%- macro email_domain(expr) -%}
  lower(regexp_replace({{ expr }}, '^.*@', ''))
{%- endmacro -%}
```

Example staging model (`models/staging/users_base.ff.sql`):

```jinja
{{ config(
    materialized='view',
    tags=['shared:staging', 'engine:duckdb', 'engine:postgres', 'engine:databricks_spark', 'engine:bigquery'],
) }}

with raw_users as (
  select
    cast(id as integer) as user_id,
    lower(email)        as email,
    cast(signup_date as date) as signup_date
  from {{ source('crm', 'users') }}
)
select
  user_id,
  email,
  {{ email_domain("email") }} as email_domain,
  signup_date
from raw_users;
```

This package expects the **consumer project** to define `source('crm','users')`.

---

### 1.2. Declare the package in your project

In your main project:

```text
my_project/
  project.yml
  packages.yml   ← new
  models/
  seeds/
  …
```

Create `packages.yml`:

```yaml
packages:
  - name: shared_package
    path: "../shared_package"
    models_dir: "models"
```

* `name`
  Logical name for the package (used for logs/diagnostics). Does *not* change how you `ref()` models.
* `path`
  Filesystem location of the package folder, resolved **relative to the directory containing `packages.yml`**.
* `models_dir` (optional)
  Subdirectory containing the package’s models. Defaults to `models` if omitted.

---

### 1.3. Use package models in your project

Now, in `my_project/models/marts/mart_users_from_package.ff.sql`:

```jinja
{{ config(
    materialized='table',
    tags=['example:packages_demo', 'scope:mart', 'engine:duckdb'],
) }}

with base as (
  select
    email_domain,
    signup_date
  from {{ ref('users_base.ff') }}    -- defined in the package
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

Run as usual:

```bash
fft seed . --env dev_duckdb
fft run  . --env dev_duckdb
fft dag  . --env dev_duckdb --html
```

The DAG will show:

```text
crm.users (source) → users_base.ff (from package) → mart_users_from_package.ff (local)
```

---

## 2. `packages.yml` – configuration reference

`packages.yml` must live in the **project root**, next to `project.yml`:

```text
my_project/
  project.yml
  packages.yml
  models/
  …
```

Structure:

```yaml
packages:
  - name: <string>             # required
    path: <string>             # required, relative or absolute path to the package root
    models_dir: <string>       # optional, defaults to "models"
```

Notes:

* `path` is resolved relative to `packages.yml`’s directory:

  * `../shared_package` → sibling folder
  * `vendor/my_pkg` → subfolder
* `models_dir` allows you to keep a different structure in the package:

  * Example: `models_dir: "src/models"`.

### Multiple packages

You can declare multiple packages:

```yaml
packages:
  - name: shared_staging
    path: "../shared_staging"

  - name: analytics_macros
    path: "../analytics_macros"
    models_dir: "macros_only"
```

All models/macros from all packages are loaded into the same project.

---

## 3. What gets loaded (and what doesn’t)

Currently, packages are focused on **models and macros**.

When FFT loads a package, it will:

* Read the package’s `project.yml` (if present) for:

  * `name`, `version` (for metadata),
  * `models_dir` (overridden by `packages.yml` if provided).
* Load:

  * SQL models (`*.ff.sql`) from the package’s `models_dir`.
  * Python models (`*.ff.py`) from the package’s `models_dir`.
  * SQL macros under `models_dir/macros/` (standard Jinja macro files).
  * Python render-time helpers/macros if your core exposes them from the package (same mechanism as the main project).

And it will **not**:

* Load or execute the package’s `profiles.yml` – the consumer project’s profiles are always used.
* Automatically register package **seeds** or **sources**; those stay local to the consumer.
* Automatically run the package’s DQ tests; only tests declared in the **consumer project’s** `project.yml` are executed on `fft test`.

> In practice, package models often still refer to `source('…')` or `ref('…')`.
> The *consumer* project is responsible for:
>
> * defining sources in its own `sources.yml`, and
> * wiring any extra seeds needed.

---

## 4. Name resolution & conflicts

### 4.1. Model names

Once loaded, a package model is just a regular model in the registry:

* It has a **logical name** (e.g. `users_base.ff`).
* Its file path and package association are recorded as metadata.

Rules:

* `ref('<model_name>')` has **no package prefix**. You always use the bare model name.
* Model names must be **globally unique** across:

  * your main project,
  * all packages.

If two models with the same name are found (e.g. `users_base.ff` in both main and package), FFT raises a clear error during project loading. You must rename or decide which one you want.

### 4.2. Macros

Macros from packages are injected into the **same Jinja environment** as your own macros:

* Name collisions are possible.
* If two macros share the same name, whichever is registered last will “win”.

Best practice:

* Prefix shared macros with a **package-ish** prefix (e.g. `shared_email_domain`), or
* Group them in macro files you explicitly `{% import 'macros/shared_utils.sql' as shared %}` and then call `shared.email_domain()`.

---

## 5. DAGs, caching, and manifests

Once packages are loaded, the pipeline behaves like a single large project.

### 5.1. DAG & docs

* `fft dag` sees package models as part of the DAG.
* The generated HTML docs show:

  * nodes for package models,
  * nodes for local models,
  * edges between them.

Package models typically carry an extra metadata field (`package_name`) used in the catalog/manifest; you can inspect `.fastflowtransform/target/manifest.json` if you want to differentiate them programmatically.

### 5.2. Caching and fingerprints

Build caching (`--cache`) treats package models like any other:

* Fingerprints include:

  * SQL/Python source from the **package** file,
  * environment vars,
  * upstream dependencies, etc.
* If a package model’s code changes, its fingerprint changes, and:

  * that model will rebuild on the next run,
  * downstream models (local or from other packages) will also rebuild if needed.

### 5.3. Tests and selectors

Selectors (`--select`, `--exclude`) are agnostic to package vs. local:

* You can tag package models with `tags: ['shared:staging']` and run:

  ```bash
  fft run . --env dev_duckdb --select tag:shared:staging
  ```

* You can define DQ tests in your **main project**’s `project.yml` targeting package tables:

  ```yaml
  tests:
    - type: not_null
      table: users_base
      column: email
      tags: [example:packages_demo]
  ```

---

## 6. Best practices

### 6.1. Keep packages stable and versioned

Treat a shared package like a library:

* Maintain a `version` in `project.yml`.
* Avoid backwards-incompatible changes without coordination:

  * e.g. dropping columns or changing semantics in shared staging models.
* Consider tagging or branching in Git to coordinate upgrades across consumers.

(There’s no built-in package registry or version pinning yet; you control which commit of the package you point to via Git + `path`.)

### 6.2. Package responsibility

A good rule of thumb:

* **Package:** “What does *user* mean for us?” — common cleaning, typing, normalization, derivations (e.g. `email_domain`, `customer_segment`).
* **Consumer project:** “What do we need for *this* product/report?” — marts, joins across domains, project-specific logic.

This keeps packages focused and low-churn.

### 6.3. Avoid tight coupling to local schemas

Shared packages shouldn’t depend on highly project-specific schemas or seeds. Instead:

* Use `source('domain', 'table')` with generic names (“crm.users”, “billing.invoices”).
* Document in the package README what sources it expects.
* Let each consumer wire those sources to its concrete tables via its own `sources.yml`.

### 6.4. Tag everything

Give package models clear tags:

```jinja
{{ config(
    tags=[
      'pkg:shared_package',
      'scope:staging',
      'engine:duckdb',
      'engine:postgres',
    ],
) }}
```

Then consumers can:

* include only `tag:pkg:shared_package` in some runs,
* or exclude them via `--exclude tag:pkg:shared_package` if they want to run only local marts.

---

## 7. Common pitfalls & how to avoid them

**❌ Package model fails: `source('crm','users')` not found**

* You’re using a package model that references a source your main project hasn’t declared.
* Fix: add a matching `sources.yml` entry in your main project:

  ```yaml
  version: 2
  sources:
    - name: crm
      tables:
        - name: users
          identifier: seed_users
  ```

---

**❌ Duplicate model name between project and package**

* You have `models/staging/users_base.ff.sql` locally **and** in the package.
* Fix: rename one of them, or drop the local one if you want to fully delegate to the package.

---

**❌ Macro name collision**

* Same macro name in package and project; behavior seems “random”.
* Fix: rename macros or use explicit `{% import %}` and call macros with a namespace alias.

---

## Summary

Packages let you:

* Factor out **shared staging** and **macro libraries**.
* Reuse them across many projects via a simple `packages.yml`.
* Keep execution, caching, DAGs, and tests working as if everything were one project.

The mental model is:

> “My project + all packages = one big FastFlowTransform project
> where some models just happen to live in other directories.”

As your internal ecosystem grows, you can introduce multiple packages (per domain, per team, per capability) and let downstream projects compose them like building blocks.
