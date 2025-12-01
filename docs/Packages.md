# Packages

FastFlowTransform **packages** let you reuse models and macros across projects.

A *package* is just another FFT project (or mini-project) whose `models/` you want to treat as if they were part of your current project. You wire it in via a `packages.yml` file in your project root.

Typical use cases:

* A shared **staging layer** (e.g. CRM / ERP cleaning) used by multiple teams.
* A central **macro library** (casting helpers, email parsing, date tricks).
* A “starter kit” of **canonical marts** that downstream projects can add on top of.

Packages can come from:

* A **local path** on disk.
* A **git repository** (with optional branch/tag/commit + subdir).

---

## 1. High-level behavior

When you declare packages in `packages.yml`:

1. FFT loads your **main project** as usual.

2. It runs the **package resolver**:

   * For each entry in `packages.yml` it:

     * locates the package:

       * local: resolves the `path` on disk.
       * git: clones/fetches a repo into `.fastflowtransform/packages` and checks out the requested ref.
     * reads that package’s **`project.yml`** (the package manifest):

       * `name`, `version`, optional `fft_version`, optional `dependencies`, optional `models_dir`.
     * validates:

       * manifest `name` matches the `name` from `packages.yml`.
       * `fft_version` (if present) is compatible with the running FFT version.
       * the spec’s `version` constraint (if present) is satisfied by `manifest.version`.
       * package dependencies (if declared) are satisfied by other packages.
   * Writes a `packages.lock.yml` with **pinned** sources (paths / git commit SHAs).

3. For each **resolved package**, FFT:

   * decides which directory to treat as its `models_dir`:

     * `packages.yml:models_dir` overrides `project.yml:models_dir`, default `"models"`.
   * loads SQL / Python models and macros from that directory.
   * registers them into the **same namespace** as your own models.

From inside your project you can:

* `ref('users_base.ff')` even if `users_base.ff.sql` physically lives in a package folder.
* Use macros defined in `models/macros/*.sql` inside a package.

> There is no special syntax for package references; once loaded, package models look like any other model.

---

## 2. Minimal setup

### 2.1. Create a reusable package

A package is structured like a normal FFT project, but consumers mainly care about its `models/` and macros.

```text
shared_package/
  project.yml
  models/
    macros/
      shared_utils.sql
    staging/
      users_base.ff.sql
```

Example `project.yml` in the **package**:

```yaml
name: shared_package
version: "0.1"

# Where this package’s models live relative to the package root.
# This can be overridden by packages.yml in the consumer project.
models_dir: models

# Optional: constrain which FFT core versions can use this package.
# If omitted, any FFT version is allowed.
fft_version: ">=0.6.0,<0.7.0"

# Optional: dependencies on other packages (by name) if you compose packages.
# These are validated against the set of packages declared in the consumer’s packages.yml.
dependencies: []
```

Example macro (`models/macros/shared_utils.sql`):

```jinja
{# Shared SQL macros for the package #}

{%- macro email_domain(expr) -%}
  lower(regexp_replace({{ expr }}, '^.*@', ''))
{%- endmacro -%}
```

Example staging model (`models/staging/users_base.ff.sql`):

```jinja
{{ config(
    materialized='view',
    tags=[
        'pkg:shared_package',
        'scope:staging',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery',
    ],
) }}

with raw_users as (
    select
        cast(id as integer)       as user_id,
        lower(email)              as email,
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

### 2.2. Declare the package in your project

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
    path: "../shared_package"   # resolved relative to this file
    models_dir: "models"        # optional; defaults to "models"
```

* `name`
  Logical name for the package, taken from the package’s own `project.yml`. This must match the manifest; it’s used for logs, diagnostics, and dependency checks.
* `path`
  Filesystem location of the **package root**, resolved relative to the directory containing `packages.yml`.
* `models_dir` (optional)
  Subdirectory within the package root that contains the package’s models. Defaults to `models`. If both `project.yml:models_dir` and `packages.yml:models_dir` are set, **`packages.yml` wins**.

---

### 2.3. Use package models in your project

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
    from {{ ref('users_base.ff') }}    -- defined in the shared_package
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

The DAG will show something like:

```text
crm.users (source) → users_base.ff (from package) → mart_users_from_package.ff (local)
```

---

## 3. `packages.yml` – configuration reference

`packages.yml` must live in the **project root**, next to `project.yml`:

```text
my_project/
  project.yml
  packages.yml
  models/
  …
```

Top-level structure:

```yaml
packages:
  - name: ...
    ...
```

You can declare both **path-based** and **git-based** packages. Exactly one of `path` or `git` must be set per package.

### 3.1. Path packages

```yaml
packages:
  - name: shared_package
    path: "../shared_package"   # relative or absolute
    models_dir: "models"        # optional
    # optional semver constraint on the package’s manifest version:
    version: ">=0.1.0,<0.2.0"
```

Fields:

* `name` (required)
  Must match `project.yml:name` inside the package root.
* `path` (required for path packages)
  Relative or absolute path to the package root. Resolved relative to `packages.yml`.
* `models_dir` (optional)
  Models directory inside the package root. Default `"models"`.
* `version` (optional)
  Semver constraint for the package’s `project.yml:version`. See [4.2 Version constraints](#42-version-constraints).

### 3.2. Git packages

```yaml
packages:
  - name: shared_package_git
    git: "https://github.com/fftlabs/fastflowtransform.git"

    # Directory inside the repo that contains the package
    subdir: "examples/packages_demo/shared_package_git_remote"

    # Optional ref selectors (only one needs to be set; see notes below)
    ref: "main"        # generic alias (branch / tag / commit)
    # rev: "abc1234"   # explicit commit SHA
    # tag: "v0.6.11"   # tag name
    # branch: "main"   # branch name

    models_dir: "models"

    # Optional semver constraint on the package's manifest version
    version: ">=0.1.0,<0.2.0"
```

Fields:

* `name` (required)
  Must match `project.yml:name` in the package subdir.
* `git` (required for git packages)
  Git URL (HTTPS or SSH, depending on your environment).
* `subdir` (optional but recommended)
  Path inside the repo that should be treated as the package root (relative to the repo root). If omitted, the repo root itself is the package root.
* `ref` (optional)
  Generic *user-facing* selector (branch, tag, or commit). If you don’t specify a more precise field (`rev` / `tag` / `branch`), `ref` is mapped internally to `rev` and passed directly to `git checkout`.
* `rev` / `tag` / `branch` (optional)
  More explicit selectors, used in preference to `ref` if set.
* `models_dir` (optional)
  Models directory inside the `subdir` root (default `"models"`).
* `version` (optional)
  Semver constraint for the package’s `project.yml:version`.

Resolution rules:

* FFT clones/fetches git packages into:

  ```text
  .fastflowtransform/packages/git/<slug>/repo
  ```

  where `<slug>` encodes the package name and git URL.
* For each package, FFT:

  * clones the repo (if missing),
  * attempts a `git fetch --all` (best effort) if it already exists,
  * runs `git checkout <ref>` using:

    * `rev` or `tag` or `branch` (first non-empty),
    * or `HEAD` if none are provided.

If Git commands fail, you get targeted error messages:

* Missing git binary → “git executable not found…”
* Auth issues → “authentication error…”
* Wrong repo / URL → “repository not found…”
* Bad ref / branch / tag → “requested ref/branch/tag does not exist…”

---

### 3.3. Shorthand mapping form

For local packages you can use a shorter mapping form:

```yaml
# Equivalent to packages: [ { name: shared_package, path: ../shared_package } ]

shared_package: "../shared_package"
other_pkg:
  path: "../other"
  models_dir: "dbt_models"
```

Internally this is normalized to the explicit `packages:` list.

---

### 3.4. Multiple packages

You can declare multiple packages:

```yaml
packages:
  - name: shared_staging
    path: "../shared_staging"

  - name: analytics_macros
    git: "https://github.com/my-org/analytics-macros.git"
    subdir: "packages/sql_macros"
    models_dir: "models"
```

All models/macros from all packages are loaded into the same logical project.

---

## 4. Manifests, versions & dependencies

### 4.1. Package manifests (`project.yml` inside the package)

Every package has its own `project.yml` at the package root (or package `subdir` for git packages):

```yaml
name: shared_package
version: "0.1.0"
models_dir: "models"    # optional; may be overridden by packages.yml
fft_version: ">=0.6.0,<0.7.0"   # optional

dependencies:
  - name: other_shared_pkg
    version: ">=1.0.0,<2.0.0"
    optional: false
```

FFT uses this manifest for:

* `name`
  Must match the `name` from `packages.yml`.
* `version`
  Compared to the spec’s `version` constraint, if provided.
* `fft_version` (optional)
  Semver constraint against the running FFT version. If your package only supports certain FFT versions, set this. If the constraint is not satisfied, resolution fails with a clear error.
* `models_dir` (optional)
  Default path for models within the package root; overridden by `packages.yml:models_dir` if set.
* `dependencies` (optional)
  A list of other **packages** (by name) this package expects to be present in the same project.

  Each dependency entry may include:

  * `name` – required, another package’s name.
  * `version` – optional semver constraint on that package’s `project.yml:version`.
  * `optional` – if `true`, missing dependency is allowed; otherwise it is an error.

Resolution validates that:

* Every non-optional dependency `name` is present in the set of packages declared in the **consumer’s** `packages.yml`.
* If a `version` constraint is given for a dependency, the resolved dependency’s version satisfies it.

---

### 4.2. Version constraints

Package specs and dependencies support a tiny semver subset. Version strings must be in `MAJOR.MINOR.PATCH` form (e.g. `1.2.3`).

Supported constraint forms:

* Bare version:

  ```text
  "1.2.3"         # equivalent to "==1.2.3"
  ```

* Comparators (can be combined with commas or spaces):

  ```text
  ">=1.2.0,<2.0.0"
  ">1.0.0 <=2.0.0"
  ```

* `^` (caret) ranges:

  ```text
  "^1.2.3"        # >=1.2.3,<2.0.0
  "^0.3.0"        # >=0.3.0,<0.4.0
  "^0.0.4"        # >=0.0.4,<0.0.5
  ```

* `~` (tilde) ranges:

  ```text
  "~1.2.3"        # >=1.2.3,<1.3.0
  ```

The resolver checks:

* Consumers → packages: `packages.yml:version` vs package’s `project.yml:version`.
* Package → package: `dependencies[].version` vs the dependent package’s version.
* Package → FFT core: `project.yml:fft_version` vs the running FFT version.

If a constraint fails, you get a clear runtime error showing which package and which constraint failed.

---

## 5. What gets loaded (and what doesn’t)

When FFT loads a package, it will:

**Loads:**

* `project.yml` manifest (for name, version, fft_version, dependencies, models_dir).
* SQL models: `*.ff.sql` under the resolved `models_dir`.
* Python models: `*.ff.py` under `models_dir`.
* SQL macros: under `models_dir/macros/` (e.g. `macros/shared_utils.sql`).
* Python helpers/macros: under `models_dir/macros_py/` (same mechanism as the main project).

**Does NOT load / run automatically:**

* `profiles.yml` from the package — the consumer project’s profiles are always used.
* Seeds / sources defined in the package — these are still local to the consumer project.
* Tests declared in the package’s `project.yml` — only tests in the **consumer project’s** `project.yml` are run on `fft test`.

> In practice, package models still call `source('…')` and `ref('…')`. The **consumer project** is responsible for defining sources / seeds / additional models.

---

## 6. Name resolution & conflicts

### 6.1. Model names

Once loaded, a package model is just a regular model:

* It has a logical name (e.g. `users_base.ff`).
* It is registered in the same global registry as your local models.

Rules:

* `ref('<model_name>')` never has a package prefix. You always use the model name alone.
* Model names must be **globally unique** across:

  * your main project,
  * all loaded packages.

If two models share a name (e.g. `users_base.ff` in both main and package), FFT will fail loading with a clear “Duplicate model name” error. You must rename or delete one of them.

### 6.2. Macros

Macros from packages and local macros all end up in the same Jinja environment.

* Name collisions are possible.
* “Last one wins” — whichever macro is registered last overrides earlier ones.

Best practice:

* Prefix macro names with a package-ish prefix: `shared_email_domain`, etc.
* Or use explicit `{% import 'macros/shared_utils.sql' as shared %}` and call `shared.email_domain()` from consumer models.

---

## 7. Lock file: `packages.lock.yml`

After successful resolution, FFT writes a `packages.lock.yml` next to `packages.yml`:

```yaml
fft_version: "0.6.11"
packages:
  - name: shared_package
    version: "0.1.0"
    source:
      kind: path
      path: "/absolute/path/to/shared_package"

  - name: shared_package_git
    version: "0.1.0"
    source:
      kind: git
      git: "https://github.com/fftlabs/fastflowtransform.git"
      rev: "abc1234deadbeef..."             # resolved commit SHA
      subdir: "examples/packages_demo/shared_package_git_remote"
```

Today the lock file is:

* **Written** after each successful resolution.
* Useful for diagnostics, reproducibility, CI logs, etc.

(Resolution is still driven by `packages.yml`; the lockfile does not yet drive resolution itself.)

---

## 8. CLI: `fft deps`

The `deps` command inspects packages for your project and shows their resolved status:

```bash
fft deps .
```

Behavior:

* Resolves the project directory.

* Runs the **full** package resolver (same as `fft run` would):

  * locates local path packages,
  * clones/fetches git packages,
  * loads `project.yml` manifests,
  * validates version constraints and dependencies,
  * writes `packages.lock.yml`.

* Prints a small report for each package:

  ```text
  Project: /path/to/my_project
  Packages:
    - shared_package (0.1.0)
        kind:       path
        path:       /abs/path/to/shared_package
        models_dir: models  -> /abs/path/to/shared_package/models
        status:     OK

    - shared_package_git (0.1.0)
        kind:       git
        git:        https://github.com/fftlabs/fastflowtransform.git
        rev:        abc1234deadbeef...
        subdir:     examples/packages_demo/shared_package_git_remote
        models_dir: models  -> /abs/.../repo/examples/packages_demo/shared_package_git_remote/models
        status:     OK
  ```

* Exits with **non-zero** status if any package’s `models_dir` is missing or invalid.

This is the easiest way to debug:

* git connectivity / credentials,
* bad refs (`tag` / `branch` / `rev`),
* missing `project.yml` in a package,
* version constraint mismatches,
* missing `models_dir` directories.

---

## 9. DAGs, caching, selectors

Once packages are resolved, FFT essentially treats:

> **“main project + all packages” as one large logical project.**

### 9.1. DAG & docs

* `fft dag` and the generated HTML docs include package models and edges between them and your local models.
* You can inspect `.fastflowtransform/target/manifest.json` if you need to distinguish package vs local models programmatically (nodes carry metadata like their originating package).

### 9.2. Caching

Build caching behaves the same for package models as for local ones:

* Fingerprints incorporate:

  * SQL/Python source of the package model,
  * upstream dependencies,
  * environment, etc.
* Changing a package model’s code changes its fingerprint and invalidates cache for that model and its downstream dependents.

### 9.3. Selectors & tests

Selectors (`--select`, `--exclude`) are package-agnostic:

* You can tag package models:

  ```jinja
  {{ config(
      tags=['pkg:shared_package', 'scope:staging'],
  ) }}
  ```

* Then:

  ```bash
  fft run . --env dev_duckdb --select tag:pkg:shared_package
  ```

You can define tests in your **main project** for tables produced by package models:

```yaml
tests:
  - type: not_null
    table: users_base
    column: email
    tags: [example_packages_demo]
```

Only the tests defined in the **consumer** project’s `project.yml` are executed on `fft test`.

---

## 10. Best practices & pitfalls

### 10.1. Treat packages like libraries

* Always set a `version` in the package’s `project.yml`.
* Use tags/releases/branches on the git repo for meaningful versions.
* Use `packages.yml:version` constraints to avoid accidental breaking upgrades.

### 10.2. Keep responsibilities clear

* **Package:** shared semantics (cleaning, typing, derived fields), stable over time.
* **Consumer project:** product/report-specific marts and joins.

### 10.3. Avoid tight coupling to specific schemas

* Use generic `source('domain','table')` names in packages.
* Document expected sources in the package README.
* Let each consumer wire those to their actual tables via their own `sources.yml`.

### 10.4. Tag and namespace thoughtfully

* Tag package models with something like `pkg:<name>` to make them easy to select/exclude.
* Use macro namespaces or prefixes to reduce collisions.

### 10.5. Common errors

* **`Package root … has no project.yml`**
  Your package directory (or git `subdir`) is wrong. Point `path`/`subdir` at the folder that actually contains the package’s `project.yml`.

* **Git errors about authentication or unknown revision**
  Check your git URL/credentials and branch/tag/commit. `fft deps` will show the raw git error stderr to help you debug.

* **Version mismatch errors**
  Align:

  * the package’s `version` and your `packages.yml:version`,
  * the package’s `fft_version` and your installed FFT version.

---

**In short:** packages let you compose FFT projects like libraries, with both local and git-backed sources, basic versioning, and a resolver + lockfile that make behavior explicit and debuggable.
