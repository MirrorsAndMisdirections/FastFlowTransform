# Packages Demo

The **packages demo** shows how to split FastFlowTransform logic into:

* a **reusable local package** (path-based), and
* a **git-backed package**,

and a **consumer project** that imports both via `packages.yml`.

It answers:

* How do I **share staging models** and macros across multiple projects?
* How does `packages.yml` work for **local path** and **git** sources?
* How do I `ref()` a model that physically lives in another directory/tree?
* How do I see which exact versions / commits are being used?

Use this as a template for building your own internal “FFT packages” repo.

---

## Layout

The example lives under:

```text
examples/packages_demo/
  shared_package/             # reusable local package (path-based)
  shared_package_git_remote/  # git-style package (lives in this repo for the demo)
  main_project/               # normal FFT project that consumes both
```

### 1. `shared_package` – local reusable code

This folder behaves like a **mini FFT project** that’s consumed via a local path:

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

Key pieces:

* `project.yml` – minimal manifest so FFT knows:

  * the package `name` / `version`,
  * where its models live (`models_dir`).
* `models/macros/shared_utils.sql` – shared SQL macros (e.g. `email_domain(expr)`).
* `models/staging/users_base.ff.sql` – a reusable staging model that:

  * reads `source('crm', 'users')`,
  * normalizes emails,
  * derives `email_domain` using the shared macro.

In this demo you **do not** run `fft` inside `shared_package/` directly.
It’s pulled in by `main_project` via `packages.yml`.

---

### 2. `shared_package_git_remote` – git-backed reusable code

For the git example we use a second package:

```text
shared_package_git_remote/
  project.yml
  models/
    README.md
    macros/
      shared_utils_git.sql
    staging/
      users_base_git.ff.sql
```

Conceptually this folder represents a **separate git repo**. In the demo it lives in the main FFT repo so that:

* `main_project` can point to the **GitHub URL** of this repo,
* and specify `subdir: examples/packages_demo/shared_package_git_remote` to use it as a git-based package.

Key ideas:

* It has its own `project.yml` with a different `name` (e.g. `shared_package_git`) and `version`.
* It has its own models / macros, distinct from `shared_package`, so you can see clearly which package a model came from.
* For a real deployment, you would typically push this folder into a dedicated repo and update the `git:` URL in `packages.yml` accordingly.

Again, you don’t run `fft` directly here; it’s consumed by `main_project` as a git package.

---

### 3. `main_project` – consumer FFT project

```text
main_project/
  env.dev_duckdb
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
      mart_users_from_git_package.ff.sql
  seeds/
    README.md
    seed_users.csv
  tests/
    unit/
      README.md
```

This is a regular FFT project that:

* owns the **engine configuration** (`profiles.yml`, `env.dev_duckdb`),
* defines **seeds** and **sources** for `crm.users`,
* declares **package dependencies** in `packages.yml`,
* defines **local marts** that depend on package models.

Key pieces:

* `profiles.yml` – DuckDB profile (`dev_duckdb`).
* `env.dev_duckdb` – sets `FF_DUCKDB_PATH`, `FFT_ACTIVE_ENV`, `FF_ENGINE`.
* `seeds/seed_users.csv` + `sources.yml` – define the `crm.users` source.
* `packages.yml` – declares both the local and git-backed packages.
* `models/marts/mart_users_from_package.ff.sql` – mart using the local package.
* `models/marts/mart_users_from_git_package.ff.sql` – mart using the git-backed package.

---

## Declaring packages – `packages.yml`

In `main_project/packages.yml` you wire in both packages:

```yaml
packages:
  # Local path package
  - name: shared_package
    path: "../shared_package"
    models_dir: "models"
    # optional: constrain acceptable versions from shared_package/project.yml
    version: ">=0.1.0,<0.2.0"

  # Git-based package (same repo, different subdir for the demo)
  - name: shared_package_git
    git: "https://github.com/fftlabs/fastflowtransform.git"

    # Directory inside the repo that contains the package root
    subdir: "examples/packages_demo/shared_package_git_remote"

    # Optional ref selectors (you typically choose *one* of these)
    ref: "main"
    # rev: "abc1234"   # explicit commit SHA
    # tag: "v0.6.11"   # tag name
    # branch: "main"   # branch name

    models_dir: "models"

    # optional: version constraint matched against shared_package_git_remote/project.yml
    version: ">=0.1.0,<0.2.0"
```

Notes:

* For **path packages**, set `path` and (optionally) `models_dir`.
* For **git packages**, set:

  * `git` – repo URL,
  * `subdir` – where the package lives inside the repo,
  * **one** of `ref` / `rev` / `tag` / `branch` (or nothing = `HEAD`),
  * `models_dir` if needed,
  * optional `version` constraint.
* Internally, `ref` is treated as a generic alias and mapped to `rev` if you don’t provide `rev` / `tag` / `branch`.

At load time, the core:

1. Reads `packages.yml`.
2. For each package:

   * materializes its source:

     * path: resolve `path` relative to `packages.yml`,
     * git: clone/fetch repo into `.fastflowtransform/packages/git/...` and `git checkout` the requested ref.
   * loads its `project.yml` manifest:

     * checks `name` and `version`,
     * validates `fft_version` (if present) against the running FFT core,
     * validates `version` constraints from `packages.yml`,
     * validates inter-package `dependencies` (from the package manifest).
   * decides which `models_dir` to use (packages.yml overrides manifest).
3. Loads models and macros from each package and registers them into the same namespace as local models.

---

## Using package models with `ref()`

### 1. Local path package

In `main_project/models/marts/mart_users_from_package.ff.sql`:

```jinja
{{ config(
    materialized='table',
    tags=[
        'example:packages_demo',
        'scope:mart',
        'engine:duckdb',
    ],
) }}

with base as (
    select
        email_domain,
        signup_date
    from {{ ref('users_base.ff') }}   -- defined in shared_package
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

* `users_base.ff` physically lives in `shared_package/models/staging/users_base.ff.sql`.
* From `main_project` you just `ref('users_base.ff')` — no package prefix.

### 2. Git-backed package

In `main_project/models/marts/mart_users_from_git_package.ff.sql`:

```jinja
{{ config(
    materialized='table',
    tags=[
        'example:packages_demo',
        'scope:mart',
        'engine:duckdb',
    ],
) }}

with base as (
    select
        email_domain,
        signup_date
    from {{ ref('users_base_git.ff') }}   -- defined in shared_package_git_remote
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

* `users_base_git.ff` comes from the **git-backed** package (`shared_package_git_remote`).
* Again, the model name is **global**: `ref('users_base_git.ff')` doesn’t mention the package.

**Important:** model names are globally unique across:

* main project
* all path packages
* all git packages

If two models use the same name, FFT fails with a clear “duplicate model name” error at load time.

---

## Shared macros from packages

The local package ships a macro file, for example:

```jinja
-- shared_package/models/macros/shared_utils.sql
{%- macro email_domain(expr) -%}
  lower(regexp_replace({{ expr }}, '^.*@', ''))
{%- endmacro -%}
```

`shared_package/models/staging/users_base.ff.sql` uses it:

```jinja
select
    user_id,
    email,
    {{ email_domain("email") }} as email_domain,
    signup_date
from raw_users;
```

Similarly, the git-backed package can provide its own macros (e.g. in `shared_utils_git.sql`).

Because macros from all packages and the main project share a single Jinja environment:

* package models can use package macros,
* main project models can also use package macros,
* name collisions are possible, and “last macro wins”.

Best practice:

* use tags like `pkg:shared_package`, `pkg:shared_package_git`, and/or
* namespace macros with `{% import 'macros/shared_utils.sql' as shared %}`.

---

## Data flow

The demo mirrors the basic_demo pipeline while splitting staging into packages.

**Local package:**

```text
(main_project) seeds/seed_users.csv
       │
       ├─ fft seed
       ▼
   seed_users (via sources.yml → crm.users)
       │
       ├─ shared_package/models/staging/users_base.ff.sql
       ▼
   users_base
       │
       ├─ main_project/models/marts/mart_users_from_package.ff.sql
       ▼
   mart_users_from_package
```

**Git package:**

```text
seed_users (same table)
       │
       ├─ shared_package_git_remote/models/staging/users_base_git.ff.sql
       ▼
   users_base_git
       │
       ├─ main_project/models/marts/mart_users_from_git_package.ff.sql
       ▼
   mart_users_from_git_package
```

The DAG will show both paths, with nodes annotated by their originating files, but **no special syntax** for package references.

---

## Running the demo

From the repo root:

```bash
cd examples/packages_demo/main_project
```

### 1. Configure DuckDB env

```bash
set -a; source env.dev_duckdb; set +a
# Sets:
#   FF_DUCKDB_PATH=.local/packages_demo.duckdb
#   FFT_ACTIVE_ENV=dev_duckdb
#   FF_ENGINE=duckdb
```

### 2. Inspect dependencies (local + git)

You can check the package resolver and git clone behavior with:

```bash
fft deps .
```

You should see output similar to:

```text
Project: /.../examples/packages_demo/main_project
Packages:
  - shared_package (0.1.0)
      kind:       path
      path:       /.../examples/packages_demo/shared_package
      models_dir: models  -> /.../shared_package/models
      status:     OK

  - shared_package_git (0.1.0)
      kind:       git
      git:        https://github.com/fftlabs/fastflowtransform.git
      rev:        abc1234deadbeef...     # resolved commit SHA
      subdir:     examples/packages_demo/shared_package_git_remote
      models_dir: models  -> /.../repo/examples/packages_demo/shared_package_git_remote/models
      status:     OK
```

If anything goes wrong (missing git, auth problems, bad ref, missing `project.yml`, missing `models_dir`, …) this command will fail with a targeted error message.

### 3. Run the full demo

```bash
make demo ENGINE=duckdb
```

This will:

1. **clean** – drop local artifacts and DuckDB file via `_scripts/cleanup_env.py`.
2. **seed** – `fft seed . --env dev_duckdb`:

   * loads `seeds/seed_users.csv` as `seed_users`.
3. **run** – `fft run . --env dev_duckdb` with example tags:

   * builds `users_base.ff` and `users_base_git.ff` from the two packages,
   * builds `mart_users_from_package` and `mart_users_from_git_package` in the main project.
4. **dag** – `fft dag . --env dev_duckdb --html`:

   * writes DAG HTML to `site/dag/index.html`.
5. **test** – `fft test . --env dev_duckdb`:

   * runs DQ tests from `project.yml` that reference both package and local tables.
6. **artifacts** – prints locations of `manifest.json`, `catalog.json`, `run_results.json`.

### 4. Inspect results

* DAG HTML:

  ```text
  examples/packages_demo/main_project/site/dag/index.html
  ```

* Artifacts:

  ```text
  examples/packages_demo/main_project/.fastflowtransform/target/manifest.json
  examples/packages_demo/main_project/.fastflowtransform/target/catalog.json
  examples/packages_demo/main_project/.fastflowtransform/target/run_results.json
  ```

* DuckDB file:

  ```text
  examples/packages_demo/main_project/.local/packages_demo.duckdb
  ```

* Package cache (including git clones):

  ```text
  examples/packages_demo/main_project/.fastflowtransform/packages/
    git/
      shared_package_git_https___github.com_fftlabs_fastflowtransform.git/
        repo/  # git checkout used for the package
  ```

---

## What this demo demonstrates

1. **Path-based packages**

   - `shared_package` is resolved via a local path.
   - Models/macros from `../shared_package` appear as if local to `main_project`.

2. **Git-based packages**

   - `shared_package_git` is resolved by:
     - cloning a remote repo,
     - checking out a specific ref/branch/tag/commit,
     - using a `subdir` as the package root.
   - The exact commit hash is written to `packages.lock.yml`.

3. **Versioning and constraints**

   - Package manifests (`project.yml`) expose `name` / `version` / `fft_version`.
   - `packages.yml` can constrain:
     - which **package versions** are allowed,
     - which **FFT core versions** a package supports (via `fft_version`).

4. **Diagnostics via `fft deps`**

   - Single command to see:
     - path vs git packages,
     - clones and refs,
     - resolved models_dir,
     - basic validity (missing dirs, bad refs, etc.).

5. **Separation of concerns**

   - Packages: reusable staging and macro logic.
   - Consumer project: seeds, sources, engine config, marts.

---

## Things to try

1. **Change the git ref**

   - In `packages.yml`, switch `ref: "main"` to a tag or SHA.
   - Run `fft deps .` and see which commit is used.
   - Re-run `make demo` and confirm behavior is still consistent.

2. **Break the git package**

   - Temporarily set `branch: "this-does-not-exist"`.
   - Run `fft deps .` and observe the “requested ref/branch/tag does not exist” error.
   - This is how you’d debug mis-typed branch/tag names in real life.

3. **Introduce a version mismatch**

   - Change `shared_package_git_remote/project.yml:version` to something incompatible with your `packages.yml:version` constraint.
   - Run `fft deps .` and see the “spec requires … but resolved version is …” error.
   - This is how you enforce “only use 0.1.x of this package” across projects.

4. **Add more packages**

   - Create another mini-package under `examples/packages_demo/another_package`.
   - Declare it in `main_project/packages.yml`.
   - Use its models in a new mart and watch them appear in the DAG.

---

## Summary

The updated `packages_demo` shows:

- How to **consume a local package** via `path` in `packages.yml`.
- How to **consume a git-based package** via `git` + `subdir` + `ref` and validate versions.
- How to use `fft deps` and `packages.lock.yml` to see exactly what code you’re running.
- How package models and macros integrate into your project as if everything lived in one tree.

Use this pattern to gradually factor out:

- shared staging layers (CRM, billing, web analytics),
- macro / utility libraries,
- and sharable marts—

all while keeping local projects thin, focused, and easy to reason about.