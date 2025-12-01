# shared_package models

This folder contains reusable models and macros that can be imported
into any FastFlowTransform project via `packages.yml`.

Contents:

- `macros/shared_utils.sql` – SQL Jinja macros (e.g. `email_domain(expr)`).
- `staging/users_base.ff.sql` – a simple staging model that normalizes users
  and derives `email_domain`, intended to be referenced as:

    {{ ref('users_base.ff') }}

from a consuming project.
