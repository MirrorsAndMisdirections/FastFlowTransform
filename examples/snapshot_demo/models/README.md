# Models directory (snapshot_demo)

This demo shows how to:

- Clean seed data into a staging view.
- Build a small mart (`mart_users_by_domain`).
- Maintain two slowly-changing snapshot tables:

  - `users_clean_snapshot` – timestamp-based snapshot of the staging view.
  - `mart_users_by_domain_snapshot` – check-based snapshot of the mart.

The snapshot models use:

```jinja
materialized='snapshot'
strategy='timestamp' | 'check'
unique_key='...'
updated_at='...'
check_cols=['...']
```