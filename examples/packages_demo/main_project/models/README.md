# Main project models

This project keeps its own models under `models/` and consumes an additional
staging model `users_base.ff` from `../shared_package` via `packages.yml`.

Local models:

- `marts/mart_users_from_package.ff.sql` – aggregates over the packaged
  `users_base.ff` model.
