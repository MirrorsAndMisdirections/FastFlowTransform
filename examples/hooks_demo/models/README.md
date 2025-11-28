# Models directory (hooks_demo)

Models:

- `staging/events_clean.ff.sql`
  - Minimal staging over `seed_events` with proper typing.
  - Tagged `example:hooks_demo` and `scope:staging`.

- `marts/mart_events_daily.ff.sql`
  - Simple aggregation (events per day).
  - Tagged `example:hooks_demo` and `scope:mart`.

Hooks:

- Run hooks (`on_run_start` / `on_run_end`) create and update `_ff_run_audit`.
- Model hooks (`before_model` / `after_model`) write rows into `_ff_model_audit` whenever a model
  with `tag:example:hooks_demo` is executed.

The hooks are configured centrally in `project.yml` so the models stay clean and portable.
