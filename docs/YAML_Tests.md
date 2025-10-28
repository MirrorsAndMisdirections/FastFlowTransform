# YAML Tests (Schema-bound)

Schema-bound tests live in `models/*.yml` or `models/**/schema.yml` and complement (or replace) `project.yml`-based tests.

## Example

```yaml
# examples/r1_demo/models/users_enriched.yml
version: 2
models:
  - name: users_enriched
    description: "Adds gmail flag"
    columns:
      - name: id
        tests:
          - not_null: { severity: error }
          - unique
      - name: email
        tests:
          - not_null
          - accepted_values:
              values: ["a@example.com","b@example.com","c@gmail.com"]
              severity: warn
````

### Severities

* `error` → contributes to failures (exit code 2).
* `warn` → surfaced in summary as ❕, does not affect exit code.

### Run

```bash
fft test examples/r1_demo --env dev
# Select only tests tagged 'reconcile' (if present)
fft test examples/r1_demo --env dev --select tag:reconcile
```

### Output (excerpt)

```
Data Quality Summary
────────────────────
✅ not_null           users.id                               (3ms)
❌ unique             users.id                               (2ms)
   ↳ [unique] users.id: found 1 duplicate
❕ accepted_values     users_enriched.email                   (1ms)

Totals
──────
✓ passed: 2
✗ failed: 1
! warnings: 1
```