# R1 Demo

Minimal project showing:
- Incremental model (`fct_events_inc.ff.sql`)
- YAML tests (`models/users_enriched.yml`)
- State selection (`state:modified`, `result:*`)

## DuckDB (local)

```bash
make -C examples/r1_demo seed
make -C examples/r1_demo run
make -C examples/r1_demo dag
````

Incremental-only:

```bash
make -C examples/r1_demo inc
```

## Postgres (optional)

Set `FF_PG_DSN` and `FF_PG_SCHEMA`, then:

```bash
make -C examples/r1_demo pg-seed
make -C examples/r1_demo pg-run
```

## Expected Artifacts

```
examples/r1_demo/.fastflowtransform/target/
├── manifest.json
├── run_results.json
└── catalog.json
```

## Sample Output (excerpt)

```
✔ L00 [DUCK] users.ff (120ms)
✔ L01 [DUCK] users_enriched (35ms)
✔ L01 [DUCK] fct_events_inc.ff (41ms)

Data Quality Summary
────────────────────
✅ not_null           users.email                        (2ms)
❕ accepted_values    users_enriched.email               (1ms)