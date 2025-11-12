# Troubleshooting & Error Codes

Use this checklist when FastFlowTransform commands misbehave. Each item points to the quickest fix plus the relevant CLI options.

## Quick Fixes

- **DuckDB seeds not visible** → ensure `FF_DUCKDB_PATH` (or the profile path) is identical for `seed`, `run`, `dag`, and `test`. If you configure `FF_DUCKDB_SCHEMA` / `FF_DUCKDB_CATALOG`, keep them consistent across commands so unqualified references resolve to the right namespace.
- **Postgres connection refused** → confirm `FF_PG_DSN`, container status (`docker ps`), and that port `5432` is open.
- **BigQuery permissions** → set `GOOGLE_APPLICATION_CREDENTIALS` and match dataset/location to your profile.
- **HTML docs missing** → run `fft dag <project> --html` and open `<project>/docs/index.html`.
- **Unexpected test failures** → inspect rendered SQL in CLI output, refine selection via `--select`, refresh seeds if needed.
- **Dependency table not found in utests** → provide all physical upstream relations in the YAML spec.

## Error Codes

| Type                      | Class/Source              | Exit | Notes                                                   |
|---------------------------|---------------------------|------|---------------------------------------------------------|
| Missing dependency        | `DependencyNotFoundError` | 1    | Per-node list; tips for `ref()` / names                |
| Cycle in DAG              | `ModelCycleError`         | 1    | “Cycle detected among nodes: …”                        |
| Model execution (KeyError)| `cli.py` → formatted block| 1    | Inspect columns, use `relation_for(dep)` as keys       |
| Data quality failures     | `cli test` → summary      | 2    | Totals section prints passed/failed counts             |
| Unknown/unexpected        | generic                   | 99   | Optional trace via `FFT_TRACE=1`                       |

Error types map to the classes documented in `docs/Technical_Overview.md#core-modules` and the CLI source.
