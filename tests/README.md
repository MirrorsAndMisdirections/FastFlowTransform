# Tests

We bucket tests into a few categories:

- `tests/unit/` contain fast, dependency-free unit tests.
- `tests/executors/` exercise individual engines (DuckDB, Postgres, etc.).
- `tests/integration/` cover end-to-end CLI and workflow scenarios.

## Pytest Markers

We use markers to make selective runs easy:

| Marker    | Meaning                                    |
|-----------|--------------------------------------------|
| `duckdb`  | Requires DuckDB executor/fixtures          |
| `postgres`| Requires Postgres backend                   |
| `cli`     | Runs the CLI or Typer commands              |
| `artifacts`| Exercises artifact generation helpers       |
| `render`  | Exercises render-time template helpers       |
| `schema`  | Exercises schema parsing/validation helpers  |
| `streaming`| Exercises streaming/sessionizer features   |
| `slow`    | Slower end-to-end scenarios                 |
| `http`    | Exercises the HTTP API client/cache         |

Example selective runs:

```bash
pytest -m duckdb
pytest -m "cli and not slow"
```

Markers are declared in `pytest.ini`. Remember to tag new tests appropriately.
