# Auto-Docs & Lineage

FastFlowTransform can generate a lightweight documentation site (DAG + model detail pages) plus an optional JSON manifest for external tooling.

## Commands

```bash
# Classic
fft dag . --env dev --html

# Convenience wrapper (loads schema + descriptions + lineage, can emit JSON)
fft docgen . --env dev --out site/docs --emit-json site/docs/docs_manifest.json
```

Add `--open-source` if you want the default browser to open the rendered `index.html` immediately.

## Descriptions

Descriptions can be provided in YAML (`project.yml`) and/or Markdown files. Markdown has higher priority.

YAML in `project.yml`:

```yaml
docs:
  models:
    users.ff:
      description: "Raw users table imported from CRM."
      columns:
        id: "Primary key."
        email: "User email address."
    users_enriched:
      description: "Adds gmail flag."
      columns:
        is_gmail: "True if email ends with @gmail.com"
```

Markdown overrides YAML when present:

```
<project>/docs/models/<model>.md
<project>/docs/columns/<relation>/<column>.md
```

Optional front matter is ignored for now (title/tags may be used later).

## Column Lineage

- SQL models: expressions like `col`, `alias AS out`, `upper(u.email) AS email_upper)` are parsed; `u` must come from a `FROM ... AS u` clause that resolves to a relation. Functions mark lineage as *transformed*.
- Python (pandas) models: simple patterns like `rename`, `out["x"] = df["y"]`, `assign(x=...)` are recognized.
- Override hints in YAML when the heuristic is insufficient:

```yaml
docs:
  models:
    mart_orders_enriched:
      lineage:
        email_upper:
          from: [{ table: users, column: email }]
          transformed: true
```

## JSON Manifest

The optional manifest (via `--emit-json`) includes models, relations, descriptions, columns (with nullable/dtype), and lineage per column—useful for custom doc portals or CI checks.

## Notes

- Schema introspection currently supports DuckDB and Postgres. For other engines, the Columns card may be empty.
- Lineage is optional; when uncertain, entries fall back to “unknown” and never fail doc generation.
