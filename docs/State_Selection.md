
# State Selection — R1

Build only changed nodes or select by last run results.

## Changed Nodes

- `state:modified` — models that have changed since last cached fingerprint.
- `state:modified+` — the above plus all downstream dependents.

```bash
# First run populates cache
fft run examples/r1_demo --env dev --cache rw
# Touch files / change SQL → next run:
fft run examples/r1_demo --env dev --cache rw --select state:modified
fft run examples/r1_demo --env dev --cache rw --select state:modified+
````

## Result-based Selection

Use the last `run_results.json`:

* `result:ok`   — successful models (no warnings)
* `result:warn` — successful but with warnings
* `result:fail` — alias of `result:error`
* `result:error`— failed models

```bash
fft run examples/r1_demo --env dev --select result:error
```

### Artifacts

```
examples/r1_demo/.fastflowtransform/target/
├── manifest.json
├── run_results.json
└── catalog.json
```