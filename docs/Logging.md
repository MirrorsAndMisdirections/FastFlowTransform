# Logging & Verbosity

FastFlowTransform exposes uniform logging controls across all CLI commands plus a dedicated SQL debug channel for tracing rendered SQL, dependency loading, and auxiliary queries.

## CLI Flags

- `-q` / `--quiet` → only errors (`ERROR`)
- *(default)* → concise warnings (`WARNING`)
- `-v` / `--verbose` → progress/info (`INFO`)
- `-vv` → full debug (`DEBUG`) including SQL debug output

`-vv` automatically flips on the SQL debug channel (same effect as `FFT_SQL_DEBUG=1`).

## SQL Debug Channel

Enable it to inspect Python-model inputs, dependency columns, and helper SQL emitted by data-quality checks:

```bash
# full debug (recommended)
fft run . -vv

# equivalent using the env var (legacy behaviour retained)
FFT_SQL_DEBUG=1 fft run .
```

## Usage Patterns

```bash
fft run . -q     # quiet (errors only)
fft run .        # default (concise)
fft run . -v     # verbose progress (model names, executor info)
fft run . -vv    # full debug + SQL channel
```

## Parallel Logging UX

- Each node emits start/end lines with duration, truncated name, and engine abbreviation (DUCK/PG/BQ/…).
- Output remains line-stable via a thread-safe log queue; per-level summaries trail each run.
- Failures still surface the familiar “error block” per node for quick diagnosis.

**Notes**

- SQL debug output routes through the `fastflowtransform.sql` logger; use `-vv` or `FFT_SQL_DEBUG=1` to reveal it.
- Existing projects do not need changes: the environment variable keeps working even without `-vv`.
