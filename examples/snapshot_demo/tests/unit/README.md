# Unit tests (snapshot_demo)

Add YAML unit specs for fine-grained expectations, for example:

- that `users_clean_snapshot` has at least N rows
- that each `user_id` has at most one open (`ff_valid_to IS NULL`) row

Invoke via:

```bash
fft utest . --env dev_duckdb
```