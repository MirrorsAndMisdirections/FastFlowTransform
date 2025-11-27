-- examples/hooks_demo/hooks/audit_run_end.sql

-- Update the run-level audit row when the run finishes.

-- Default: update the single row for this run_id
update _ff_run_audit
set
    finished_at = current_timestamp,
    status      = {{ run.status | sql_literal }},
    row_count   = {{ run.row_count if run.row_count is not none else 'NULL' }},
    error       = NULL
where run_id = {{ run.run_id | sql_literal }};
