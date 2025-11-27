-- examples/hooks_demo/hooks/audit_run_end_spark.sql

-- Update the run-level audit row when the run finishes.

update _ff_run_audit
set
    finished_at = current_timestamp(),
    status      = {{ run.status | sql_literal }},
    row_count   = {{ run.row_count if run.row_count is not none else 'NULL' }},
    error       = {{ run.error if run.error is not none else 'NULL' }}
where run_id = {{ run.run_id | sql_literal }};
