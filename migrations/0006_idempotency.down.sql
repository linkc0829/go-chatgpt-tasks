ALTER TABLE jobs
  DROP COLUMN IF EXISTS idempotency_scope,
  DROP COLUMN IF EXISTS side_effecting;

ALTER TABLE job_runs DROP COLUMN IF EXISTS idempotency_key;

DROP TABLE IF EXISTS idempotency_records;
