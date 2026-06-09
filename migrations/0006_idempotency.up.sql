CREATE TABLE idempotency_records (
  idempotency_key TEXT PRIMARY KEY,
  job_run_id UUID NOT NULL,
  handler_name TEXT NOT NULL,
  status TEXT NOT NULL,
  response_hash TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE job_runs ADD COLUMN idempotency_key TEXT;
UPDATE job_runs SET idempotency_key = job_id::text || ':' || sequence::text WHERE idempotency_key IS NULL;
ALTER TABLE job_runs ALTER COLUMN idempotency_key SET NOT NULL;

ALTER TABLE jobs
  ADD COLUMN side_effecting BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN idempotency_scope TEXT NOT NULL DEFAULT 'job_run';
