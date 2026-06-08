ALTER TABLE run_events ADD COLUMN event_type TEXT, ADD COLUMN event_payload JSONB;

UPDATE run_events
SET event_type = 'job_run.' || status
WHERE event_type IS NULL;

ALTER TABLE run_events ALTER COLUMN event_type SET NOT NULL;

ALTER TABLE job_runs
  ADD COLUMN error_code TEXT,
  ADD COLUMN error_message TEXT,
  ADD COLUMN started_at TIMESTAMPTZ,
  ADD COLUMN completed_at TIMESTAMPTZ,
  ADD COLUMN failed_at TIMESTAMPTZ;
