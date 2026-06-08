ALTER TABLE job_runs
  DROP COLUMN IF EXISTS failed_at,
  DROP COLUMN IF EXISTS completed_at,
  DROP COLUMN IF EXISTS started_at,
  DROP COLUMN IF EXISTS error_message,
  DROP COLUMN IF EXISTS error_code;

ALTER TABLE run_events
  DROP COLUMN IF EXISTS event_payload,
  DROP COLUMN IF EXISTS event_type;
