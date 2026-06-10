ALTER TABLE jobs
  ADD COLUMN schedule_type TEXT,
  ADD COLUMN scheduled_at_utc TIMESTAMPTZ,
  ADD COLUMN recurrence_rule TEXT,
  ADD COLUMN local_time TEXT,
  ADD COLUMN timezone_id TEXT,
  ADD COLUMN original_user_text TEXT;

UPDATE jobs
SET schedule_type = kind,
    timezone_id = 'UTC',
    recurrence_rule = CASE WHEN kind = 'recurring' THEN 'FREQ=DAILY' ELSE NULL END
WHERE timezone_id IS NULL;

ALTER TABLE jobs
  ALTER COLUMN timezone_id SET NOT NULL,
  ALTER COLUMN schedule_type SET NOT NULL;
