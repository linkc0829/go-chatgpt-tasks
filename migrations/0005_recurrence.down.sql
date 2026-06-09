ALTER TABLE jobs
  DROP COLUMN IF EXISTS original_user_text,
  DROP COLUMN IF EXISTS timezone_id,
  DROP COLUMN IF EXISTS local_time,
  DROP COLUMN IF EXISTS recurrence_rule,
  DROP COLUMN IF EXISTS scheduled_at_utc,
  DROP COLUMN IF EXISTS schedule_type;
