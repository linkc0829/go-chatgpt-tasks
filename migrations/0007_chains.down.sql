DROP INDEX IF EXISTS idx_jobs_parent;

ALTER TABLE jobs
  DROP COLUMN IF EXISTS trigger_on_parent_status,
  DROP COLUMN IF EXISTS parent_job_id;
