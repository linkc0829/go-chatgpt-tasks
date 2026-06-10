ALTER TABLE jobs
  ADD COLUMN parent_job_id UUID REFERENCES jobs(id),
  ADD COLUMN trigger_on_parent_status TEXT;

CREATE INDEX idx_jobs_parent ON jobs (parent_job_id);
