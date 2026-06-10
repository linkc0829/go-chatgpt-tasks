ALTER TABLE jobs ADD COLUMN tenant_id UUID, ADD COLUMN user_id UUID;
ALTER TABLE job_runs ADD COLUMN tenant_id UUID;
ALTER TABLE run_events ADD COLUMN tenant_id UUID, ADD COLUMN job_id UUID;

UPDATE jobs
SET tenant_id = '00000000-0000-0000-0000-0000000000aa',
    user_id = '00000000-0000-0000-0000-0000000000bb'
WHERE tenant_id IS NULL;

UPDATE job_runs jr
SET tenant_id = j.tenant_id
FROM jobs j
WHERE jr.job_id = j.id AND jr.tenant_id IS NULL;

UPDATE run_events re
SET tenant_id = jr.tenant_id,
    job_id = jr.job_id
FROM job_runs jr
WHERE re.job_run_id = jr.id AND re.tenant_id IS NULL;

ALTER TABLE jobs ALTER COLUMN tenant_id SET NOT NULL, ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE job_runs ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE run_events ALTER COLUMN tenant_id SET NOT NULL, ALTER COLUMN job_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_tenant ON jobs (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_runs_tenant ON job_runs (tenant_id, created_at DESC);
