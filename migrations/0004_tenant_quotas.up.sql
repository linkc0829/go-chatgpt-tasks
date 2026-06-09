CREATE TABLE tenant_quotas (
  tenant_id UUID PRIMARY KEY,
  max_jobs_per_hour INTEGER NOT NULL,
  max_active_recurring_jobs INTEGER NOT NULL,
  max_concurrent_runs INTEGER NOT NULL,
  max_daily_llm_cost_cents INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
