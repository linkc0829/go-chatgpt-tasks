CREATE TABLE tenant_llm_daily_cost (
  tenant_id UUID NOT NULL,
  cost_date TEXT NOT NULL,
  cost_cents INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, cost_date)
);
