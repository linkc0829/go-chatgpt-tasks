Source: https://app.notion.com/p/Production-Improvement-Spec-ChatGPT-Task-Scheduler-3798b5e3718e818ebc4ded46211600f4

Scope decision (from /qrspi:1_question): Whole spec — all 6 improvement areas.

---

# 📐 Production Improvement Spec — ChatGPT Task Scheduler

## Spec Purpose
This page turns the Week 3 production-gap discussion into an implementation spec for improving the original **ChatGPT Task Scheduler** prototype.
The original system already proves the core architecture:
```plain text
LLM → MCP Server → Scheduler API → Queue → Worker
```
This spec defines the next production-oriented improvements:
```plain text
Timezone correctness
Job dependency support
Observability
Tenant isolation
Idempotent execution
LLM reliability controls
```

## 0. Implementation Status (current `main`)
The prototype has advanced past the description in §1. Baseline against what already exists on `main` before planning.

### Already implemented — do not re-scope as new
- `job_runs` and `run_events` tables exist, with the `time_bucket` hourly-partition column and a `(time_bucket, status, scheduled_at)` due-index.
- The watcher already scans `job_runs` by `time_bucket` and enqueues due runs (§15 Step 4 is done).
- Retry + dead-letter is implemented in the worker (`max_attempts = 3`), with a `RunEvent` emitted on every status transition.
- A recurring watcher creates the next run after a run reaches a terminal state.
- At-least-once delivery protection comes from Redis Streams consumer groups (claim / reclaim / ack), not from DB row locks.

### Genuinely greenfield — the real production gap
- **Multi-tenancy:** no `tenant_id` / `user_id` exists on any table, service, or query. Largest item; chosen first slice.
- **Timezone correctness:** recurrence is currently a `time.Duration` interval (`interval_seconds`), not `local_time + timezone_id + recurrence_rule`. Moving to timezone-aware recurrence replaces the interval model.
- **Idempotency:** `idempotency_key` + `idempotency_records` table and the idempotent handler contract.
- **LLM reliability:** the executor is a stub, so all of §11 is new.
- **Job chains:** `parent_job_id` is new.

### Migration mode
Post-deploy: new schema ships as stacked migrations (`0002+`) using nullable-add → backfill → `NOT NULL`, per §15. `job_runs` / `run_events` already exist, so §15 Step 3 only creates the remaining new tables (`tenant_quotas`, `idempotency_records`).

### Corrections applied to this spec
- All timestamp columns use `TIMESTAMPTZ` (not `TIMESTAMP`) to match the repo and the timezone-correctness goal.
- The `locked_by` / `locked_until` columns proposed on `job_runs` (§5.2) are redundant with the existing Redis consumer-group locking and are deferred — see the note there.

## 1. Problem Statement
The current prototype can create, list, check, cancel, schedule, and execute simple jobs. It is good enough to demonstrate the MCP + scheduler flow, but it is not yet safe for production because:
- Recurring jobs do not fully model user-local timezones.
- Jobs are independent and cannot express ordered workflows.
- Failures are hard to debug without structured events and dashboards.
- One tenant can create too many jobs and delay other tenants.
- Queue delivery is at-least-once, so duplicate execution is possible.
- LLM calls can timeout, produce invalid output, hallucinate, or exceed cost limits.

## 2. Goals
### Functional goals
- Support timezone-safe one-time and recurring jobs.
- Support simple job chains: `A → B → C`.
- Emit structured lifecycle events for every job run.
- Enforce tenant-level quotas and rate limits.
- Make side-effecting jobs idempotent by default.
- Add validation, retry, timeout, and cost controls for LLM-based jobs.

### System goals
- Preserve the original MCP interface style: namespace + action verb.
- Keep the scheduler usable by non-MCP clients, such as web, mobile, CLI, or other agents.
- Keep the first implementation simple: job chain before DAG, fair scheduling before complex weighted scheduling.
- Make correctness observable through logs, metrics, events, and dashboards.

## 3. Non-goals
These are intentionally out of scope for the first production-improvement pass:
- Full DAG workflow engine.
- Distributed exactly-once execution guarantee.
- Complex workflow compensation engine.
- User-facing visual workflow builder.
- Advanced paid-tier weighted scheduling.
- Cross-region active-active scheduling.

## 4. Proposed Architecture
### Current architecture
```plain text
MCP Server
  → Scheduler API
    → DB
    → Watcher
      → Queue
        → Worker
```
### Improved architecture
```plain text
MCP Server
  → Scheduler API
    → Tenant quota check
    → Job / JobRun / RunEvent DB
    → Watcher with timezone-aware next-run calculation
      → Tenant-aware queues
        → Fair scheduler / worker pool
          → Idempotent handler execution
          → LLM validation / retry / cost guard
          → RunEvent emission
          → Child job trigger
```

## 5. Data Model Changes
### 5.1 Job
Represents the user's scheduled task definition.
```sql
CREATE TABLE jobs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  user_id UUID NOT NULL,

  description TEXT NOT NULL,
  job_type TEXT NOT NULL,
  status TEXT NOT NULL,

  schedule_type TEXT NOT NULL,
  scheduled_at_utc TIMESTAMPTZ NULL,
  recurrence_rule TEXT NULL,

  local_time TEXT NULL,
  timezone_id TEXT NOT NULL,
  original_user_text TEXT NULL,

  parent_job_id UUID NULL,
  trigger_on_parent_status TEXT NULL,

  side_effecting BOOLEAN NOT NULL DEFAULT false,
  idempotency_scope TEXT NOT NULL DEFAULT 'job_run',

  max_retries INTEGER NOT NULL DEFAULT 3,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
```
### Field notes
- `timezone_id` must be an IANA timezone ID, such as `Asia/Taipei` or `America/New_York`.
- `local_time` preserves the user's intended local execution time, such as `08:00`.
- `scheduled_at_utc` is the next concrete execution time used by the watcher.
- `recurrence_rule` stores recurrence intent, such as daily, weekly, or cron-like recurrence.
- `parent_job_id` enables simple job chains.
- `side_effecting` marks jobs where duplicate execution is dangerous.

### 5.2 JobRun
Represents one concrete execution attempt for a job.
```sql
CREATE TABLE job_runs (
  id UUID PRIMARY KEY,
  job_id UUID NOT NULL,
  tenant_id UUID NOT NULL,

  scheduled_at_utc TIMESTAMPTZ NOT NULL,
  time_bucket TEXT NOT NULL,
  status TEXT NOT NULL,

  attempt_count INTEGER NOT NULL DEFAULT 0,
  idempotency_key TEXT NOT NULL,

  locked_by TEXT NULL,
  locked_until TIMESTAMPTZ NULL,

  started_at TIMESTAMPTZ NULL,
  completed_at TIMESTAMPTZ NULL,
  failed_at TIMESTAMPTZ NULL,

  error_code TEXT NULL,
  error_message TEXT NULL,

  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
```
### Field notes
- `time_bucket` keeps the original partitioning optimization.
- `idempotency_key` is used by handlers to dedupe side effects.
- `locked_by` / `locked_until` are **deferred**: worker-death recovery is currently handled by Redis Streams consumer-group reclaim (min-idle), not DB locks. Add these columns only if execution moves off the Redis-claim model.
- `attempt_count` supports retry and DLQ decisions.

### 5.3 RunEvent
Append-only event log for debugging, monitoring, and audit.
```sql
CREATE TABLE run_events (
  id UUID PRIMARY KEY,
  job_run_id UUID NOT NULL,
  job_id UUID NOT NULL,
  tenant_id UUID NOT NULL,

  event_type TEXT NOT NULL,
  event_payload JSONB NULL,

  created_at TIMESTAMPTZ NOT NULL
);
```
Example event types:
```plain text
job.created
job_run.created
job_run.enqueued
job_run.started
job_run.succeeded
job_run.failed
job_run.retry_scheduled
job_run.dlq
job.cancelled
llm.timeout
llm.validation_failed
quota.rejected
child_job.enqueued
```

### 5.4 TenantQuota
Tracks and enforces tenant limits.
```sql
CREATE TABLE tenant_quotas (
  tenant_id UUID PRIMARY KEY,
  max_jobs_per_hour INTEGER NOT NULL,
  max_active_recurring_jobs INTEGER NOT NULL,
  max_concurrent_runs INTEGER NOT NULL,
  max_daily_llm_cost_cents INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
```

### 5.5 IdempotencyRecord
Records completed side effects.
```sql
CREATE TABLE idempotency_records (
  idempotency_key TEXT PRIMARY KEY,
  job_run_id UUID NOT NULL,
  handler_name TEXT NOT NULL,
  status TEXT NOT NULL,
  response_hash TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
```

## 6. MCP Tool Changes
Preserve action-verb tool naming.
### Existing tools
```plain text
task.create
task.list
task.status
task.cancel
```
### Improved `task.create`
Add timezone, recurrence, dependency, and safety fields.
```json
{
  "description": "Summarize tech news every morning",
  "job_type": "generic_llm",
  "schedule_type": "recurring",
  "local_time": "08:00",
  "timezone_id": "Asia/Taipei",
  "recurrence_rule": "FREQ=DAILY",
  "parent_job_id": null,
  "trigger_on_parent_status": null,
  "side_effecting": false
}
```
### Improved `task.status`
Return both job-level and latest-run information.
```json
{
  "job_id": "...",
  "job_status": "active",
  "latest_run": {
    "job_run_id": "...",
    "status": "succeeded",
    "scheduled_at_utc": "2026-06-08T00:00:00Z",
    "started_at": "2026-06-08T00:00:03Z",
    "completed_at": "2026-06-08T00:00:12Z",
    "attempt_count": 1
  },
  "next_run_at_utc": "2026-06-09T00:00:00Z",
  "timezone_id": "Asia/Taipei"
}
```
### New optional tool: `task.runs`
Lists recent runs for one job.
```json
{
  "job_id": "...",
  "limit": 20
}
```
### New optional tool: `task.events`
Lists event history for a job run.
```json
{
  "job_run_id": "..."
}
```

## 7. Scheduler API Changes
The MCP server should remain a client of the scheduler service. The scheduler API should expose the same production concepts to any client.
### Create job
```javascript
POST /jobs
```
Responsibilities:
- Validate timezone ID.
- Validate recurrence rule.
- Enforce tenant quota.
- Compute first `scheduled_at_utc`.
- Create `Job`.
- Create first `JobRun` if applicable.
- Emit `job.created` and `job_run.created` events.
### List jobs
```javascript
GET /jobs?tenant_id=...&status=...
```
### Get job status
```javascript
GET /jobs/{job_id}
```
### Cancel job
```javascript
POST /jobs/{job_id}/cancel
```
Responsibilities:
- Mark job as cancelled.
- Mark pending runs as cancelled.
- Decide whether child jobs should also be cancelled.
- Emit `job.cancelled` event.
### List job runs
```javascript
GET /jobs/{job_id}/runs
```
### List run events
```javascript
GET /runs/{job_run_id}/events
```

## 8. Timezone-Safe Scheduling
### Requirement
The system must preserve the user's intended local time.
For example:
```plain text
User intent: every day at 08:00 America/New_York
```
This must continue to mean 8 AM local time before and after DST changes.
### Storage rule
Store:
```plain text
local_time + timezone_id + recurrence_rule
```
Do not store only:
```plain text
UTC offset
```
### Watcher behavior
For each active recurring job:
1. Read `local_time`, `timezone_id`, and `recurrence_rule`.
2. Compute the next local occurrence.
3. Convert that occurrence to UTC.
4. Create a `JobRun` with `scheduled_at_utc` and `time_bucket`.
5. Watcher scans due `JobRun` records by current `time_bucket`.
6. Watcher enqueues due runs.
### DST policy
Initial policy:
- If local time is skipped by DST, run at the next valid local time.
- If local time occurs twice, run once at the first occurrence.
- Record the decision in `RunEvent` for auditability.

## 9. Job Chain Support
### Requirement
Support simple linear dependencies before building a full DAG engine.
Example:
```plain text
Job A: check exchange rate
Job B: place order

Job B.parent_job_id = Job A.id
Job B.trigger_on_parent_status = succeeded
```
### Worker behavior
When a parent job run reaches a terminal state:
```plain text
succeeded | failed | cancelled
```
The worker checks for child jobs where:
```plain text
child.parent_job_id = parent.id
child.trigger_on_parent_status = parent.status
```
If matched:
1. Create child `JobRun`.
2. Enqueue child run.
3. Emit `child_job.enqueued` event.
### Cancellation policy
Initial policy:
- Cancelling a parent cancels pending child jobs by default.
- Running child jobs are not force-stopped in v1.
- This behavior should be documented in `task.cancel` output.

## 10. Idempotent Execution
### Requirement
The system should assume the queue can deliver the same message more than once.
The guarantee is:
```plain text
At-least-once delivery + idempotent handlers
```
### Handler contract
Every handler receives:
```json
{
  "job_run_id": "...",
  "idempotency_key": "...",
  "tenant_id": "...",
  "job_type": "...",
  "payload": {}
}
```
### Side-effecting handler behavior
Before performing side effects:
1. Check `idempotency_records` by `idempotency_key`.
2. If already completed, return the recorded result or safe duplicate response.
3. If not completed, insert an in-progress record.
4. Perform side effect.
5. Mark idempotency record as completed.
### Examples of side-effecting jobs
```plain text
send_email
place_order
charge_payment
post_message
create_ticket
```
### Examples of mostly read-only jobs
```plain text
summarize_news
check_github_prs
fetch_exchange_rate
```

## 11. LLM Reliability Controls
### Requirement
LLM calls must be treated as unreliable external dependencies.
Failure modes:
- Timeout
- Rate limit
- Invalid JSON
- Schema mismatch
- Hallucinated content
- Tool mismatch
- Excessive tokens
- Cost spike
### LLM execution policy
Each LLM-backed job type should define:
```json
{
  "timeout_seconds": 30,
  "max_retries": 2,
  "max_input_tokens": 8000,
  "max_output_tokens": 1000,
  "max_cost_cents": 20,
  "output_schema": "..."
}
```
### Validation rule
The worker must validate LLM output before marking the run as succeeded.
If validation fails:
1. Emit `llm.validation_failed`.
2. Retry if retry budget remains.
3. Otherwise mark run as failed.
4. Never silently store invalid output as success.
### Cost guard
Before running an LLM job:
1. Estimate cost from model and token budget.
2. Check tenant daily LLM cost quota.
3. Reject or defer if the tenant exceeds the quota.
4. Emit `quota.rejected` or `quota.deferred`.

## 12. Multi-Tenant Isolation
### Requirement
One tenant must not be able to delay all other tenants.
### Quota checks
Apply at task creation:
- Max jobs per hour.
- Max active recurring jobs.
- Max concurrent runs.
- Max daily LLM cost.
### Queue strategy
Start with logical per-tenant queues or queue groups.
```plain text
tenant_queue:A
tenant_queue:B
tenant_queue:C
```
Workers should poll fairly:
```plain text
A → B → C → A → B → C
```
### Over-quota behavior
Initial policy:
- API rejects new jobs when hard quota is exceeded.
- Existing jobs may be moved to low-priority execution if soft quota is exceeded.
- Rejections must be visible to users and recorded in events.

## 13. Observability
### Requirement
Production launch requires dashboards, structured events, and alerts.
### Metrics
#### Job health
- Job success rate.
- Failure rate by job type.
- Average execution delay.
- P95 / P99 execution delay.
- Retry count.
- DLQ count.
#### Queue health
- Queue depth.
- Oldest message age.
- Worker consumption rate.
- Worker error rate.
#### LLM health
- LLM latency.
- Timeout rate.
- Validation failure rate.
- Tokens per job.
- Cost per job.
- Daily / weekly / monthly cost.
#### Tenant health
- Jobs created per tenant.
- Active recurring jobs per tenant.
- Queue depth per tenant.
- Cost per tenant.
- Quota rejection count.
### Alerts
- Job success rate below target.
- Queue depth above threshold.
- Oldest message age above SLA.
- DLQ count above threshold.
- Consecutive failures for same job type.
- LLM timeout spike.
- LLM cost spike.
- Tenant quota abuse detected.

## 14. Run Lifecycle
### Normal successful run
```plain text
job_run.created
→ job_run.enqueued
→ job_run.started
→ handler.executed
→ job_run.succeeded
→ child_job.enqueued, optional
→ next_recurring_run.created, optional
```
### Retryable failed run
```plain text
job_run.started
→ handler.failed
→ job_run.retry_scheduled
→ job_run.enqueued
```
### Terminal failed run
```plain text
job_run.started
→ handler.failed
→ job_run.failed
→ job_run.dlq
```

## 15. Migration Plan
### Step 1 — Add nullable columns
Add production fields as nullable first:
```plain text
timezone_id
local_time
recurrence_rule
parent_job_id
trigger_on_parent_status
side_effecting
idempotency_scope
```
### Step 2 — Backfill existing jobs
For existing jobs:
```plain text
timezone_id = tenant default timezone, fallback UTC
side_effecting = false
idempotency_scope = job_run
```
### Step 3 — Create new tables
Create:
```plain text
job_runs
run_events
tenant_quotas
idempotency_records
```
### Step 4 — Move watcher to JobRun scanning
Original watcher scans jobs directly. Improved watcher scans `job_runs` by `time_bucket`.
### Step 5 — Enable production behavior by job type
Roll out in this order:
1. Timezone-safe recurring jobs.
2. RunEvent emission.
3. Idempotency keys.
4. Tenant quota checks.
5. Job chain support.
6. LLM validation and cost guard.

## 16. Implementation Plan
### Phase 1 — Data model and watcher correctness
- [x] Add `JobRun` table. *(done)*
- [x] Add `RunEvent` table. *(done)*
- [ ] Add timezone fields to `Job`.
- [ ] Implement IANA timezone validation.
- [ ] Compute next UTC run from local schedule.
- [x] Update watcher to scan `JobRun` by `time_bucket`. *(done)*
- [x] Emit lifecycle events. *(done — status-only today; enrich with **`event_type`** + **`event_payload`**)*
### Phase 2 — Worker reliability
- [ ] Add `idempotency_key` to each `JobRun`.
- [ ] Add `IdempotencyRecord` table.
- [ ] Update handler interface to include idempotency key.
- [ ] Add retry policy per job type. *(global **`max_attempts = 3`** exists; per-job-type override is new)*
- [x] Add DLQ behavior after max retries. *(done)*
- [ ] Add duplicate-run tests.
### Phase 3 — Tenant isolation
- [ ] Add `TenantQuota` table.
- [ ] Enforce quota at job creation.
- [ ] Add tenant queue metadata.
- [ ] Implement fair worker polling.
- [ ] Add quota rejection events.
- [ ] Add per-tenant dashboard metrics.
### Phase 4 — Job chains
- [ ] Add parent-child relationship fields.
- [ ] Create child run after parent terminal status.
- [ ] Implement cancellation propagation for pending children.
- [ ] Add `task.runs` and `task.events` tools.
- [ ] Add chain visibility to status response.
### Phase 5 — LLM reliability
- [ ] Define per-job-type LLM policy.
- [ ] Add timeout handling.
- [ ] Add schema validation.
- [ ] Add validation-failure retry.
- [ ] Add token and cost budget checks.
- [ ] Emit LLM-specific events and metrics.

## 17. Acceptance Criteria
### Timezone
- [ ] A daily task at `08:00 Asia/Taipei` runs at 8 AM Taiwan time.
- [ ] A daily task at `08:00 America/New_York` remains 8 AM local time across DST changes.
- [ ] The system rejects invalid timezone IDs.
- [ ] Status output shows both local schedule and next UTC run.
### JobRun and events
- [ ] Every scheduled execution is represented as a `JobRun`.
- [ ] Every status transition emits a `RunEvent`.
- [ ] Failed runs include error code and message.
- [ ] Run history is queryable by job ID.
### Idempotency
- [ ] Duplicate queue messages do not duplicate side effects.
- [ ] Side-effecting handlers use `idempotency_key`.
- [ ] Duplicate detection is visible in logs or events.
### Tenant isolation
- [ ] Tenant quota is enforced at task creation.
- [ ] One tenant creating many jobs does not block unrelated tenants.
- [ ] Queue depth and cost are visible per tenant.
### Job chains
- [ ] A child job can run after parent success.
- [ ] A child job does not run before its dependency is satisfied.
- [ ] Cancelling a parent cancels pending child jobs.
### LLM reliability
- [ ] LLM output is validated before success.
- [ ] Timeout and retry policies are enforced.
- [ ] Cost budget is checked before LLM execution.
- [ ] Invalid LLM output never becomes a successful job result.

## 18. Test Plan
### Unit tests
- Timezone conversion.
- DST skipped local time.
- DST duplicated local time.
- Time bucket generation.
- Idempotency record lookup.
- Quota calculation.
- LLM output schema validation.
### Integration tests
- Create recurring task through `task.create`.
- Watcher creates and enqueues due `JobRun`.
- Worker executes and marks run succeeded.
- Worker retries failed run.
- Worker sends run to DLQ after max retries.
- Parent job success triggers child job.
- Over-quota tenant receives rejection.
### Failure tests
- Worker crashes after receiving queue message.
- Same queue message delivered twice.
- LLM returns invalid JSON.
- LLM times out.
- Tenant creates burst of jobs.
- Queue backlog grows beyond alert threshold.

## 19. Design Tradeoffs
### Job chain instead of DAG
Use job chain first because most early workflows are linear. DAG support should wait until the product needs fan-out or fan-in.
### At-least-once instead of exactly-once
Exactly-once is not realistic across queues, workers, databases, and external APIs. The practical design is at-least-once delivery with idempotent handlers.
### Timezone ID instead of UTC offset
Timezone ID preserves user intent across DST. Offset only captures one moment in time.
### RunEvent table instead of logs only
Logs are useful for operators, but `RunEvent` gives the product and MCP tools a queryable execution history.
### Tenant fair scheduling before weighted scheduling
Round-robin or simple fair scheduling is easier to reason about. Weighted scheduling can be added later for paid tiers.

## 20. Updated Definition of Done
The original prototype is considered production-improved when:
- [ ] Jobs are scheduled using timezone-safe local intent.
- [ ] Watcher operates on `JobRun`, not only `Job`.
- [ ] Workers are retry-safe and idempotency-aware.
- [ ] Every run has structured events.
- [ ] Basic dashboards and alerts exist.
- [ ] Tenant quota prevents noisy-neighbor behavior.
- [ ] Simple job chains are supported.
- [ ] LLM jobs have timeout, validation, retry, and cost controls.

## 21. Summary
This improvement keeps the original design simple but makes it production-oriented.
The key shift is:
```plain text
From: schedule a job and execute it later
To: safely manage job runs across timezones, tenants, retries, dependencies, and unreliable LLM calls
```
The recommended implementation order is:
```plain text
1. JobRun + RunEvent ........... DONE (enrich RunEvent payloads)
2. Tenant identity + quota ..... NEXT (largest blast radius)
3. Timezone-safe recurrence
4. Idempotency records ......... (retry + DLQ already done)
5. Job chain
6. LLM validation + timeout + cost guard
```
