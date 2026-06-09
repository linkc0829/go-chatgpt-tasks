# Implementation Plan

## Overview
Make the task scheduler production-oriented: tenant/user-scoped jobs behind an authenticated
HTTP API, typed lifecycle events + metrics, tenant quotas, timezone-safe recurrence,
idempotent execution, linear job chains, and an LLM reliability layer behind a port.
Seven phases, each a shippable vertical slice. Follow phase order; do not reorganize.

**Prerequisite (verify before starting any phase):** `sqlc`, `mockgen`, and `migrate` must be
installed and on `PATH` (`sqlc version`, `mockgen --version`, `migrate -version`). All SQL goes
through sqlc and all mocks through mockgen — **do not hand-edit generated files** in
`internal/platform/postgres/sqlc/*` or `internal/task/mocks/*`; that creates unreproducible
generated state and violates R3.5. If a tool is missing, stop and install it first.

**Conventions for every phase** (from CLAUDE.md + research):
- All SQL via sqlc. After editing `migrations/` + `sql/queries/task.sql`, run `make sqlc-generate` and commit the regenerated output.
- New ports go in `ports.go`; regenerate mocks with `make mock-gen`. New tests use the generated `internal/task/mocks` (gomock) per R5.1 — do not extend the hand-rolled fakes.
- Migrations are stacked (`0002+`), add-nullable → backfill → `NOT NULL` in one up/down pair.
- Service/repo methods take `ctx` first (R3.1); external calls get timeouts (R3.2); wrap errors `%w` (R3.3); sentinel errors `ErrXxx*` in `errors.go` (R2).
- Per-phase gate: `make verify` (= `make lint` + `make test`) must pass.

---

## Phase 1: Tenant identity + HTTP scheduler API

### Changes

#### 1. Migration — identity columns
**File**: `migrations/0002_identity.up.sql` / `.down.sql` **Action**: create
```sql
-- up
ALTER TABLE jobs      ADD COLUMN tenant_id UUID, ADD COLUMN user_id UUID;
ALTER TABLE job_runs  ADD COLUMN tenant_id UUID;
ALTER TABLE run_events ADD COLUMN tenant_id UUID, ADD COLUMN job_id UUID;
-- backfill: pre-existing demo rows are assigned ONE explicit legacy tenant/user sentinel
-- (NOT per-row id, which would orphan every row from a real authenticated user).
-- These demo rows are intentionally NOT surfaced through the tenant-scoped API; only a
-- caller whose resolved tenant == the legacy sentinel could read them. This is acceptable
-- for the template's seed data. The sentinel is a fixed constant, mirrored in code as
-- task.LegacyTenantID / task.LegacyUserID for any tooling that needs it.
UPDATE jobs SET tenant_id = '00000000-0000-0000-0000-0000000000aa',
                user_id   = '00000000-0000-0000-0000-0000000000bb' WHERE tenant_id IS NULL;
UPDATE job_runs jr SET tenant_id = j.tenant_id FROM jobs j WHERE jr.job_id = j.id AND jr.tenant_id IS NULL;
UPDATE run_events re SET tenant_id = jr.tenant_id, job_id = jr.job_id FROM job_runs jr WHERE re.job_run_id = jr.id AND re.tenant_id IS NULL;
ALTER TABLE jobs      ALTER COLUMN tenant_id SET NOT NULL, ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE job_runs  ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE run_events ALTER COLUMN tenant_id SET NOT NULL, ALTER COLUMN job_id SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_tenant ON jobs (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_runs_tenant ON job_runs (tenant_id, created_at DESC);
```
`.down.sql` drops the indexes and columns in reverse.

#### 2. `shared.TenantID`
**File**: `internal/shared/ids.go` **Action**: modify — add `type TenantID uuid.UUID` plus the
full method set (`NewTenantID`, `String`, `IsZero`, `MarshalText`/`UnmarshalText`, `Scan`/`Value`,
`ParseTenantID`) mirroring the existing four ID types exactly.

#### 3. Domain — carry identity
**File**: `internal/task/domain.go` **Action**: modify
- `Job` add `tenantID shared.TenantID`, `userID shared.UserID`; `JobRun` add `tenantID shared.TenantID`; `RunEvent` add `tenantID shared.TenantID`, `jobID shared.JobID`.
- `NewJob(tenantID, userID, kind, description, interval)` — validate `tenantID`/`userID` non-zero → new `ErrInvalidOwner`.
- `NewJobRun(tenantID, jobID, sequence, scheduledAt)`; `NewRunEvent(tenantID, jobID, runID, status)`.
- Extend `rehydrateJob`/`rehydrateJobRun` and add getters `TenantID()`, `UserID()`, `JobID()` (RunEvent).

#### 4. Identity type + service signatures
**File**: `internal/task/service.go` **Action**: modify
```go
type Identity struct {
    TenantID shared.TenantID
    UserID   shared.UserID
}
func (s *Service) Create(ctx context.Context, id Identity, in CreateInput) (*JobRun, error)
func (s *Service) List(ctx context.Context, id Identity, p shared.Pagination) ([]*JobRun, int64, error)
func (s *Service) Status(ctx context.Context, id Identity, runID shared.JobRunID) (*JobRun, error)
func (s *Service) Cancel(ctx context.Context, id Identity, runID shared.JobRunID) (*JobRun, error)
```
`Create` passes `id.TenantID`/`id.UserID` into `NewJob`/`NewJobRun`. `Status`/`Cancel` load the
run via the **unscoped** `FindRunByID` then verify `run.TenantID() == id.TenantID`, else
`ErrJobRunNotFound` (tenant isolation — never leak cross-tenant existence).

**Tenant scoping lives at the service/query boundary, NOT in `FindRunByID`.** The worker is a
system actor that processes queue messages with no caller identity (`worker.go:94`); it must keep
calling the unscoped single-run lookup. So `FindRunByID(ctx, id)` keeps its current signature.
Only *list* operations (which are always caller-initiated) gain a `tenantID` filter.

#### 5. Repo + queries — tenant scoping
**Files**: `sql/queries/task.sql`, `internal/task/{dto_internal,repo_postgres}.go` **Action**: modify
- `InsertJob`/`InsertJobRun`/`InsertRunEvent` gain `tenant_id` (+ `user_id`, `job_id`) columns.
- `ListJobRuns`/`CountJobRuns` gain `WHERE tenant_id = $1`; `Repo.ListRuns(ctx, tenantID shared.TenantID, p)`; service `List(ctx, id Identity, p)` passes `id.TenantID`.
- `GetJobRunByID` (and `Repo.FindRunByID(ctx, id)`) **stay unscoped** — service-layer tenant check handles isolation; the worker relies on this.
- Update all `*FromSqlc`/`*ToInsertParams` mappers for the new columns.

#### 6. TenantResolver port (v1 = identity map)
**File**: `internal/task/ports.go` **Action**: modify
```go
type TenantResolver interface {
    ResolveTenant(ctx context.Context, userID shared.UserID) (shared.TenantID, error)
}
```
v1 implementation lives in `bootstrap` as a closure casting the user UUID to a `TenantID`
(tenant = user). Avoids importing `internal/user` (depguard `no-cross-feature-task`).

#### 7. HTTP layer (new)
**Files**: `internal/task/{handler_http.go,routes.go,dto_http.go}` **Action**: create — model on
`internal/user/{handler_http,routes,dto_http}.go`.
- `dto_http.go`: `CreateJobRequest`/`JobResponse`/`RunResponse` with JSON+validation tags; `to*`/`from*` mappers.
- `handler_http.go`: `Handler{ svc service; resolver TenantResolver }`; `service` interface mirrors the 4 service methods. Each handler: `auth.UserIDFromContext` → `shared.ParseUserID` → `resolver.ResolveTenant` → build `Identity` → call service → map. `writeError` with `errors.Is` for `ErrJobNotFound`/`ErrJobRunNotFound` (404), `ErrInvalidDescription`/`ErrInvalidSchedule`/`ErrInvalidOwner` (400), default 500.
- `routes.go`: `RegisterRoutes(rg, h, authMW)` — group `/jobs` + `auth.Middleware`: `POST ""`, `GET ""`, `GET ":id"`, `POST ":id/cancel"`.

#### 8. Wiring + MCP
**Files**: `internal/bootstrap/wire.go`, `cmd/mcp/main.go`, `internal/task/mcp/tools.go`, `api/openapi.yaml` **Action**: modify
- `wire.go`: build `taskResolver := task.TenantResolverFunc(func(_, uid) (shared.TenantID, error){ return shared.TenantID(uid-as-uuid), nil })`; `taskHandler := task.NewHandler(taskSvc, taskResolver)`; `task.RegisterRoutes(api, taskHandler, authMgr)`.
- MCP: `ToolService` methods gain `id Identity`; `mcp.Register(reg, svc, ident Identity)` captures a service-principal identity. `cmd/mcp/main.go` builds `ident` from config (new `MCP_TENANT_ID`/`MCP_USER_ID` env, fallback to a fixed dev UUID).
- `api/openapi.yaml`: add the four `/jobs` paths + schemas.

### Verification
#### Automated
- [x] `make sqlc-generate` regenerates without error
- [x] `make mock-gen` regenerates task mocks
- [x] `make verify` passes
- [x] new service test: `Create` with identity A then `List`/`Status` with identity B returns empty / `ErrJobRunNotFound`
#### Manual
- [x] `make migrate-up` applies `0002`; `psql \d jobs` shows `tenant_id`, `user_id` NOT NULL
- [x] `POST /api/v1/jobs` with a valid JWT → 201; same call without token → 401
- [x] `GET /api/v1/jobs` returns only the caller's runs

---

## Phase 2: Typed lifecycle events + run/event queries + metrics

### Changes

#### 1. Migration — event enrichment + run timestamps/errors
**File**: `migrations/0003_events.up.sql` / `.down.sql` **Action**: create
```sql
ALTER TABLE run_events ADD COLUMN event_type TEXT, ADD COLUMN event_payload JSONB;
UPDATE run_events SET event_type = 'job_run.' || status WHERE event_type IS NULL;
ALTER TABLE run_events ALTER COLUMN event_type SET NOT NULL;
ALTER TABLE job_runs ADD COLUMN error_code TEXT, ADD COLUMN error_message TEXT,
  ADD COLUMN started_at TIMESTAMPTZ, ADD COLUMN completed_at TIMESTAMPTZ, ADD COLUMN failed_at TIMESTAMPTZ;
```
(Keep the legacy `run_events.status` column — `ListTerminalRecurringRuns` still filters on it.)

#### 2. Domain — EventType + enriched RunEvent
**File**: `internal/task/domain.go` **Action**: modify
```go
type EventType string
const (
    EventJobRunCreated   EventType = "job_run.created"
    EventJobRunEnqueued  EventType = "job_run.enqueued"
    EventJobRunStarted   EventType = "job_run.started"
    EventJobRunSucceeded EventType = "job_run.succeeded"
    EventJobRunFailed    EventType = "job_run.failed"
    EventJobRunRetry     EventType = "job_run.retry_scheduled"
    EventJobRunDLQ       EventType = "job_run.dlq"
    EventJobCancelled    EventType = "job.cancelled"
    // child_job.enqueued, llm.*, quota.* added in later phases
)
```
`NewRunEvent(tenantID, jobID, runID, status, eventType, payload map[string]any)`; add
`EventType()`/`Payload()` getters and `errorCode`/`errorMessage`/`startedAt`/… fields on `JobRun`
set by a new `setError(code, msg string)` + timestamping inside the Mark* transitions.

#### 3. Emit sites → typed events
**Files**: `internal/task/{worker.go,service.go}` **Action**: modify
- `worker.appendEvent(ctx, run, eventType, payload)`; call with `EventJobRunStarted`, `EventJobRunSucceeded`, `EventJobRunRetry` (payload `{attempt, error}`), `EventJobRunFailed` + `EventJobRunDLQ` (payload `{error_code, error_message}`). On failure, `run.setError(...)` before persist so `error_*` columns are written.
- `service.Cancel` emits `EventJobCancelled`.

#### 4. Metrics (new feature pattern)
**File**: `internal/task/metrics.go` **Action**: create
```go
type Metrics struct { runs *prometheus.CounterVec; dur prometheus.Histogram; dlq prometheus.Counter }
func NewMetrics(reg *prometheus.Registry) *Metrics // registers task_runs_total{status}, task_run_duration_seconds, task_dlq_total
```
Inject `*Metrics` into `Worker` (nil-safe). `bootstrap/app.go` passes `metricsReg.Underlying()` —
add a `Registry.Underlying() *prometheus.Registry` accessor in `internal/platform/metrics/metrics.go`.

#### 5. Query endpoints + tools
**Files**: `sql/queries/task.sql`, `internal/task/{ports,service,repo_postgres,dto_internal,handler_http,routes,dto_http}.go`, `internal/task/mcp/tools.go`, `cmd/mcp/main.go`, `api/openapi.yaml` **Action**: modify
- New queries `ListJobRunsByJob`, `ListRunEventsByRun` (tenant-scoped); `Repo.ListRunsByJob`, `Repo.ListEvents`.
- Service `RunsForJob(ctx, id, jobID, p)`, `EventsForRun(ctx, id, runID)`.
- HTTP `GET /jobs/:id/runs`, `GET /runs/:id/events`; MCP `task.runs`, `task.events`.

### Verification
#### Automated
- [x] `make verify` passes
- [x] worker test asserts a `run_events` row with `event_type='job_run.started'` then `…succeeded`
- [x] failure test asserts `event_type='job_run.failed'` + `error_message` populated on the run
#### Manual
- [x] `make migrate-up` applies `0003`
- [x] `GET /runs/{id}/events` returns ordered typed events
- [x] `curl /metrics | grep task_runs_total` shows the series

---

## Phase 3: Tenant quota enforcement

### Changes

#### 1. Migration — quotas
**File**: `migrations/0004_tenant_quotas.up.sql` / `.down.sql` **Action**: create
```sql
CREATE TABLE tenant_quotas (
  tenant_id UUID PRIMARY KEY,
  max_jobs_per_hour INTEGER NOT NULL,
  max_active_recurring_jobs INTEGER NOT NULL,
  max_concurrent_runs INTEGER NOT NULL,
  max_daily_llm_cost_cents INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
);
```

#### 2. Queries + port
**Files**: `sql/queries/task.sql`, `internal/task/ports.go` **Action**: modify
- `GetTenantQuota`, `CountJobsCreatedSince(tenant, since)`, `CountActiveRecurringJobs(tenant)`.
```go
type QuotaRepo interface {
    Get(ctx context.Context, tenantID shared.TenantID) (Quota, error)
    CountJobsSince(ctx context.Context, tenantID shared.TenantID, since time.Time) (int64, error)
    CountActiveRecurring(ctx context.Context, tenantID shared.TenantID) (int64, error)
}
type Quota struct { MaxJobsPerHour, MaxActiveRecurring, MaxConcurrentRuns, MaxDailyLLMCostCents int }
```

#### 3. Enforcement
**Files**: `internal/task/{service.go,errors.go,domain.go,metrics.go}` **Action**: modify
- `Service` gains `quota QuotaRepo` (constructor `NewService(repo, quota)`); default quota used if no row (config-driven defaults).
- `Create` calls `s.checkQuota(ctx, id, kind)` **before** persisting: jobs-per-hour and active-recurring (concurrent + LLM cost checked in later phases). Over hard limit → `ErrQuotaExceeded`.
- **No `run_events` row on creation-time rejection.** At this point no job/run exists, and `run_events.{job_run_id,job_id}` are `NOT NULL` (`0001_init.up.sql:45`, plan Phase 1) — a rejection can't be a `RunEvent`. Record the rejection as a structured log (`tenant_id`, `reason`) + metric `m.QuotaRejections.Inc()` only. (The `quota.rejected`/`quota.deferred` *RunEvent* is emitted later in Phase 7, at execution time, where a run already exists.)
- `ErrQuotaExceeded` in `errors.go`. (No new `EventType` added in this phase.)

#### 4. HTTP mapping + config
**Files**: `internal/task/handler_http.go`, `internal/platform/config/config.go`, `internal/bootstrap/wire.go` **Action**: modify
- `writeError`: `ErrQuotaExceeded` → 429.
- Config `Quota` defaults (`QUOTA_MAX_JOBS_PER_HOUR`, …); wire `NewQuotaRepo(pool)` + defaults.

### Verification
#### Automated
- [x] `make verify` passes
- [x] service test: tenant A over `max_jobs_per_hour` → `ErrQuotaExceeded`, no job/run persisted, `task_quota_rejections_total` incremented; tenant B unaffected
#### Manual
- [x] `make migrate-up` applies `0004`
- [x] over-quota `POST /jobs` → 429; `GET /metrics` shows `task_quota_rejections_total`; logs show the rejection with `tenant_id`

---

## Phase 4: Timezone-safe recurrence

### Changes

#### 1. Migration — recurrence fields
**File**: `migrations/0005_recurrence.up.sql` / `.down.sql` **Action**: create
```sql
ALTER TABLE jobs
  ADD COLUMN schedule_type TEXT, ADD COLUMN scheduled_at_utc TIMESTAMPTZ,
  ADD COLUMN recurrence_rule TEXT, ADD COLUMN local_time TEXT,
  ADD COLUMN timezone_id TEXT, ADD COLUMN original_user_text TEXT;
UPDATE jobs SET schedule_type = kind, timezone_id = 'UTC',
  recurrence_rule = CASE WHEN kind='recurring' THEN 'FREQ=DAILY' ELSE NULL END
  WHERE timezone_id IS NULL;
ALTER TABLE jobs ALTER COLUMN timezone_id SET NOT NULL, ALTER COLUMN schedule_type SET NOT NULL;
```
(`interval_seconds` retained for back-compat; no longer the source of truth.)

#### 2. Recurrence engine (new)
**File**: `internal/task/recurrence.go` **Action**: create
```go
type Rule struct { Freq string; Interval int } // FREQ=DAILY|WEEKLY, INTERVAL default 1
func ParseRule(s string) (Rule, error)          // ErrInvalidRecurrence on bad/unsupported
type DSTNote string                              // "", "shifted_forward", "ambiguous_first"
// NextOccurrence: next instant strictly after `after`, honoring local_time ("HH:MM") in tz.
func NextOccurrence(rule Rule, localTime string, tz *time.Location, after time.Time) (time.Time, DSTNote, error)
```
DST policy via stdlib: build the local wall-clock time; if `time.Date` normalizes it forward
(skipped hour) → `shifted_forward`; if the offset is ambiguous (fall-back) pick the earlier
offset → `ambiguous_first`. Pure + table-test friendly.

#### 3. Domain + validation
**File**: `internal/task/domain.go` **Action**: modify
- `Job` add `scheduleType`, `recurrenceRule`, `localTime`, `timezoneID`, `originalUserText`, `scheduledAtUTC` fields + getters.
- `NewJob(... ScheduleSpec)` where `ScheduleSpec{ Type, ScheduledAtUTC, LocalTime, TimezoneID, RecurrenceRule, OriginalUserText }`; validate IANA via `time.LoadLocation` → `ErrInvalidTimezone`; validate `ParseRule` for recurring → `ErrInvalidRecurrence`.

#### 4. Recurring watcher rewrite
**File**: `internal/task/recurring_watcher.go` **Action**: modify — for each terminal recurring
run, load the job (need `FindJob`/extend `NextRunSpec` to carry tz+rule+localTime), compute
`NextOccurrence(rule, localTime, tz, lastScheduledUTC)`, build `JobRun` with that UTC time, record
a `RunEvent` with the `DSTNote` in payload when non-empty. Update `ListTerminalRecurringRuns` +
`NextRunSpec` to project `timezone_id`, `recurrence_rule`, `local_time`.

#### 5. Create/status surface
**Files**: `internal/task/{service.go,dto_http.go,handler_http.go}`, `internal/task/mcp/tools.go`, `cmd/mcp/main.go`, `api/openapi.yaml` **Action**: modify
- `CreateInput` gains `LocalTime, TimezoneID, RecurrenceRule, ScheduleType, OriginalUserText`; `Create` computes first `scheduled_at_utc` via `NextOccurrence` for recurring (or uses explicit `ScheduledAt` for one-off).
- `task.create`/`POST /jobs` accept the new fields; `task.status`/`GET /jobs/:id` return `timezone_id`, `local_time`, `next_run_at_utc`.

### Verification
#### Automated
- [x] `make verify` passes
- [x] `recurrence_test.go`: `08:00 America/New_York` resolves to the correct UTC before & after the 2026 DST transition; skipped & ambiguous cases return the right `DSTNote`
- [x] invalid timezone / unsupported FREQ → `ErrInvalidTimezone` / `ErrInvalidRecurrence`
#### Manual
- [x] `make migrate-up` applies `0005`
- [x] create a daily `08:00 Asia/Taipei` job; status shows local schedule + next UTC = `00:00Z`

---

## Phase 5: Idempotent execution

### Changes

#### 1. Migration — idempotency
**File**: `migrations/0006_idempotency.up.sql` / `.down.sql` **Action**: create
```sql
CREATE TABLE idempotency_records (
  idempotency_key TEXT PRIMARY KEY, job_run_id UUID NOT NULL, handler_name TEXT NOT NULL,
  status TEXT NOT NULL, response_hash TEXT, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE job_runs ADD COLUMN idempotency_key TEXT;
ALTER TABLE jobs ADD COLUMN side_effecting BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN idempotency_scope TEXT NOT NULL DEFAULT 'job_run';
```

#### 2. Key generation + payload
**Files**: `internal/task/{domain.go,dto_internal.go}` **Action**: modify
- `NewJobRun` computes `idempotencyKey` deterministically (`<job_id>:<sequence>`) unless overridden; getter `IdempotencyKey()`.
- `JobRunMsg` gains `TenantID string`, `IdempotencyKey string`; update `xadd`/decode + all `Enqueue` call sites (watcher, worker retry).

#### 3. Idempotency store + handler contract
**Files**: `internal/task/{ports.go,executor.go,worker.go}`, `sql/queries/task.sql` **Action**: modify
```go
type IdempotencyStore interface {
    Lookup(ctx context.Context, key string) (rec IdempotencyRecord, found bool, err error)
    // Begin atomically claims the key: INSERT ... (status='in_progress') ON CONFLICT DO NOTHING,
    // backed by :execrows. acquired == (rows == 1). A losing caller gets acquired=false.
    Begin(ctx context.Context, key, handler string, runID shared.JobRunID) (acquired bool, err error)
    Complete(ctx context.Context, key, responseHash string) error
}
type IdempotencyRecord struct { Key, Handler, Status, ResponseHash string }
type HandlerInput struct { JobRunID shared.JobRunID; IdempotencyKey, TenantID, JobType string; Payload map[string]any }
```
For `side_effecting` jobs the executor wraps the side effect with an **atomic claim**, not a
check-then-act (two workers can both miss `Lookup`):
1. `acquired, _ := Begin(...)` — the insert is the claim.
2. If `acquired`: execute the side effect → `Complete(key, hash)`.
3. If `!acquired`: re-`Lookup`. If `status=='completed'` → return the recorded result (emit a `duplicate detected` log/event, no re-execution). If `status=='in_progress'` → another worker holds the claim; return a retryable error so the message is redelivered later (do **not** execute).
Read-only jobs skip the store entirely.

### Verification
#### Automated
- [x] `make verify` passes
- [x] failure test: same `JobRunMsg` processed twice → fake side-effecting handler's side effect runs once; second pass logs/events a duplicate
#### Manual
- [x] `make migrate-up` applies `0006`; `idempotency_records` exists

---

## Phase 6: Job chains

### Changes

#### 1. Migration — parent/child
**File**: `migrations/0007_chains.up.sql` / `.down.sql` **Action**: create
```sql
ALTER TABLE jobs ADD COLUMN parent_job_id UUID REFERENCES jobs(id),
  ADD COLUMN trigger_on_parent_status TEXT;
CREATE INDEX idx_jobs_parent ON jobs (parent_job_id);
```

#### 2. Domain + queries
**Files**: `internal/task/domain.go`, `sql/queries/task.sql`, `internal/task/{ports,repo_postgres}.go` **Action**: modify
- `Job` add `parentJobID *shared.JobID`, `triggerOnParentStatus Status`; `NewJob` accepts them (validate the trigger status is terminal).
- Query `FindChildJobs(parent_job_id, trigger_status)`; `Repo.FindChildren(ctx, jobID, status)`.

#### 3. Worker child trigger + cancel propagation
**Files**: `internal/task/{worker.go,service.go,domain.go}`, `internal/task/mcp/tools.go` **Action**: modify
- After a parent run reaches `success`/`failed`/`cancelled`, worker calls `FindChildren`, creates a child `JobRun` (sequence 1), enqueues it, emits `EventChildEnqueued EventType = "child_job.enqueued"`.
- `service.Cancel` cancels pending (`pending`/`queued`) child runs; running children left alone; document in `task.cancel` response + status response shows `parent_job_id`/children.

### Verification
#### Automated
- [x] `make verify` passes
- [x] worker test: parent `success` creates+enqueues child with `child_job.enqueued`; child not created before parent terminal
- [x] service test: cancelling parent cancels a pending child, leaves a running child
#### Manual
- [x] `make migrate-up` applies `0007`

---

## Phase 7: LLM reliability controls

### Changes

#### 1. Migration + job-type dispatch contract
**Files**: `migrations/0008_llm_job_type.up.sql` / `.down.sql`,
`sql/queries/task.sql`, `internal/task/{domain.go,dto_internal.go,repo_postgres.go}` **Action**:
create/modify
- Add `jobs.job_type TEXT NOT NULL DEFAULT 'generic_llm'`. Existing jobs become
  `generic_llm`; this is the only executable job type in v1.
- Add `JobTypeGenericLLM = "generic_llm"` and carry `jobType` through `Job`, `ScheduleSpec`,
  rehydration, persistence, and a `JobType()` getter. `NewJob` defaults an empty job type to
  `generic_llm` and rejects unsupported values with `ErrInvalidJobType`.
- The executor loads the persisted job and dispatches on `job.JobType()`; unknown types fail
  loudly rather than silently executing as an LLM job.

#### 2. LLM port + policy
**Files**: `internal/task/{ports.go,llm.go}` **Action**: create/modify
```go
type LLMClient interface { Complete(ctx context.Context, req LLMRequest) (LLMResponse, error) }
type LLMRequest  struct { Model, Prompt string; MaxInputTokens, MaxOutputTokens int }
type LLMResponse struct { Content string; InputTokens, OutputTokens int }
type LLMPolicy struct { TimeoutSeconds, MaxRetries, MaxInputTokens, MaxOutputTokens, MaxCostCents int; OutputSchema string }
```
`llm.go`: `ValidateOutput(schema, content) error`, `EstimateCostCents(model, in, out int) int`.

#### 3. Executor with reliability wrapper
**File**: `internal/task/executor.go` **Action**: modify — replace the no-op with a job-type
dispatcher. For `generic_llm`:
1. cost estimate vs tenant `max_daily_llm_cost_cents` (via `QuotaRepo` + a daily-cost counter) → over budget: emit `quota.deferred` RunEvent (a run exists here, unlike the Phase 3 creation gate), fail without calling the model;
2. `context.WithTimeout(policy.TimeoutSeconds)`; on timeout emit `llm.timeout`;
3. `ValidateOutput` → on fail emit `llm.validation_failed`, return error so worker retries within budget; never mark success on invalid output;
4. success records token/cost metrics.
New event types (define in `domain.go`): `EventLLMTimeout = "llm.timeout"`, `EventLLMValidationFailed = "llm.validation_failed"`, `EventQuotaDeferred = "quota.deferred"`.

#### 4. Fake client + wiring + config
**Files**: `internal/task/llm_fake.go` (test), `internal/platform/config/config.go`, `internal/bootstrap/wire.go`, `internal/task/metrics.go` **Action**: create/modify
- `fakeLLMClient` returns scripted content/tokens/errors for deterministic tests.
- Config: per-job-type `LLMPolicy` defaults (`LLM_TIMEOUT_SECONDS`, `LLM_MAX_COST_CENTS`, …).
- `wire.go`: inject the fake (real Anthropic adapter deferred — design "What We're NOT Doing"). New metrics: `task_llm_latency_seconds`, `task_llm_timeouts_total`, `task_llm_validation_failures_total`, `task_llm_cost_cents`.

### Verification
#### Automated
- [x] `make sqlc-generate` regenerates without error
- [x] `make mock-gen` regenerates task mocks
- [x] `make verify` passes
- [x] executor test: invalid JSON/schema → never `success` (retries then `failed`); `llm.validation_failed` emitted
- [x] timeout → `llm.timeout` emitted, retried within budget
- [x] cost over `max_daily_llm_cost_cents` → rejected/deferred before any model call
#### Manual
- [ ] `make migrate-up` applies `0008`; `jobs.job_type` exists and defaults to `generic_llm`
- [ ] `curl /metrics | grep task_llm_` shows LLM series

---

## Ticket coverage cross-check
- §5 data model: tenant/user (P1), event enrichment + run errors/timestamps (P2), quotas (P3), recurrence fields (P4), idempotency + side_effecting/scope (P5), parent/child (P6). `locked_by`/`locked_until` **excluded** (design "Not Doing", spec §5.2 deferred).
- §6 MCP tools: improved `task.create`/`task.status` (P1/P4), `task.runs`/`task.events` (P2).
- §7 scheduler API: all `/jobs` + `/runs` endpoints (P1/P2).
- §8 timezone + DST (P4). §9 job chains + cancel policy (P6). §10 idempotency (P5).
- §11 LLM reliability + persisted `generic_llm` dispatch contract (P7). §12 quotas (P3); **fair/per-tenant queues excluded** (design "Not Doing") — `tenant_id` in `JobRunMsg` (P5) keeps the door open.
- §13 observability: metrics + typed events per phase; **Grafana dashboards/Alertmanager provisioning excluded** (design "Not Doing"); alert *thresholds* documented, not wired.
- §14 lifecycle events (P2/P6/P7). §15 migration plan: stacked `0002–0008` (all phases).
- §16 implementation plan / §17 acceptance / §18 tests: each phase's Verification maps to the matching acceptance + test-plan items.

---
Next: run `/qrspi/6_worktree thoughts/qrspi/2026-06-08-production-improvement-spec/` to set up an isolated worktree, or `/qrspi/7_implement thoughts/qrspi/2026-06-08-production-improvement-spec/` to implement in the current tree.
