# Structure Outline

## Approach
Seven vertical slices, each crossing migration → domain → service → API/MCP → tests.
Order follows the ticket's blast-radius logic: identity first (every later slice needs it
on the service signature), then the typed-event/metrics foundation, then quota, recurrence,
idempotency, chains, and LLM reliability. Each phase is independently shippable; if a later
phase is dropped, earlier ones still deliver value. Migrations are stacked (`0002+`,
add-nullable → backfill → `NOT NULL` within each phase's up/down pair). New ports use the
generated `mocks/` gomock packages (R5.1).

---

## Phase 1: Tenant identity + HTTP scheduler API
Establishes tenant/user scoping and the authenticated HTTP surface that threads identity
into the task service. Foundation for every later phase.

**Files**: `migrations/0002_identity.{up,down}.sql`, `internal/shared/ids.go`,
`internal/task/{domain,service,ports,errors,handler_http,routes,dto_http,dto_internal,repo_postgres}.go`,
`sql/queries/task.sql`, `internal/bootstrap/{wire,app}.go`, `cmd/mcp/main.go`,
`internal/task/mcp/tools.go`, `api/openapi.yaml`, `.golangci.yml`
**Key changes**:
- `0002`: `jobs`/`job_runs`/`run_events` add `tenant_id UUID`, `user_id UUID` (jobs) — nullable, backfill from a default, then `NOT NULL`; index `(tenant_id, status)` on jobs.
- `shared.TenantID` (UUIDv7) mirroring existing ID types.
- `type Identity struct { TenantID shared.TenantID; UserID shared.UserID }`
- `Service.Create(ctx, id Identity, in CreateInput)`, `List/Status/Cancel(ctx, id Identity, ...)` — identity threaded as first-class arg; repo queries filter by `tenant_id`.
- `type TenantLookup interface { ResolveTenant(ctx, shared.UserID) (shared.TenantID, error) }` in task `ports.go`; satisfied structurally by `user.Service` (v1: tenant = user).
- New `internal/task` HTTP: `Handler`, `RegisterRoutes(rg, h, authMW)` modeled on `user/` — `POST /jobs`, `GET /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/cancel` under `auth.Middleware`; `errors.Is` → status mapping.
- MCP path supplies identity via a configured service principal (settle exact mechanism here).

**Verify**: `make test` passes; `POST /api/v1/jobs` with a valid JWT creates a job scoped to the caller's tenant; no/invalid token → 401; `GET /jobs` returns only the caller's tenant's runs.

---

## Phase 2: Typed lifecycle events + run/event queries + metrics
Enriches `run_events` from status-only to typed events with payloads, exposes run/event
history, and adds the first feature-level Prometheus metrics.

**Files**: `migrations/0003_events.{up,down}.sql`, `internal/task/{domain,service,worker,dto_internal,handler_http,routes}.go`, `internal/task/mcp/tools.go`, `sql/queries/task.sql`, `internal/platform/metrics/metrics.go` (or new task metrics file), `api/openapi.yaml`
**Key changes**:
- `0003`: `run_events` add `event_type TEXT NOT NULL`, `event_payload JSONB NULL`, `job_id UUID`; `job_runs` add `error_code TEXT`, `error_message TEXT`, `started_at/completed_at/failed_at TIMESTAMPTZ`.
- `type EventType string` constants (`job_run.created/enqueued/started/succeeded/failed/retry_scheduled/dlq`, `job.cancelled`, …); `NewRunEvent(jobRunID, jobID, EventType, payload map[string]any)`.
- Update every emit site (`service.Cancel`, `worker` running/success/retry/failed) to typed events; failures persist `error_code`/`error_message`.
- `GET /jobs/{id}/runs`, `GET /runs/{id}/events`; MCP `task.runs`, `task.events`.
- Prometheus: `task_runs_total{status}`, `task_run_duration_seconds`, `task_dlq_total` registered on the platform registry.

**Verify**: `make test` passes; each transition writes a typed `run_events` row (assert in worker tests); `GET /runs/{id}/events` returns ordered history; `/metrics` exposes `task_*` series.

---

## Phase 3: Tenant quota enforcement
Adds quota storage and creation-time enforcement with rejection events.

**Files**: `migrations/0004_tenant_quotas.{up,down}.sql`, `internal/task/{ports,service,errors,domain}.go`, `sql/queries/task.sql`, `internal/task/handler_http.go`, metrics file
**Key changes**:
- `0004`: `tenant_quotas` (tenant_id PK, `max_jobs_per_hour`, `max_active_recurring_jobs`, `max_concurrent_runs`, `max_daily_llm_cost_cents`, timestamps).
- `type QuotaRepo interface { Get(ctx, shared.TenantID) (Quota, error); CountJobsSince(...); CountActiveRecurring(...) }` in `ports.go`.
- `Service.Create` calls a `checkQuota(ctx, id)` gate before persisting; over hard limit → `ErrQuotaExceeded` + `quota.rejected` event.
- Per-tenant metrics: `task_quota_rejections_total{tenant}`, `task_jobs_created_total{tenant}`.

**Verify**: `make test` passes; a tenant over `max_jobs_per_hour` gets a 429-style rejection + `quota.rejected` event; a second tenant under quota is unaffected (service test with two identities).

---

## Phase 4: Timezone-safe recurrence
Replaces the `interval_seconds` model with `local_time + timezone_id + recurrence_rule` and
DST-correct next-run computation.

**Files**: `migrations/0005_recurrence.{up,down}.sql`, `internal/task/{domain,recurrence.go,recurring_watcher,service,dto_internal,dto_http,handler_http}.go`, `internal/task/mcp/tools.go`, `sql/queries/task.sql`, `api/openapi.yaml`
**Key changes**:
- `0005`: `jobs` add `schedule_type TEXT`, `scheduled_at_utc TIMESTAMPTZ`, `recurrence_rule TEXT`, `local_time TEXT`, `timezone_id TEXT` (backfill `UTC`), `original_user_text TEXT`; backfill `interval_seconds` → equivalent rule then deprecate.
- `recurrence.go`: `ParseRule(string) (Rule, error)` for `FREQ=DAILY|WEEKLY` (+ `INTERVAL`); `NextOccurrence(rule, localTime string, tz *time.Location, after time.Time) (time.Time, DSTNote, error)` via `time.LoadLocation`; DST policy (skipped→next valid, ambiguous→first), note recorded as a `RunEvent`.
- `NewJob` validates IANA tz + rule → `ErrInvalidTimezone`/`ErrInvalidRecurrence`; `recurring_watcher` computes next local → UTC.
- `task.create`/`POST /jobs` accept tz/recurrence fields; `task.status`/`GET /jobs/{id}` return local schedule + `next_run_at_utc`.

**Verify**: `make test` passes; unit tests: `08:00 America/New_York` stays 8 AM local across a DST boundary, skipped/ambiguous cases; invalid tz rejected; status shows both local + next UTC.

---

## Phase 5: Idempotent execution
Adds idempotency keys and the side-effecting handler dedupe contract.

**Files**: `migrations/0006_idempotency.{up,down}.sql`, `internal/task/{domain,ports,service,worker,executor,dto_internal}.go`, `sql/queries/task.sql`
**Key changes**:
- `0006`: `idempotency_records` (idempotency_key PK, job_run_id, handler_name, status, response_hash, timestamps); `job_runs` add `idempotency_key TEXT`, `jobs` add `side_effecting BOOLEAN`, `idempotency_scope TEXT`.
- `JobRunMsg` adds `TenantID`, `IdempotencyKey`.
- `type IdempotencyStore interface { Lookup(ctx, key) (Record, bool, error); Begin(...); Complete(...) }`.
- Handler contract `HandlerInput{ JobRunID, IdempotencyKey, TenantID, JobType, Payload }`; side-effecting flow: lookup → if complete return recorded result → begin in-progress → execute → complete.

**Verify**: `make test` passes; failure test: same `JobRunMsg` delivered twice → side effect runs once (fake side-effecting handler counts calls); duplicate detection visible in events/logs.

---

## Phase 6: Job chains
Adds linear parent→child dependencies and cancellation propagation.

**Files**: `migrations/0007_chains.{up,down}.sql`, `internal/task/{domain,service,worker}.go`, `sql/queries/task.sql`, `internal/task/{handler_http,mcp/tools}.go`
**Key changes**:
- `0007`: `jobs` add `parent_job_id UUID NULL`, `trigger_on_parent_status TEXT NULL`; index on `parent_job_id`.
- Worker: on terminal status, `FindChildren(ctx, jobID, status)` → create child `JobRun` + enqueue + `child_job.enqueued` event.
- `Service.Cancel` cancels pending child jobs (running children not force-stopped); `task.cancel`/status output document the policy and show chain linkage.

**Verify**: `make test` passes; child run is created only after parent terminal status and not before; cancelling a parent cancels pending children (service + worker tests).

---

## Phase 7: LLM reliability controls
Replaces `StubExecutor` with a job-type-dispatching executor wrapping an `LLMClient` port in
a reliability layer.

**Files**: `internal/task/{executor.go,llm.go,ports,domain}.go`, `internal/task/llm_fake.go` (test), `internal/platform/config/config.go`, `internal/bootstrap/wire.go`, metrics file
**Key changes**:
- `type LLMClient interface { Complete(ctx, LLMRequest) (LLMResponse, error) }`; `LLMPolicy{ TimeoutSeconds, MaxRetries, MaxInput/OutputTokens, MaxCostCents, OutputSchema }` per job type (config-driven).
- Executor dispatches by `job_type`; reliability wrapper: `context.WithTimeout`, output-schema validation, retry within budget, pre-run cost estimate vs tenant `max_daily_llm_cost_cents`; emits `llm.timeout`/`llm.validation_failed`/`quota.rejected`.
- Deterministic `fakeLLMClient` for tests; real Anthropic adapter explicitly deferred.

**Verify**: `make test` passes; invalid JSON/schema → run never marked success (retry then failed); timeout enforced; cost guard rejects/defers over-budget tenant; `llm.*` events + metrics emitted.

---

## Testing Checkpoints
- **After P1**: identity threaded through service; HTTP API auth-gated; tenant-scoped reads. App boots with new routes.
- **After P2**: every status transition emits a typed `run_events` row with payload + error detail; run/event history queryable; `task_*` metrics live.
- **After P3**: creation-time quota gate rejects over-limit tenants with events; no noisy-neighbor.
- **After P4**: DST-correct recurrence; invalid tz/rule rejected; status shows local + next UTC.
- **After P5**: duplicate deliveries don't double side effects; idempotency records written.
- **After P6**: linear chains fire on parent terminal status; cancellation propagates to pending children.
- **After P7**: LLM jobs are timeout/validation/retry/cost guarded; invalid output never succeeds.

## Notes / Can't-fully-slice
- **Migration ceremony**: stacked add-nullable→backfill→`NOT NULL` is heavier than the
  template's data warrants, but matches the chosen post-deploy mode (design Decision 3).
- **MCP identity** (design Open Risk) is settled inside Phase 1; if the service-principal
  approach proves wrong it affects only P1's wiring, not later slices.
- **Fair scheduling / per-tenant queues** are intentionally absent (deferred); `tenant_id`
  in `JobRunMsg` (P5) keeps the door open without the queue rework.

---
Next: run `/qrspi/5_plan thoughts/qrspi/2026-06-08-production-improvement-spec/`
