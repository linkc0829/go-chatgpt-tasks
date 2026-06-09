# Design Discussion

Turns the Production Improvement Spec (`ticket.md`, 6 areas) into a steering document.
Scope for this cycle: **all 6 areas**, fair/weighted scheduling excepted (deferred).
Grounded in [research.md](research.md).

## Current State
- `internal/task/` is MCP-only: no HTTP handler, no auth, driven by the unauthenticated
  stdio server (`cmd/mcp/main.go:40`, research Q2/Q6). No `tenant_id`/`user_id` anywhere
  (`migrations/0001_init.up.sql`, `domain.go`).
- Recurrence = `interval_seconds` `time.Duration`; next-run = `last.scheduled_at + interval`
  (`recurring_watcher.go:53`). No timezone/DST awareness.
- `run_events` is status-only: `id, job_run_id, status, created_at` — no `event_type`,
  no payload, no error detail (`dto_internal.go:84-91`, research Q4).
- Executor is `StubExecutor` no-op behind the `Executor` port (`executor.go:9-31`).
- Single Redis stream `task:runs`; `JobRunMsg{JobRunID, Attempts}` (research Q5).
  At-least-once via consumer-group reclaim; **no idempotency** on side effects.
- Retry + DLQ done (`maxAttempts=3`, `worker.go:131-166`); DLQ stream has no consumer.
- Hex discipline intact: unexported domain fields + `New*`/`rehydrate*`, ports as
  interfaces, `wire.go` sole composition root, `var _ Repo = ...` checks.

## Desired End State
A scheduler service usable by HTTP and MCP clients that: scopes every job/run/event to a
tenant + user; enforces tenant quotas at creation; schedules recurring jobs by
`local_time + timezone_id + recurrence_rule` with DST correctness; supports linear job
chains (`parent_job_id` + `trigger_on_parent_status`); makes side-effecting handlers
idempotent; runs LLM jobs behind a reliability layer (timeout, schema validation, retry,
token/cost guard); and emits typed `RunEvent`s + Prometheus metrics for every transition.

**Verify via** the spec's Acceptance Criteria (§17) and Test Plan (§18): DST-stable daily
jobs, invalid-tz rejection, every transition emits a typed event with error code/message,
duplicate queue messages don't double side effects, over-quota tenants rejected with a
`quota.rejected` event, child runs only after parent terminal status, invalid LLM output
never marked success.

## Patterns to Follow
- **IDs**: add `shared.TenantID` mirroring `shared/ids.go:27-29,39` (UUIDv7, Text/SQL ifaces).
- **Domain**: invariant-checking `New*` + `rehydrate*` split (`domain.go:62-151`); status
  transitions as methods returning `ErrInvalidStatusTransition` (`domain.go:154-211`).
- **Cross-feature**: define task-local capability ports (e.g. `TenantLookup`/`UserLookup`)
  satisfied structurally by `user.Service`; never import `internal/user` (R1.4; the
  contract is documented at `user/service.go:93-99`).
- **HTTP**: model the new scheduler API on `user/handler_http.go` + `routes.go` +
  `dto_http.go`, with `auth.Middleware` on protected groups (`routes.go:21`) and
  `errors.Is` → status mapping (`handler_http.go:107-121`). Derive identity via
  `auth.UserIDFromContext` (`middleware.go:37`).
- **Persistence**: all SQL through sqlc; `pgtype` helpers (`pgtype.go:21-44`); domain↔row
  mapping in `dto_internal.go`. Pagination via `shared.NewPagination` (clamps 20/100).
- **Errors**: sentinel `ErrXxxNotFound`/`ErrXxxInvalid` per `errors.go` (R2).
- **Config**: viper pattern (`config/config.go`) for quota defaults, LLM policy, tz fallback.

### Patterns to NOT follow / to fix
- **`run_events` status-only** → enrich to `event_type TEXT` + `event_payload JSONB`
  (+ `job_id`, `tenant_id`); `NewRunEvent` must carry type + payload + error fields.
- **Tests**: CLAUDE.md R5.1 mandates **gomock**, but the live tests use hand-rolled fakes
  (`task/service_test.go:15-40`) while the generated `mocks/` go unused (research Q7).
  *Decision: follow R5.1 — use the `mocks/` gomock packages for new ports;* treat the
  hand-rolled fakes as the divergent pattern, don't extend them.
- **No feature-level metrics today** (`metrics.go` is platform-only). §13 requires new
  Prometheus vectors in the task feature — a genuinely new pattern here.
- **Integration test is a skipped stub** referencing a non-existent `order` feature
  (`test/integration/order_repo_test.go`) — replace with real task integration tests.

## Design Decisions
1. **Scope = all 6 areas this cycle**, minus fair/weighted scheduling. The structure phase
   must slice aggressively (one area ≈ one+ vertical slice) with test checkpoints; this is
   a large change set and altitude control happens there.
2. **Identity via HTTP scheduler API + JWT** (§7). New `internal/task` HTTP surface
   (`POST /jobs`, `GET /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/cancel`,
   `GET /jobs/{id}/runs`, `GET /runs/{id}/events`) under `auth.Middleware`; tenant/user
   derived from the JWT subject. Service methods take an explicit `Identity{TenantID,
   UserID}` first-class arg (not via context) so both HTTP and MCP supply it the same way.
3. **Migrations stacked `0002+`** (ticket §0 overrides CLAUDE.md R0.1): nullable-add →
   backfill → `NOT NULL`. `jobs`/`job_runs`/`run_events` already exist, so `0002` alters
   them and adds `tenant_quotas` + `idempotency_records`. Backfill: `timezone_id='UTC'`,
   `side_effecting=false`, `idempotency_scope='job_run'`, existing `interval_seconds` →
   equivalent `recurrence_rule` (or leave one-off).
4. **Single stream + `tenant_id` in `JobRunMsg`** (defer fairness). Add `tenant_id` +
   `idempotency_key` to the payload now so downstream logic exists; per-tenant streams /
   round-robin polling are a later slice.
5. **Recurrence: minimal hand-rolled RRULE subset.** Support `FREQ=DAILY|WEEKLY`
   (+ optional `INTERVAL`) with `local_time`, computed via stdlib `time.LoadLocation` for
   IANA/DST. DST policy per §8.5 (skipped→next valid, ambiguous→first), recorded in a
   `RunEvent`. No external RRULE dependency.
6. **LLM: `LLMClient` port + reliability wrapper + fake client.** Replace `StubExecutor`
   with a job-type-dispatching executor. Implement timeout, output-schema validation,
   retry, token/cost estimation + tenant cost-quota guard, and `llm.*` events around an
   `LLMClient` interface; wire a deterministic fake for tests. Real Anthropic adapter is a
   thin, later add.
7. **`tenant_id` sourced 1:1 from user for v1.** No `tenants` table in the spec; treat each
   user as their own tenant (or a tenant resolved via a `TenantLookup` port) until a real
   tenant model is introduced. `tenant_quotas` keyed by that id.
8. **Idempotency**: `idempotency_key` per `JobRun` (deterministic from job_id+sequence for
   non-side-effecting; explicit for side-effecting). Side-effecting handlers check/insert
   `idempotency_records` (check → in-progress → side effect → completed) per §10.

## What We're NOT Doing
- Fair / weighted / per-tenant-queue scheduling (Decision 4 defers §12 queue strategy).
  Note: a lightweight per-tenant round-robin *within a single worker's already-read
  batch* (`Worker.fairOrder`) was added as a cheap, bounded down-payment on §12's
  "poll fairly A→B→C". It is intra-batch only — it does NOT introduce per-tenant
  queues, weighting, or cross-worker fairness, which remain deferred.
- `locked_by`/`locked_until` on `job_runs` (§5.2 — redundant with Redis reclaim, deferred).
- DLQ consumer / reprocessing pipeline (out of spec scope).
- Real Anthropic API adapter, live keys (port + fake only this cycle).
- Full RFC-5545 RRULE; cron expressions beyond the daily/weekly subset.
- §3 non-goals: DAG engine, exactly-once, compensation, visual builder, cross-region.
- Grafana dashboard / Alertmanager provisioning — emit metrics + define alert thresholds;
  actual dashboard JSON and alert wiring are infra work outside the code change.

## Open Risks
- **MCP identity**: with JWT-primary identity, the stdio MCP path still needs a tenant/user.
  Likely a configured service principal or a tenant arg on MCP tools — detail to settle in
  structure. Surfaces a trust-boundary assumption.
- **Magnitude**: 6 areas in one cycle risks an unreviewable change. Mitigation lives in the
  structure phase (independent, individually-shippable slices + checkpoints).
- **Backfill correctness**: existing rows have no tenant and an `interval_seconds` model;
  mapping to the new recurrence/tenant model needs a defined default and a tested migration.
- **DST edge cases**: skipped/ambiguous local times — must be unit-tested per §18.
- **LLM cost estimation**: pre-execution cost is an estimate (token budget × model rate);
  accuracy of the guard depends on that estimate.
- **Status/state-machine growth**: new statuses (e.g. job-level `active`/`cancelled`,
  `deferred`) must extend the domain transition methods, not bypass them.

---
Next: run `/qrspi/4_structure thoughts/qrspi/2026-06-08-production-improvement-spec/`
