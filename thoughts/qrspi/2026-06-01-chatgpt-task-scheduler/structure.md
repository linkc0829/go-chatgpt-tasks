# Structure Outline — ChatGPT Task Scheduler

## Approach

Build a new `internal/task/` feature slice (11-file hex layout) plus a stdio MCP
transport, then layer the queue-decoupled execution machinery on top. Slices are
ordered so the MCP surface and persistence land first (immediately demoable in the
inspector), then the watcher → queue, then workers → execution, then recurring
re-scheduling. Redis Streams is the queue; background components run as supervised
goroutines under the existing `App.Run`/`App.Shutdown` lifecycle. Real task execution
is a stub `Executor`.

**Decisions to confirm before Phase 1 (from design Open Risks):**
- Migration mode: new pair `0002_task.up/down.sql` (default) vs. folding into `0001`.
- MCP Go SDK: pin `github.com/modelcontextprotocol/go-sdk` (official) unless you prefer another.
- Partitioning: start with single `job_runs` table + `time_bucket` index (fallback form);
  native range partitioning deferred unless explicitly wanted.

---

## Phase 1: Task feature core + MCP CRUD surface

Delivers the `task` slice end-to-end with no execution: 4 MCP tools persist and read
`Job`/`JobRun` rows. `task.create` writes a `pending` JobRun; list/status/cancel work.

**Files**: `migrations/0002_task.up.sql` / `.down.sql`, `sql/queries/task.sql`,
`internal/task/domain.go`, `ports.go`, `service.go`, `errors.go`, `dto_internal.go`,
`repo_postgres.go`, `internal/task/mcp/registry.go`, `internal/task/mcp/tools.go`,
`cmd/mcp/main.go`, `internal/bootstrap/wire.go` (+ `.golangci.yml` cross-feature block).

**Key changes**:
- `domain.go`: `Job{ id, kind (one-off|recurring), schedule, description }`,
  `JobRun{ id, jobID, status, scheduledAt, timeBucket, attempts }`, `RunEvent{...}` (append-only).
  `NewJob(...) (*Job, error)`, `NewJobRun(jobID, scheduledAt) (*JobRun, error)`,
  pure transitions `MarkQueued/MarkRunning/MarkSuccess/MarkRetry/MarkFailed/Cancel()`,
  `rehydrate(...)`. Statuses: `pending|queued|running|success|retry|failed|cancelled`.
- `ports.go`: `Repo` (`SaveJob`, `SaveRun`, `UpdateRunStatus`, `FindRunByID`, `ListRunsByJob`/owner,
  `FindDueRuns(ctx, bucket, before, limit)`, `AppendEvent`). Ctx-first; imports `context`+`shared` only.
- `service.go`: `NewService(repo Repo) *Service`; `Create(ctx, CreateInput) (*JobRun, error)`,
  `List(ctx, ...)`, `Status(ctx, runID)`, `Cancel(ctx, runID)`. Errors wrapped `%w`.
- `mcp/registry.go`: `type toolHandler func(ctx, json.RawMessage) (any, error)`;
  `Registry map[string]toolHandler`; `Register(name, h)`; O(1) dispatch.
- `cmd/mcp/main.go`: wires `task.Service`, registers `task.create/list/status/cancel`, serves stdio.

**Verify**: `make lint && make test` pass; `npx @modelcontextprotocol/inspector go run ./cmd/mcp`
→ Connect shows 4 tools; `task.create` (past time) → `{job_id, status:"pending"}`;
`task.list`/`task.status`/`task.cancel` behave. Status stays `pending` (no watcher yet).

---

## Phase 2: Redis Streams queue + watcher (pending → queued)

Adds the queue adapter and a supervised watcher goroutine that scans due `pending` runs
in the current time bucket and publishes them, marking them `queued`.

**Files**: `internal/task/queue_redis.go`, `internal/task/watcher.go`,
`internal/task/ports.go` (+`Queue` port), `internal/bootstrap/app.go` + `shutdown.go`
(lifecycle), `internal/bootstrap/wire.go`.

**Key changes**:
- `ports.go`: `Queue interface { Enqueue(ctx, JobRunMsg) error; ... }`.
- `queue_redis.go`: `NewRedisQueue(client *redis.Client) *RedisQueue`; `Enqueue` → `XADD`;
  marshal via `dto_internal.go`. Redis types confined here (mirrors repo boundary).
- `watcher.go`: `NewWatcher(repo Repo, queue Queue, interval time.Duration) *Watcher`;
  `Run(ctx context.Context) error` — `time.Ticker` loop, `FindDueRuns` (bucket, now+5min),
  per-run `Enqueue` then `MarkQueued`/`UpdateRunStatus`. Exits on `ctx.Done()`.
- `app.go`/`shutdown.go`: `App.Run` derives a cancelable ctx, launches watcher under a
  `sync.WaitGroup`; `App.Shutdown` cancels and `wg.Wait()` before closing Redis/PG.

**Verify**: `make test` (watcher unit test w/ mocked Repo+Queue) passes; run `cmd/api`,
`task.create` past time → within one tick `task.status` shows `queued` and a message exists
in the Redis stream (`XLEN`). Graceful shutdown returns (no goroutine leak).

---

## Phase 3: Worker pool + executor + transitions + DLQ

Workers consume the stream, execute via stub `Executor`, drive
`queued → running → success | retry → failed→DLQ`, writing a `RunEvent` per transition.
This satisfies the ticket's core check (past-time job → `completed` ~10s).

**Files**: `internal/task/worker.go`, `internal/task/executor.go` (stub),
`internal/task/queue_redis.go` (consumer group, ack, claim, DLQ), `ports.go` (+`Executor`),
`internal/bootstrap/app.go`/`wire.go`.

**Key changes**:
- `ports.go`: `Executor interface { Execute(ctx, JobRun) error }`.
- `executor.go`: `StubExecutor` logs + returns nil (configurable failure for tests).
- `queue_redis.go`: consumer-group `XREADGROUP`, `XACK` on success, `XAUTOCLAIM` for
  visibility-timeout redelivery, `XADD` to DLQ stream after max attempts.
- `worker.go`: `NewWorker(repo, queue, exec Executor) *Worker`; `Run(ctx) error`;
  `job_run_id` idempotency guard; on success `MarkSuccess`+`AppendEvent`; on failure
  `MarkRetry`+requeue until max attempts → `MarkFailed`+DLQ. Pool of N under the WaitGroup.

**Verify**: `make test` (worker unit tests: success, transient-fail→retry, max→DLQ) pass;
inspector flow — `task.create` past time → `task.status` becomes `success`/`completed`
within ~10s; future time + `task.cancel` → `cancelled`. Forced-fail executor → run lands in DLQ.

---

## Phase 4: Recurring-job watcher (next-run creation)

Polls `RunEvent`; when a recurring job's run terminates (`success`/`failed`), creates the
next `pending` JobRun from the schedule spec, guarded against duplicate creation.

**Files**: `internal/task/recurring_watcher.go`, `sql/queries/task.sql` (terminal-event query
+ unique guard), `migrations/0002_task.up.sql` (unique `(job_id, sequence)` on `job_runs`),
`internal/bootstrap/app.go`.

**Key changes**:
- `recurring_watcher.go`: `NewRecurringWatcher(repo Repo, interval) *RecurringWatcher`;
  `Run(ctx) error` — polls terminal `RunEvent`s, computes next `scheduledAt`, `SaveRun`
  for recurring jobs. Unique `(job_id, sequence)` makes double-creation a no-op (idempotent).
- Launched as a third supervised goroutine in `App.Run`.

**Verify**: `make test` (next-run created once per terminated recurring run; duplicate poll
is a no-op) passes; create a recurring job → after a run terminates, a new `pending` JobRun
appears with the next `scheduled_at`.

---

## Testing Checkpoints

- **After Phase 1**: `task` slice compiles & lint-clean; inspector shows 4 tools; CRUD persists
  to Postgres; runs stay `pending`. (MCP surface demoable on its own.)
- **After Phase 2**: due pending runs become `queued` and land in the Redis stream; watcher is a
  supervised goroutine that shuts down cleanly. (Producer side complete.)
- **After Phase 3**: full execution path — `pending→queued→running→success`, retry, and DLQ work;
  ticket's primary verification (past-time → completed ~10s, cancel → cancelled) passes.
- **After Phase 4**: recurring jobs self-reschedule once per terminated run; uniqueness guard
  prevents duplicates.

Each phase is independently valuable: if Phase 3 stalls, Phases 1–2 still deliver a working MCP
CRUD surface plus a queue-publishing watcher.
