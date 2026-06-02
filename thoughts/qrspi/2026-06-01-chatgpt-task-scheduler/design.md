# Design Discussion — ChatGPT Task Scheduler

## Current State

The repo is a Go hexagonal HTTP backend (feature-first packages). Three demo slices
(`user`, `order`, `payment`) follow an 11-file structure; composition lives only in
`internal/bootstrap/wire.go`; infra in `internal/platform/`; single entry `cmd/api/main.go`.

Relevant gaps for this work (from research):
- **No scheduler/cron/worker precedent.** No background loops, `time.Ticker`, queues, worker
  pools, or supervised goroutines beyond the HTTP runner (`research.md` Q6, line 63-64, 77).
- **Redis wired but unused.** Pool built in `app.go:74`, passed to `wireFeatures` as `_`
  (`wire.go:27`); no cache/queue adapter pattern exists yet (line 78). This feature establishes it.
- **Lifecycle is synchronous.** `App.Run` runs one goroutine; `App.Shutdown` (`shutdown.go:13`)
  is fixed-order sequential, every step attempted. We must extend it to supervise N goroutines.
- **One transport only.** HTTP via `cmd/api/main.go`. MCP (stdio) is a second inbound transport.

## Desired End State

A `task` feature slice + a stdio MCP server, realizing the ticket's full deep-dive:

1. **MCP server** (`cmd/mcp/main.go`) exposes 4 tools — `task.create`, `task.list`,
   `task.status`, `task.cancel` — over stdio, routed via a registry (not if-else). Verifiable with
   `npx @modelcontextprotocol/inspector` per ticket lines 87-95.
2. **Watcher** goroutine scans `pending` JobRuns due within a 5-min window (time-bucketed query)
   and publishes them to a Redis queue, marking them `queued`.
3. **Workers** consume the queue, execute the job, and drive status transitions:
   `pending → queued → running → success` | `retry → (requeue)` | `failed → DLQ`. Every transition
   writes a `RunEvent`. `job_run_id` is the idempotency key (ticket lines 28-32).
4. **Recurring-job watcher** polls `RunEvent`; when a recurring job's run terminates
   (`success`/`failed`), it creates the next `JobRun` (ticket lines 25-26).

**Verification:** inspector shows 4 tools; `task.create` with a past `scheduled_at` → status
becomes `completed` within ~10s; future time + `task.cancel` → `cancelled`; `task.list` lists all.
`make lint && make test` pass. Worker survives transient executor failures (retry) and dead-letters
after max attempts.

## Patterns to Follow

- **11-file feature layout** as in `internal/order/` — `domain.go` / `ports.go` / `service.go` /
  `errors.go` / `dto_internal.go` / `repo_postgres.go` (+ MCP adapter, + background components).
- **Domain purity** — entities hold unexported fields, `New<Entity>` validates invariants,
  state transitions are pure ctx-free methods (`order/domain.go:31,60,69`), `rehydrate(...)` rebuilds
  from trusted storage (`domain.go:50`). `JobRun.MarkRunning/MarkSuccess/MarkRetry/MarkFailed`.
- **Service takes interfaces only** (`order/service.go:16`); errors wrapped `%w`; ownership checks
  return NotFound to avoid leaking existence (`service.go:62`).
- **Repo boundary** — `PostgresRepo{q *sqlc.Queries}`, `New(pool)` wraps `sqlc.New`
  (`order/repo_postgres.go:22,26`); `pgx.ErrNoRows → ErrNotFound`; all SQL via sqlc (R3.5);
  `pgtype` confined to repo via helpers (`platform/postgres/pgtype.go:21-44`).
- **Redis adapter (NEW pattern, mirror the repo one)** — `queue_redis.go`: narrow
  `New(client *redis.Client) *RedisQueue`, marshal via `dto_internal.go`, no business logic,
  confine redis types to the adapter (analogous to `cache_redis.go` slot in CLAUDE.md R2).
- **Cross-transport reuse** — MCP adapter mirrors `handler_http.go`: parse args → call the *service
  interface* → map result/error. No business logic in the adapter (R1.3). Tool registry = a
  `map[string]toolHandler` (ticket Q5, O(1) routing).
- **Wiring only in bootstrap** — `wireFeatures` (`wire.go:24`) is the sole multi-feature importer;
  shape-mismatch adapters live there (`userLookupAdapter`, `wire.go:78-90`).
- **Per-operation context timeouts** (`order/handler_http.go:45,73`); all external calls timed (R3.2).

**Do NOT follow / avoid:** there is no precedent for unsupervised goroutines — do not fire-and-forget
the watcher/workers. Do not let the MCP adapter or repo carry business logic. Do not hand-write SQL.

## Design Decisions

1. **Redis Streams as the queue** — `XADD` to enqueue, consumer group + `XREADGROUP` for
   per-message exclusivity, `XACK` on success, `XCLAIM`/idle reclaim for the visibility-timeout
   redelivery the ticket requires (lines 29-30). DLQ = a separate stream. This satisfies
   at-least-once natively; `job_run_id` gives best-effort exactly-once (R3 ticket Q2).
2. **Two processes, one core** — `cmd/api` (HTTP API + watcher + workers as supervised goroutines)
   and `cmd/mcp` (stdio MCP, create/list/status/cancel only). Both wire the same `task.Service` and
   share Postgres + Redis.
3. **Supervised goroutines via extended lifecycle** — `App.Run` launches watcher + worker pool +
   recurring-watcher under a derived `context.Context`; `App.Shutdown` cancels and waits
   (`sync.WaitGroup`) before closing Redis/PG, extending the existing fixed-order teardown.
4. **Domain model: `Job` / `JobRun` / `RunEvent`** — `Job` is the definition (one-off or recurring +
   schedule spec); `JobRun` is one execution attempt with status; `RunEvent` is the append-only audit
   the recurring-watcher polls. Mirrors the ticket's deep-dive vocabulary.
5. **Time-bucket partitioning** — `job_runs` carries a `time_bucket` (hour) column; the watcher query
   filters `status='pending' AND time_bucket = $bucket AND scheduled_at <= now()+5min`, backed by a
   composite index. Declarative Postgres range partitioning is the target; exact form decided in the
   structure phase (see Open Risks).
6. **Pluggable `JobExecutor` port** — actual task execution is a stub `Executor` for the prototype
   (logs/marks success), so the queue/retry/DLQ machinery is exercised without real side effects.
7. **MCP Go SDK** — use a maintained MCP library rather than hand-rolling the protocol; tool registry
   wraps SDK registration. Library choice confirmed in structure phase (Open Risks).

## What We're NOT Doing

- **Not building a Python server.** The ticket's Python verification commands are realized as Go
  (`go run ./cmd/mcp` under the inspector instead of `python -m app.mcp_server`).
- **Not removing the demo slices** (R0.3). `user`/`order`/`payment` stay unless you ask to prune them.
- **Not implementing real task side effects** — `Executor` is a stub (Decision 6).
- **Not adding auth to the MCP transport** in this pass (stdio, local inspector). HTTP API keeps JWT.
- **Not horizontally proving 10K jobs/sec** — we build the *shape* (queue decoupling, partitioned
  scan, scalable worker pool) the ticket's reasoning targets; load-testing is out of scope.
- **Not building an MCP-side cancel/status race protocol** beyond the idempotency key + status guards.

## Open Risks

- **Migration mode (R0.1).** Scheduler tables are *new*, not alterations of template tables. Default:
  add them in a new migration pair; confirm vs. folding into `0001` during structure phase.
- **Redis Streams reclaim tuning.** Visibility timeout = consumer idle threshold for `XAUTOCLAIM`;
  picking a value that balances duplicate execution vs. stuck-message latency needs a concrete number.
- **Native partitioning + sqlc.** sqlc generates against the parent table fine, but partition
  creation/rotation is DDL the app or a migration must manage; if it complicates the slice, fall back
  to a single table + `time_bucket` index (still satisfies the query pattern).
- **MCP SDK maturity.** The Go MCP ecosystem is young; the chosen library's stdio + tool-registration
  API shapes the adapter. Pin a version and isolate it behind our registry.
- **Recurring-watcher duplicate creation.** Polling `RunEvent` risks creating two next-runs if two
  watchers run; needs a uniqueness guard (e.g. unique `(job_id, sequence)` on `job_runs`).
