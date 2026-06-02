# go-chatgpt-tasks

A **ChatGPT-style scheduled-task backend** built on a Go Hexagonal Architecture
(feature-first package layout). Designed for **AI vibe-coding** and
**system-design demos** — solo developers building portfolio projects targeting
backend roles at international software companies and crypto/web3 firms.

An MCP server lets an AI client (or `mcp-inspector`) create/list/cancel
scheduled tasks; a Postgres-backed watcher + Redis-Streams worker pool executes
them and drives the run lifecycle, including retries, a dead-letter queue, and
recurring re-scheduling.

## Highlights

- **Hexagonal Architecture + feature-first package layout** — minimum AI
  context-switch cost, max cross-feature isolation. See
  [`docs/adr/0001`](docs/adr/0001-hexagonal-feature-first.md).
- **`CLAUDE.md`** at repo root: numbered rules + anti-pattern examples for AI tools.
- **`depguard`** in `.golangci.yml` enforces architectural boundaries — wrong
  imports fail lint.
- **Two inbound transports, one core** — an HTTP API (`cmd/api`) and a stdio
  **MCP server** (`cmd/mcp`) both wire the same `task.Service`.
- **Async execution pipeline** — a **watcher** scans due `pending` runs into a
  **Redis Stream**, a **worker pool** consumes them with a consumer group
  (`XREADGROUP` / `XACK`, `XAUTOCLAIM` for visibility-timeout reclaim), and a
  **recurring watcher** schedules the next run for recurring jobs.
- **Run lifecycle with retry + DLQ** — `pending → queued → running → success` |
  `retry → (requeue)` | `failed → DLQ` (`task:dlq` stream after `maxAttempts`).
  Every transition appends a `RunEvent` audit row; `job_run_id` is the
  idempotency key.
- **Production-shaped infra**: pgx + sqlc, Redis, zap, OpenTelemetry,
  golang-migrate.

## Architecture

Two processes share one core (`task.Service`) and the same datastores. Inbound
transports are thin adapters; all execution happens in supervised background
runners.

```
            ┌──────────────────────┐        ┌──────────────────────┐
 AI client →│  cmd/mcp (stdio MCP) │        │  cmd/api (HTTP + JWT) │← curl
            │  task.create/list/…  │        │  user auth, health   │
            └──────────┬───────────┘        └──────────┬───────────┘
                       │ tool registry adapter         │ handler_http
                       └───────────────┬───────────────┘
                                       ▼
                              ┌──────────────────┐
                              │   task.Service   │  use-cases, ctx timeouts
                              │  (ports only)    │
                              └────┬────────┬────┘
                        repo port  │        │  queue port
                                   ▼        ▼
                        ┌──────────────┐  ┌──────────────────────┐
                        │  PostgreSQL  │  │   Redis Streams       │
                        │ jobs/job_runs│  │ task:runs / task:dlq  │
                        │  run_events  │  └──────────────────────┘
                        └──────────────┘
                                   ▲                 ▲
   Background runners (supervised goroutines under cmd/api's lifecycle):
   ┌───────────────┐   scans due     ┌───────────────┐   recurring next-run
   │   Watcher     │── pending ─────▶│ Worker pool×3 │   ┌──────────────────┐
   │ time-bucket   │   XADD          │ XREADGROUP    │   │ RecurringWatcher │
   │ scan, 5s tick │                 │ exec → mark   │   │ polls run_events │
   └───────────────┘                 │ retry / DLQ   │   │ 10s tick         │
                                     │ XACK/XAUTOCLM │   └──────────────────┘
                                     └───────────────┘
```

### Per-feature layering (hexagon)

Each feature is one package; the file boundaries *are* the hexagon's edges,
enforced by `depguard`:

```
handler_http.go / mcp/      (inbound adapters) → only the service + DTOs
        │
        ▼
service.go                  (use-cases)        → only its own ports.go interfaces
        │
        ▼
ports.go                    (outbound ports)   → interfaces, no implementations
        │
   ┌────┴─────────────┬──────────────┐
   ▼                  ▼              ▼
repo_postgres.go  queue_redis.go  (cross-feature port impls)   (outbound adapters)
        │                  │
        ▼                  ▼
     pgx + sqlc        go-redis        ← drivers confined to adapters
```

`domain.go` sits at the center: stdlib + `internal/shared/` only, no I/O, pure
status-transition methods. Wiring of features + runners happens in exactly one
place, `internal/bootstrap/wire.go`.

### Request → execution flow

1. **Create** — `task.create` (MCP) → `task.Service` persists a `Job` + first
   `pending` `JobRun` (Postgres). HTTP transport mirrors this via `handler_http`.
2. **Dispatch** — the **Watcher** scans `status='pending' AND time_bucket=$b AND
   scheduled_at ≤ now()+window`, marks each `queued`, and `XADD`s it to
   `task:runs`.
3. **Execute** — a **Worker** `XREADGROUP`s a message, marks the run `running`,
   invokes the `JobExecutor`, then `MarkSuccess` / `MarkRetry` (requeue) /
   `MarkFailed`. After `maxAttempts` (3) it dead-letters to `task:dlq`. Each
   step `XACK`s; stuck messages are reclaimed via `XAUTOCLAIM`.
4. **Audit & recur** — every transition appends a `RunEvent`. The
   **RecurringWatcher** polls `run_events`; when a *recurring* job's run reaches
   a terminal state, it creates the next `JobRun`.

`job_run_id` is the idempotency key across the whole pipeline (at-least-once
delivery, best-effort exactly-once execution).

## Domain model

| Entity | Meaning |
|--------|---------|
| `Job` | The task definition — one-off or recurring (with an interval). |
| `JobRun` | A single execution attempt of a job, carrying `status`, `scheduled_at`, `time_bucket`, and `attempts`. |
| `RunEvent` | Append-only audit row written on every status transition; the recurring watcher polls it to schedule the next run. |

Domain entities hold unexported fields, validate invariants in `New<Entity>`,
and expose pure status-transition methods (`MarkQueued` / `MarkRunning` /
`MarkSuccess` / `MarkRetry` / `MarkFailed` / `Cancel`).

## MCP tools

`cmd/mcp` exposes four tools over stdio, routed through an O(1) registry:

| Tool | Purpose |
|------|---------|
| `task.create` | Create a scheduled run. Args: `description`, `scheduled_at` (RFC3339), optional `recurring_interval_seconds`. |
| `task.list` | List runs (`limit` / `offset`). |
| `task.status` | Get a run's status by `job_id`. |
| `task.cancel` | Cancel a run by `job_id`. |

Verify with the MCP inspector:

```bash
npx @modelcontextprotocol/inspector go run ./cmd/mcp
```

A `task.create` with a past `scheduled_at` reaches a terminal status within
~10s; a future time + `task.cancel` ends as `cancelled`.

## Quick start

```bash
# 1. Start local Postgres (port 5432) and Redis (port 6379).
#    Defaults expect database=chatgpt-tasks. See .env.example to override DSNs.

# 2. Set required env (or copy .env.example to .env)
cp .env.example .env
# edit JWT_SECRET at minimum

# 3. Apply migrations (requires golang-migrate CLI)
make migrate-up

# 4. Run the API + background runners (watcher, worker pool, recurring watcher)
make run

# 5. Run the MCP server against the same datastores
go run ./cmd/mcp
```

## Using this template

This repo is a starting point, not a library. When you fork it for a new project:

1. **Edit migration `0001` directly — don't stack new migrations on top.** Until
   you've shipped the first version, schema is not yet history. Modify
   `migrations/0001_*.up.sql` / `*.down.sql` in place to match your real domain.
   Only start creating `0002`, `0003`, … after the first real deploy.
2. **Copy `.env.example` to `.env` yourself** and fill in real values
   (`JWT_SECRET` is mandatory; DSNs only if you deviate from defaults). The
   template will not auto-create `.env` — it's intentionally in `.gitignore`.
3. **Delete template code you don't need.** Removing a feature slice means
   deleting `internal/<feature>/`, its block in `internal/bootstrap/wire.go`, any
   cross-feature port wiring, `sql/queries/<feature>.sql` + its tables in
   migration `0001`, its paths in `api/openapi.yaml`, its
   `no-cross-feature-<name>` depguard block in `.golangci.yml`, and any related
   `.env.example` entries. `make lint && make test` after each removal catches
   dangling references.

## Directory layout

```
cmd/api/main.go              # HTTP API + supervised background runners
cmd/mcp/main.go              # stdio MCP server (task.create/list/status/cancel)
internal/
  task/                      # scheduler feature slice
    domain.go                #   Job / JobRun / RunEvent + transitions
    service.go ports.go      #   use-cases + outbound interfaces
    repo_postgres.go         #   sqlc-backed persistence
    queue_redis.go           #   Redis Streams queue + DLQ
    watcher.go               #   scans due runs → queue
    worker.go                #   consumes queue, drives lifecycle, retry/DLQ
    recurring_watcher.go     #   schedules next run for recurring jobs
    mcp/                     #   MCP tool registry + handlers
  user/                      # JWT auth feature slice
  shared/                    # zero-dependency value objects (IDs, …)
  bootstrap/                 # composition root — wires features + runners
  platform/                  # infrastructure (pgx, redis, jwt, otel, …)
migrations/                  # golang-migrate (jobs, job_runs, run_events, users)
sql/queries/                 # sqlc input
api/openapi.yaml             # HTTP API contract (source of truth)
docs/adr/                    # architecture decision records
test/integration/            # integration tests against local Postgres
CLAUDE.md                    # AI rules
.golangci.yml                # lint + depguard
sqlc.yaml
Makefile
```

## Make targets

```
make run                  # run api + background runners (requires postgres+redis)
make build                # build ./bin/api
make test                 # unit tests
make test-integration     # integration tests (requires local postgres)
make test-cover           # coverage report
make lint                 # golangci-lint
make sqlc-generate        # regenerate sqlc code
make mock-gen             # regenerate gomock ports mocks
make migrate-up           # apply migrations
make migrate-create NAME=add_xxx
```

## Adding a new feature

1. Read [`CLAUDE.md`](CLAUDE.md) and invoke the `new-feature` skill.
2. Create `internal/<feature>/` with the standard per-file layout.
3. Add SQL to `sql/queries/<feature>.sql` + migrations.
4. Wire it in `internal/bootstrap/wire.go`.
5. Update `api/openapi.yaml`.
6. `make lint && make test` — depguard will catch architecture violations.

## Cross-feature dependencies

Features must not import each other. A feature declares the *capability* it needs
as an interface in its own `ports.go`; another feature's service structurally
satisfies it; `internal/bootstrap/wire.go` injects it. The dependent feature has
zero knowledge of the provider.

## Roadmap (not built in)

The prototype builds the *shape* of a high-throughput scheduler (queue
decoupling, time-bucketed due scan, scalable worker pool) without proving load.
Recommended next moves:

- **Real `JobExecutor`** — replace the stub executor with actual task side effects.
- **Native range partitioning** on `job_runs` by `time_bucket` for scale.
- **Reclaim tuning** — pick a concrete `XAUTOCLAIM` idle threshold (visibility timeout).
- **MCP auth** — add authentication to the MCP transport (currently local stdio only).
- **Idempotency-Key middleware** on the HTTP API.

Each fits the existing structure with no architectural changes.
