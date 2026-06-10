# go-chatgpt-tasks

A **ChatGPT-style scheduled-task backend** built on a Go Hexagonal Architecture
(feature-first package layout). Designed for **AI vibe-coding** and
**system-design demos** — solo developers building portfolio projects targeting
backend roles at international software companies and crypto/web3 firms.

An MCP server lets an AI client (or `mcp-inspector`) create/list/cancel
scheduled tasks; a Postgres-backed watcher + Redis-Streams worker pool executes
them and drives the run lifecycle, including retries, a dead-letter queue, and
recurring re-scheduling. The scheduler is **production-hardened**: every
job/run/event is scoped to a tenant + user, recurrence is timezone/DST-safe,
side-effecting jobs execute idempotently, jobs can form linear chains, and
LLM-backed jobs run behind timeout / validation / retry / cost controls.

## Highlights

- **Hexagonal Architecture + feature-first package layout** — minimum AI
  context-switch cost, max cross-feature isolation. See
  [`docs/adr/0001`](docs/adr/0001-hexagonal-feature-first.md).
- **`CLAUDE.md`** at repo root: numbered rules + anti-pattern examples for AI tools.
- **`depguard`** in `.golangci.yml` enforces architectural boundaries — wrong
  imports fail lint.
- **Two inbound transports, one core** — a JWT-authenticated HTTP scheduler API
  (`cmd/api`: `POST/GET /api/v1/jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/cancel`,
  `GET /jobs/{id}/runs`, `GET /runs/{id}/events`) and a stdio **MCP server**
  (`cmd/mcp`) both call the same `task.Service` with an explicit
  `Identity{TenantID, UserID}`.
- **Multi-tenant isolation** — every job/run/event carries `tenant_id` +
  `user_id`; creation enforces per-tenant quotas (jobs/hour, active recurring,
  concurrent-run admission), and workers claim per-tenant execution slots
  atomically (Postgres advisory lock) before running.
- **Timezone-safe recurrence** — recurring jobs store
  `local_time + timezone_id + recurrence_rule` (`FREQ=DAILY|WEEKLY;INTERVAL=n`),
  so "08:00 America/New_York" stays 8 AM local across DST. Skipped/ambiguous DST
  times follow a documented policy and are recorded in a `RunEvent`.
- **Async execution pipeline** — a **watcher** scans due `pending` runs into a
  **Redis Stream**, a **worker pool** consumes them with a consumer group
  (`XREADGROUP` / `XACK`, `XAUTOCLAIM` for stalled-owner reclaim), and a
  **recurring watcher** schedules the next run for recurring jobs.
- **Run lifecycle with retry + DLQ** — `pending → queued → running → success` |
  `retry → (requeue)` | `failed → DLQ` (`task:dlq` stream after `maxAttempts`).
  Every transition appends a typed `RunEvent` (status + event row written in one
  transaction), with error code/message on failure.
- **Idempotent execution** — at-least-once delivery + idempotent handlers:
  each run carries a deterministic `idempotency_key` (`job_id:sequence`);
  side-effecting jobs atomically claim an `idempotency_records` row before
  executing, so duplicate deliveries never re-apply side effects.
- **Linear job chains** — `parent_job_id` + `trigger_on_parent_status`: a child
  job runs after its parent reaches the matching terminal status; cancelling a
  parent cancels pending children.
- **LLM reliability controls** — LLM jobs run behind an `LLMClient` port with
  per-job-type policy: timeout, retry budget, JSON-schema output validation
  (never mark invalid output as success), and a durable per-tenant daily cost
  guard (`tenant_llm_daily_cost`, worst-case budget reserved upfront).
- **Observability** — feature-level Prometheus metrics (runs, duration, DLQ,
  quota rejections, LLM latency/timeouts/validation/cost) plus a Grafana
  dashboard and Prometheus alert rules under `observability/`.
- **Built-in browser demo** — a dependency-free single-page UI embedded in the
  API binary (`go:embed`) at `http://localhost:8080/demo/`: register/sign in,
  schedule jobs, watch the run lifecycle, inspect events, cancel pending work.
- **Production-shaped infra**: pgx + sqlc, Redis, zap, OpenTelemetry,
  golang-migrate.

## Architecture

Two processes share one core (`task.Service`) and the same datastores. Inbound
transports are thin adapters; all execution happens in supervised background
runners.

```
            ┌──────────────────────┐        ┌───────────────────────┐
 AI client →│  cmd/mcp (stdio MCP) │        │  cmd/api (HTTP + JWT)  │← curl /
            │ task.create/list/    │        │ /api/v1/jobs, /runs,  │  browser
            │ status/cancel/runs/  │        │ user auth, health,    │
            │ events               │        │ /metrics, /demo/ UI   │
            └──────────┬───────────┘        └──────────┬────────────┘
                       │ service-principal identity    │ JWT → Identity
                       └───────────────┬───────────────┘
                                       ▼
                              ┌──────────────────┐
                              │   task.Service   │  Identity-scoped use-cases,
                              │  (ports only)    │  tenant quota checks
                              └────┬────────┬────┘
                        repo port  │        │  queue port
                                   ▼        ▼
                        ┌──────────────────┐  ┌──────────────────────┐
                        │   PostgreSQL     │  │   Redis Streams       │
                        │ jobs / job_runs  │  │ task:runs / task:dlq  │
                        │ run_events       │  └──────────────────────┘
                        │ tenant_quotas    │
                        │ idempotency_recs │
                        │ llm_daily_cost   │
                        └──────────────────┘
                                   ▲                 ▲
   Background runners (supervised goroutines under cmd/api's lifecycle):
   ┌───────────────┐   scans due     ┌────────────────┐   recurring next-run
   │   Watcher     │── pending ─────▶│ Worker pool×3  │   ┌──────────────────┐
   │ time-bucket   │   XADD          │ XREADGROUP     │   │ RecurringWatcher │
   │ scan, 5s tick │                 │ tenant slot →  │   │ tz-aware next    │
   └───────────────┘                 │ idempotency →  │   │ occurrence,      │
                                     │ LLM executor → │   │ 10s tick         │
                                     │ retry / DLQ /  │   └──────────────────┘
                                     │ child trigger  │
                                     │ XACK/XAUTOCLM  │
                                     └────────────────┘
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

1. **Create** — `task.create` (MCP) or `POST /api/v1/jobs` (HTTP+JWT) →
   `task.Service` checks the tenant's quotas (jobs/hour, active recurring,
   concurrent-run admission; over limit → 429 + metric), computes the first
   `scheduled_at_utc` from the local schedule, then persists `Job` + first
   `pending` `JobRun` + `job.created`/`job_run.created` events in one
   transaction.
2. **Dispatch** — the **Watcher** scans `status='pending' AND time_bucket=$b AND
   scheduled_at ≤ now()+window`, marks each `queued`, `XADD`s it to `task:runs`,
   and emits `job_run.enqueued`. If the enqueue fails, the run is reverted to
   `pending` so the next scan retries it.
3. **Execute** — a **Worker** `XREADGROUP`s a message, atomically claims a
   per-tenant concurrency slot while marking the run `running` (over the limit →
   left for redelivery), then executes through the chain *idempotency wrapper →
   LLM executor* (timeout, retry budget, output-schema validation, daily cost
   guard). Then `MarkSuccess` / `MarkRetry` (requeue) / `MarkFailed`. After
   `maxAttempts` (3) it dead-letters to `task:dlq`. Stalled owners are recovered
   via `XAUTOCLAIM` — the reclaim window is strictly longer than the message
   timeout, so a live execution is never reclaimed.
4. **Audit, chain & recur** — every transition writes a typed `RunEvent` in the
   same transaction as the status update. When a run reaches a terminal status,
   matching child jobs (`trigger_on_parent_status`) get their next run created
   and enqueued (`child_job.enqueued`). The **RecurringWatcher** computes the
   next timezone-aware occurrence and inserts the next `JobRun`.

Delivery is **at-least-once + idempotent handlers**: every run carries a
deterministic `idempotency_key` (`job_id:sequence`), and side-effecting jobs
claim an `idempotency_records` row (`INSERT … ON CONFLICT`) before executing,
so duplicate deliveries are detected instead of re-applied.

## Design decisions

1. **Queue + worker pool instead of executing from the DB scan.** The watcher
   only *finds* due runs and hands them to a Redis Stream; execution happens in
   a separate worker pool. This decouples scheduling load from execution load —
   a burst of due jobs at peak hours queues up instead of stalling the scan
   loop, workers scale horizontally without touching the scheduler, and a slow
   or crashing handler can't block dispatch. The cost is an extra hop and
   at-least-once semantics, which decision 2 absorbs.
2. **At-least-once delivery + idempotency keys, not exactly-once.** Exactly-once
   across a queue, a database, and external APIs isn't realistically
   achievable, so the design embraces redelivery: every run carries a
   deterministic `idempotency_key` (`job_id:sequence`), and side-effecting
   handlers atomically claim an `idempotency_records` row before acting.
   A duplicate delivery becomes a detected no-op instead of a double charge.
3. **`TenantQuota` caps single-user blast radius.** Per-tenant limits
   (jobs/hour, active recurring, concurrent runs, daily LLM cost) are enforced
   at creation *and* at execution — workers claim a per-tenant slot under a
   Postgres advisory lock before running. One noisy tenant gets 429s and
   deferred runs; everyone else keeps their throughput.
4. **Local time + IANA timezone ID, never a UTC offset.** Recurring jobs store
   the user's *intent* (`local_time + timezone_id + recurrence_rule`) and
   compute each concrete `scheduled_at_utc` per occurrence, so "08:00 New York"
   survives DST transitions. An offset only captures one moment in time.
5. **Time-bucketed due scan.** `job_runs.time_bucket` (hour-truncated epoch)
   lets the watcher scan only the current window's partitions instead of the
   whole table — the prototype shape of a high-throughput scheduler, and the
   natural key for native range partitioning later.
6. **Typed `RunEvent` rows instead of logs only.** Logs serve operators; a
   queryable, append-only event table serves the *product* — the API, the MCP
   tools, and the demo UI all read the same lifecycle history. Status updates
   and their event are written in one transaction so the audit trail can't
   silently diverge from the state.
7. **Reclaim window strictly longer than the message timeout.** Crash recovery
   uses Redis `XAUTOCLAIM`, but a message only becomes reclaimable *after* its
   original owner's execution deadline has certainly passed — so a stalled
   worker's run is recovered, while a slow-but-alive run is never double-executed.
8. **Linear chains before a DAG engine.** `parent_job_id` +
   `trigger_on_parent_status` covers the common "A then B" workflows with one
   column pair and no scheduler rewrite; fan-out/fan-in waits until the product
   needs it.
9. **LLM calls treated as an unreliable external dependency.** Per-job-type
   policy (timeout, retry budget, output-schema validation, cost ceiling) wraps
   every call; invalid output is never stored as success, and the daily cost
   guard reserves the *worst-case* retry budget upfront in a durable Postgres
   counter, so neither retries nor process restarts can blow the cap.
10. **Bounded metric labels.** With tenant ≈ user, `tenant_id` as a Prometheus
    label would be unbounded cardinality — per-tenant attribution lives in
    structured logs and the `tenant_llm_daily_cost` table instead, and metrics
    keep only bounded labels (`status`, `reason`).

## Domain model

| Entity | Meaning |
|--------|---------|
| `Job` | The task definition, scoped to `tenant_id` + `user_id` — one-off or recurring (`local_time` + `timezone_id` + `recurrence_rule`), optionally `side_effecting`, chained via `parent_job_id` + `trigger_on_parent_status`, dispatched by `job_type` (v1: `generic_llm`). |
| `JobRun` | A single execution attempt of a job, carrying `status`, `scheduled_at`, `time_bucket`, `attempts`, a deterministic `idempotency_key`, and error code/message + started/completed/failed timestamps. |
| `RunEvent` | Append-only typed audit row (`event_type` + JSONB payload) written on every status transition — `job_run.created/enqueued/started/succeeded/failed/retry_scheduled/dlq`, `job.created/cancelled`, `child_job.enqueued`, `llm.timeout/validation_failed`, `quota.deferred`, … |
| `TenantQuota` | Per-tenant limits: max jobs/hour, max active recurring jobs, max concurrent runs, max daily LLM cost (cents). Config-driven defaults apply when no row exists. |
| `IdempotencyRecord` | Atomic claim ledger for side-effecting handlers (`in_progress` → `completed`), keyed by `idempotency_key`. |

Domain entities hold unexported fields, validate invariants in `New<Entity>`
(IANA timezone, recurrence rule, owner, chain trigger status), and expose pure
status-transition methods (`MarkQueued` / `MarkRunning` / `MarkSuccess` /
`MarkRetry` / `MarkFailed` / `Cancel`).

## MCP tools

`cmd/mcp` exposes six tools over stdio, routed through an O(1) registry. The
MCP process acts as a service principal: its tenant/user identity comes from
`MCP_TENANT_ID` / `MCP_USER_ID` env vars (dev defaults apply when unset).

| Tool | Purpose |
|------|---------|
| `task.create` | Create a scheduled job. Args: `description`, plus either `scheduled_at` (RFC3339, one-off) or `schedule_type=recurring` with `local_time` (HH:MM), `timezone_id` (IANA), `recurrence_rule` (`FREQ=DAILY\|WEEKLY;INTERVAL=n`). Optional: `side_effecting`, `parent_job_id` + `trigger_on_parent_status` (chains), `job_type`, `original_user_text`. |
| `task.list` | List the caller's runs (`limit` / `offset`). |
| `task.status` | Job + latest-run status by `job_id`, including local schedule, `next_run_at_utc`, parent/children. |
| `task.cancel` | Cancel a job: all pending/queued/retry runs (and pending child runs) are cancelled; running runs are not force-stopped. |
| `task.runs` | List recent runs for one job (`job_id`, `limit` / `offset`). |
| `task.events` | Typed event history for a job run (`job_id` = run id). |

Verify with the MCP inspector:

```bash
npx @modelcontextprotocol/inspector go run ./cmd/mcp
```

A `task.create` with a past `scheduled_at` reaches a terminal status within
~10s; a future time + `task.cancel` ends as `cancelled`. A daily
`local_time=08:00, timezone_id=Asia/Taipei` job shows `next_run_at_utc` at
`00:00Z`.

## Browser demo

The API process serves a **zero-dependency single-page demo** at
[`http://localhost:8080/demo/`](http://localhost:8080/demo/) (the root `/`
redirects there). Three static files — vanilla HTML/CSS/JS, no framework, no
build step — are embedded into the binary with `go:embed`
(`internal/platform/httpserver/demo.go`), so the demo ships inside `cmd/api`
with nothing extra to deploy.

It exercises the real authenticated HTTP API end to end:

1. **Register / sign in** — calls `/api/v1/auth/register|login`; the JWT is kept
   in `localStorage` and sent as a `Bearer` token on every request.
2. **Jobs overview** — lists the caller's runs (`GET /jobs`) with
   pending/running/success counters, search, and refresh.
3. **Schedule a task** — one-off (`datetime-local`) or recurring (interval
   minutes) via `POST /jobs`.
4. **Inspect a run** — a detail drawer shows status, retries, failure
   code/message, the job's full run history (`GET /jobs/{id}/runs`), and the
   typed lifecycle event timeline (`GET /runs/{id}/events`) — you can watch
   `job_run.created → enqueued → started → succeeded` land in near-real time.
5. **Cancel** — `POST /jobs/{id}/cancel` cancels all pending runs (running runs
   are unaffected) and reports how many were cancelled.

Because the demo signs in as a real user, it also demonstrates tenant
isolation: each account only ever sees its own jobs, runs, and events.

### Docker Compose (one-command demo)

Brings up Postgres + Redis, applies migrations, then starts the API with its
background runners. No local Go, Postgres, Redis, or migrate CLI needed.

```bash
docker compose up --build
# Demo UI: http://localhost:8080/demo/  (register an account and schedule a job)
# API:     http://localhost:8080  (health: /healthz, metrics: /metrics)
# Postgres: localhost:5432  Redis: localhost:6379
# Tear down (and wipe the db volume):
docker compose down -v
```

The `migrate` service runs once and exits after applying migrations; the `api`
service waits for it to finish and for Postgres/Redis to report healthy before
starting. The MCP server is not part of the compose stack — run it locally
against the same datastores with `go run ./cmd/mcp`.

### Local (without Docker)

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
# then open the browser demo at http://localhost:8080/demo/

# 5. Run the MCP server against the same datastores
go run ./cmd/mcp
```

## Using this template

This repo is a starting point, not a library. When you fork it for a new project:

1. **Edit migration `0001` directly — don't stack new migrations on top.** Until
   you've shipped the first version, schema is not yet history. Modify
   `migrations/0001_*.up.sql` / `*.down.sql` in place to match your real domain.
   Only start creating `0002`, `0003`, … after the first real deploy. (This repo
   itself is past that point — its production-hardening pass shipped as stacked
   migrations `0002`–`0009`, which is the post-deploy mode of the same rule.)
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
cmd/mcp/main.go              # stdio MCP server (6 task.* tools, env identity)
internal/
  task/                      # scheduler feature slice
    domain.go                #   Job / JobRun / RunEvent + transitions, events
    service.go ports.go      #   Identity-scoped use-cases + outbound interfaces
    handler_http.go routes.go #  authenticated /jobs + /runs HTTP surface
    dto_http.go dto_internal.go # transport / persistence mapping
    recurrence.go            #   RRULE-subset parser + tz/DST next-occurrence
    executor.go              #   idempotency wrapper + LLM reliability executor
    llm.go llm_fake.go       #   LLMClient port types, schema validation, fake
    repo_postgres.go         #   sqlc-backed persistence (tx transitions, slots)
    repo_quota_postgres.go   #   quotas + durable daily LLM cost reservation
    idempotency_postgres.go  #   idempotency_records claim store
    queue_redis.go           #   Redis Streams queue + DLQ
    watcher.go               #   scans due runs → queue (revert on enqueue fail)
    worker.go                #   lifecycle, tenant slots, retry/DLQ, chains
    recurring_watcher.go     #   tz-aware next run for recurring jobs
    metrics.go               #   feature Prometheus metrics
    mcp/                     #   MCP tool registry + handlers
  user/                      # JWT auth feature slice
  shared/                    # zero-dependency value objects (IDs, …)
  bootstrap/                 # composition root — wires features + runners
  platform/                  # infrastructure (pgx, redis, jwt, otel, …)
    httpserver/demo/         #   embedded browser demo (vanilla HTML/CSS/JS)
migrations/                  # golang-migrate 0001–0009 (tenancy, events, quotas,
                             #   recurrence, idempotency, chains, llm cost)
sql/queries/                 # sqlc input
api/openapi.yaml             # HTTP API contract (source of truth)
observability/               # Grafana dashboard JSON + Prometheus alert rules
docs/adr/                    # architecture decision records
test/integration/            # integration tests against local Postgres
thoughts/qrspi/              # ticket / research / design / plan artifacts
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
make verify               # lint + unit tests (CI / pre-commit gate)
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

The scheduler now has the production controls (tenancy, quotas, idempotency,
chains, LLM guards) without proving load. Deliberately deferred (see
`thoughts/qrspi/2026-06-08-production-improvement-spec/design.md`,
"What We're NOT Doing"):

- **Real Anthropic `LLMClient` adapter** — the reliability layer is wired to a
  deterministic fake; the production adapter is a thin add behind the port.
- **Per-tenant queues / fair scheduling** — one shared stream today;
  `tenant_id` already rides in `JobRunMsg`, and an intra-batch per-tenant
  round-robin is the only down-payment.
- **DLQ consumer / reprocessing pipeline** — `task:dlq` has no consumer yet.
- **Real tenant model** — v1 maps each user 1:1 to a tenant via a
  `TenantResolver` port; swap in a `tenants` table + lookup later.
- **Native range partitioning** on `job_runs` by `time_bucket` for scale.
- **Full RFC-5545 RRULE / cron** — only the `FREQ=DAILY|WEEKLY` subset.
- **MCP auth** — the stdio transport trusts its configured service-principal
  identity (local use only).

Each fits the existing structure with no architectural changes.
