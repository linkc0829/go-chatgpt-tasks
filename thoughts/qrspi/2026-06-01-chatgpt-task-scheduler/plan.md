# Implementation Plan — ChatGPT Task Scheduler

## Overview

Add a `task` feature slice (hex layout) plus a stdio MCP transport, then layer a
Redis-Streams-decoupled execution pipeline (watcher → queue → worker pool → DLQ) and a
recurring-job rescheduler, all as supervised goroutines under the existing
`App.Run`/`App.Shutdown` lifecycle. Real task execution is a stub `Executor`.

## Confirmed decisions (from structure Open Risks)

- **Migrations: fold into `0001`.** New tables (`jobs`, `job_runs`, `run_events`) are added
  to `migrations/0001_init.up.sql` / `.down.sql` (R0.1 template mode). No `0002` pair.
- **MCP SDK: official `github.com/modelcontextprotocol/go-sdk`.** Pin a version; isolate behind
  `internal/task/mcp` registry. API (verified): `mcp.NewServer(*Implementation, *ServerOptions)`,
  `mcp.AddTool[In,Out](s, *Tool, handler)`, handler `func(ctx, *CallToolRequest, In) (*CallToolResult, Out, error)`,
  serve via `server.Run(ctx, &mcp.StdioTransport{})`.
- **Partitioning: single `job_runs` table + `time_bucket` index** (design Decision 5 fallback).
  Native range partitioning deferred.

## Conventions to follow (from research)

- Strongly-typed IDs in `internal/shared/ids.go` (add `JobID`, `JobRunID`, `RunEventID`).
- Domain: unexported fields, `New<Entity>` validates, pure ctx-free transitions, `rehydrate(...)`.
- Service takes interfaces only; wrap errors `fmt.Errorf("...: %w", err)`.
- Repo: `PostgresRepo{q *sqlc.Queries}`, `New(pool)` wraps `sqlc.New`; `pgx.ErrNoRows → Err...NotFound`;
  all SQL via sqlc; `pgtype` confined via `postgres.UUIDToPg/PgToUUID/TimeToPg/PgToTime`.
- sqlc queries use `sqlc.arg(name)`; annotations `:exec` / `:execrows` / `:one` / `:many`.
- Cross-feature isolation block per feature in `.golangci.yml`.

---

## Phase 1: Task feature core + MCP CRUD surface

Delivers the `task` slice end-to-end with no execution: 4 MCP tools persist and read
`Job`/`JobRun` rows. `task.create` writes a `pending` JobRun; list/status/cancel work.

### Changes

#### 1. Schema — fold new tables into existing migration
**File**: `migrations/0001_init.up.sql`
**Action**: modify (append after the `payments` block)

```sql
-- Jobs ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS jobs (
    id               UUID PRIMARY KEY,
    kind             TEXT NOT NULL CHECK (kind IN ('one_off', 'recurring')),
    description      TEXT NOT NULL,
    interval_seconds BIGINT NOT NULL DEFAULT 0 CHECK (interval_seconds >= 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Job runs (one execution attempt of a job) -----------------------------
CREATE TABLE IF NOT EXISTS job_runs (
    id           UUID PRIMARY KEY,
    job_id       UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    sequence     INT  NOT NULL,
    status       TEXT NOT NULL CHECK (status IN
                   ('pending','queued','running','success','retry','failed','cancelled')),
    scheduled_at TIMESTAMPTZ NOT NULL,
    time_bucket  BIGINT NOT NULL,         -- unix epoch hour (scheduled_at truncated to hour)
    attempts     INT  NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (job_id, sequence)             -- guards recurring double-creation (design Open Risk)
);

-- Time-bucket-partitioned due scan: status='pending' AND bucket=$ AND scheduled_at<=$.
CREATE INDEX IF NOT EXISTS idx_job_runs_due
    ON job_runs (time_bucket, status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_job_runs_job ON job_runs (job_id);

-- Run events (append-only audit; recurring watcher polls terminal events) -
CREATE TABLE IF NOT EXISTS run_events (
    id         UUID PRIMARY KEY,
    job_run_id UUID NOT NULL REFERENCES job_runs(id) ON DELETE RESTRICT,
    status     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_run_events_run     ON run_events (job_run_id);
CREATE INDEX IF NOT EXISTS idx_run_events_created ON run_events (created_at);
```

**File**: `migrations/0001_init.down.sql`
**Action**: modify — drop new tables in reverse dependency order, before the existing drops:

```sql
DROP TABLE IF EXISTS run_events;
DROP TABLE IF EXISTS job_runs;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;
```

#### 2. Shared IDs
**File**: `internal/shared/ids.go`
**Action**: modify — add three ID types mirroring the existing `OrderID` pattern (type alias,
`New*`, `String`, `IsZero`, `MarshalText`/`UnmarshalText`, `Value`/`Scan`, `Parse*`).

```go
type JobID uuid.UUID
type JobRunID uuid.UUID
type RunEventID uuid.UUID
// + New*/String/IsZero/MarshalText/UnmarshalText/Value/Scan/Parse* for each,
//   copied verbatim from the OrderID block (lines 28-135).
```

#### 3. sqlc queries
**File**: `sql/queries/task.sql`
**Action**: create

```sql
-- name: InsertJob :exec
INSERT INTO jobs (id, kind, description, interval_seconds, created_at, updated_at)
VALUES (sqlc.arg(id), sqlc.arg(kind), sqlc.arg(description),
        sqlc.arg(interval_seconds), sqlc.arg(created_at), sqlc.arg(updated_at));

-- name: InsertJobRun :exec
INSERT INTO job_runs (id, job_id, sequence, status, scheduled_at, time_bucket,
                      attempts, created_at, updated_at)
VALUES (sqlc.arg(id), sqlc.arg(job_id), sqlc.arg(sequence), sqlc.arg(status),
        sqlc.arg(scheduled_at), sqlc.arg(time_bucket), sqlc.arg(attempts),
        sqlc.arg(created_at), sqlc.arg(updated_at));

-- name: UpdateJobRunStatus :execrows
UPDATE job_runs
SET status = sqlc.arg(status), attempts = sqlc.arg(attempts), updated_at = sqlc.arg(updated_at)
WHERE id = sqlc.arg(id);

-- name: GetJobRunByID :one
SELECT id, job_id, sequence, status, scheduled_at, time_bucket, attempts, created_at, updated_at
FROM job_runs WHERE id = sqlc.arg(id);

-- name: ListJobRuns :many
SELECT id, job_id, sequence, status, scheduled_at, time_bucket, attempts, created_at, updated_at
FROM job_runs ORDER BY created_at DESC
LIMIT sqlc.arg(page_limit) OFFSET sqlc.arg(page_offset);

-- name: CountJobRuns :one
SELECT COUNT(*) FROM job_runs;

-- name: FindDueJobRuns :many   -- watcher (Phase 2)
SELECT id, job_id, sequence, status, scheduled_at, time_bucket, attempts, created_at, updated_at
FROM job_runs
WHERE time_bucket = sqlc.arg(time_bucket)
  AND status = 'pending'
  AND scheduled_at <= sqlc.arg(due_before)
ORDER BY scheduled_at
LIMIT sqlc.arg(lim);

-- name: InsertRunEvent :exec   -- worker / transitions (Phase 3)
INSERT INTO run_events (id, job_run_id, status, created_at)
VALUES (sqlc.arg(id), sqlc.arg(job_run_id), sqlc.arg(status), sqlc.arg(created_at));

-- name: GetJobByID :one        -- recurring watcher (Phase 4)
SELECT id, kind, description, interval_seconds, created_at, updated_at
FROM jobs WHERE id = sqlc.arg(id);

-- name: ListTerminalRecurringRuns :many  -- recurring watcher (Phase 4)
SELECT r.id, r.job_id, r.sequence, r.scheduled_at, j.interval_seconds
FROM run_events e
JOIN job_runs r ON r.id = e.job_run_id
JOIN jobs     j ON j.id = r.job_id
WHERE e.status IN ('success','failed')
  AND j.kind = 'recurring'
  AND e.created_at >= sqlc.arg(since)
  AND NOT EXISTS (
        SELECT 1 FROM job_runs n WHERE n.job_id = r.job_id AND n.sequence = r.sequence + 1)
ORDER BY e.created_at
LIMIT sqlc.arg(lim);

-- name: InsertJobRunIfAbsent :execrows  -- recurring watcher idempotent insert (Phase 4)
INSERT INTO job_runs (id, job_id, sequence, status, scheduled_at, time_bucket,
                      attempts, created_at, updated_at)
VALUES (sqlc.arg(id), sqlc.arg(job_id), sqlc.arg(sequence), 'pending',
        sqlc.arg(scheduled_at), sqlc.arg(time_bucket), 0,
        sqlc.arg(created_at), sqlc.arg(updated_at))
ON CONFLICT (job_id, sequence) DO NOTHING;
```

> Then run `make sqlc-generate`. **If sqlc is unavailable**, hand-write the generated additions in
> `internal/platform/postgres/sqlc/`: `Job`/`JobRun`/`RunEvent` row structs + `*Params` structs in
> `models.go` (use `pgtype.UUID`/`pgtype.Timestamptz`/`int64`/`int32`/`string`), the query
> funcs + SQL consts in a new `task.sql.go`, and add the new methods to the `Querier` interface in
> `querier.go`. Mirror the existing `order` generated code exactly.

#### 4. Domain
**File**: `internal/task/domain.go`
**Action**: create

```go
package task // domain.go imports: time + internal/shared only (R1.1)

type Kind string
const ( KindOneOff Kind = "one_off"; KindRecurring Kind = "recurring" )

type Status string
const (
    StatusPending   Status = "pending"
    StatusQueued    Status = "queued"
    StatusRunning   Status = "running"
    StatusSuccess   Status = "success"
    StatusRetry     Status = "retry"
    StatusFailed    Status = "failed"
    StatusCancelled Status = "cancelled"
)

type Job struct {
    id          shared.JobID
    kind        Kind
    description string
    interval    time.Duration // 0 for one-off; next-run step for recurring
    createdAt, updatedAt time.Time
}

type JobRun struct {
    id          shared.JobRunID
    jobID       shared.JobID
    sequence    int
    status      Status
    scheduledAt time.Time
    timeBucket  int64
    attempts    int
    createdAt, updatedAt time.Time
}

type RunEvent struct {
    id       shared.RunEventID
    jobRunID shared.JobRunID
    status   Status
    createdAt time.Time
}

// bucketOf truncates to the unix epoch hour used by the watcher's partition scan.
func bucketOf(t time.Time) int64 { return t.UTC().Truncate(time.Hour).Unix() }

// NewJob validates: non-empty description; recurring ⇒ interval > 0.
func NewJob(kind Kind, description string, interval time.Duration) (*Job, error)
// NewJobRun: sequence >= 1; scheduledAt non-zero. Sets status=pending, timeBucket=bucketOf(scheduledAt).
func NewJobRun(jobID shared.JobID, sequence int, scheduledAt time.Time) (*JobRun, error)
func NewRunEvent(runID shared.JobRunID, s Status) *RunEvent

func rehydrateJob(...) *Job
func rehydrateJobRun(...) *JobRun

// Pure transitions (mutate status + updatedAt; return ErrInvalidStatusTransition on illegal moves):
func (r *JobRun) MarkQueued() error   // pending|retry → queued
func (r *JobRun) MarkRunning() error  // queued        → running
func (r *JobRun) MarkSuccess() error  // running       → success
func (r *JobRun) MarkRetry() error    // running       → retry  ; r.attempts++
func (r *JobRun) MarkFailed() error   // running|retry → failed
func (r *JobRun) Cancel() error       // not terminal  → cancelled

func (r *JobRun) IsTerminal() bool // success|failed|cancelled
// getters: ID/JobID/Sequence/Status/ScheduledAt/TimeBucket/Attempts/CreatedAt/UpdatedAt
// Job getters: ID/Kind/Description/Interval/CreatedAt/UpdatedAt
```

#### 5. Errors
**File**: `internal/task/errors.go`
**Action**: create

```go
var (
    ErrJobNotFound            = errors.New("job not found")
    ErrJobRunNotFound         = errors.New("job run not found")
    ErrInvalidDescription     = errors.New("invalid description")
    ErrInvalidSchedule        = errors.New("invalid schedule")
    ErrInvalidStatusTransition = errors.New("invalid status transition")
)
```

#### 6. Ports (Phase-1 subset; `Queue`/`Executor` added in P2/P3)
**File**: `internal/task/ports.go`
**Action**: create — imports `context` + `time` + `shared` only.

```go
//go:generate mockgen -source=ports.go -destination=mocks/mock_ports.go -package=mocks
type Repo interface {
    SaveJob(ctx context.Context, j *Job) error
    SaveRun(ctx context.Context, r *JobRun) error
    UpdateRunStatus(ctx context.Context, r *JobRun) error
    FindRunByID(ctx context.Context, id shared.JobRunID) (*JobRun, error)
    ListRuns(ctx context.Context, p shared.Pagination) ([]*JobRun, int64, error)
    AppendEvent(ctx context.Context, e *RunEvent) error
    // Phase 2:
    FindDueRuns(ctx context.Context, bucket int64, before time.Time, limit int32) ([]*JobRun, error)
    // Phase 4:
    FindJob(ctx context.Context, id shared.JobID) (*Job, error)
    InsertRunIfAbsent(ctx context.Context, r *JobRun) (created bool, err error)
    FindTerminalRecurringRuns(ctx context.Context, since time.Time, limit int32) ([]NextRunSpec, error)
}

// NextRunSpec is the projection the recurring watcher needs (defined in dto_internal.go).
```

> Declare the full `Repo` interface now (all phases) so `PostgresRepo` implements one stable
> contract; P2–P4 only add the adapter bodies + queries, not new interface churn.

#### 7. Service
**File**: `internal/task/service.go`
**Action**: create — `NewService(repo Repo) *Service`. No auth/owner scoping (MCP has no auth).

```go
type CreateInput struct {
    Description string
    ScheduledAt time.Time
    Interval    time.Duration // 0 ⇒ one-off, else recurring
}

// Create builds a Job + its first JobRun (sequence 1, pending) and persists both.
func (s *Service) Create(ctx context.Context, in CreateInput) (*JobRun, error) {
    kind := KindOneOff
    if in.Interval > 0 { kind = KindRecurring }
    j, err := NewJob(kind, in.Description, in.Interval)
    if err != nil { return nil, err }
    if err := s.repo.SaveJob(ctx, j); err != nil { return nil, fmt.Errorf("save job: %w", err) }
    run, err := NewJobRun(j.ID(), 1, in.ScheduledAt)
    if err != nil { return nil, err }
    if err := s.repo.SaveRun(ctx, run); err != nil { return nil, fmt.Errorf("save run: %w", err) }
    return run, nil
}

func (s *Service) List(ctx context.Context, p shared.Pagination) ([]*JobRun, int64, error)
func (s *Service) Status(ctx context.Context, id shared.JobRunID) (*JobRun, error) // FindRunByID
func (s *Service) Cancel(ctx context.Context, id shared.JobRunID) (*JobRun, error) {
    run, err := s.repo.FindRunByID(ctx, id)
    if err != nil { return nil, err }
    if err := run.Cancel(); err != nil { return nil, err }
    if err := s.repo.UpdateRunStatus(ctx, run); err != nil { return nil, fmt.Errorf("update run: %w", err) }
    _ = s.repo.AppendEvent(ctx, NewRunEvent(run.ID(), StatusCancelled)) // audit; best-effort
    return run, nil
}
```

#### 8. DTO internal (repo mappers)
**File**: `internal/task/dto_internal.go`
**Action**: create — mirror `order/dto_internal.go`.

```go
func jobFromSqlc(r sqlc.Job) *Job                       // rehydrateJob; interval = time.Duration(r.IntervalSeconds)*time.Second
func jobToInsertParams(j *Job) sqlc.InsertJobParams
func jobRunFromSqlc(r sqlc.JobRun) *JobRun              // rehydrateJobRun
func jobRunToInsertParams(r *JobRun) sqlc.InsertJobRunParams
func jobRunToUpdateStatusParams(r *JobRun) sqlc.UpdateJobRunStatusParams // id, status, attempts, updated_at
func runEventToInsertParams(e *RunEvent) sqlc.InsertRunEventParams

// NextRunSpec is the projection returned by FindTerminalRecurringRuns (Phase 4):
type NextRunSpec struct {
    JobID       shared.JobID
    Sequence    int
    ScheduledAt time.Time
    Interval    time.Duration
}
func nextRunSpecFromSqlc(r sqlc.ListTerminalRecurringRunsRow) NextRunSpec
```

#### 9. Repo adapter (Phase-1 methods; P2-P4 method bodies in later phases)
**File**: `internal/task/repo_postgres.go`
**Action**: create — `PostgresRepo{q *sqlc.Queries}`, `NewPostgresRepo(pool *pgxpool.Pool)`.
Implement `SaveJob`, `SaveRun`, `UpdateRunStatus` (`rows==0 → ErrJobRunNotFound`), `FindRunByID`
(`pgx.ErrNoRows → ErrJobRunNotFound`), `ListRuns` (+`CountJobRuns`), `AppendEvent`. Stub the
P2–P4 methods to satisfy the interface now, or add them as you reach each phase (compile gate:
`var _ Repo = (*PostgresRepo)(nil)`).

#### 10. MCP registry
**File**: `internal/task/mcp/registry.go`
**Action**: create — O(1) dispatch map (ticket Q5).

```go
package mcp // internal/task/mcp

// toolHandler parses raw JSON args and returns a JSON-serializable result.
type toolHandler func(ctx context.Context, raw json.RawMessage) (any, error)

type Registry struct{ handlers map[string]toolHandler }

func NewRegistry() *Registry { return &Registry{handlers: map[string]toolHandler{}} }
func (r *Registry) Register(name string, h toolHandler) { r.handlers[name] = h }
func (r *Registry) Handlers() map[string]toolHandler { return r.handlers }
```

#### 11. MCP tools (bind service → registry; the only place that maps args)
**File**: `internal/task/mcp/tools.go`
**Action**: create

```go
// ToolService is the inbound contract the MCP adapter depends on (mirror of handler→service).
type ToolService interface {
    Create(ctx context.Context, in task.CreateInput) (*task.JobRun, error)
    List(ctx context.Context, p shared.Pagination) ([]*task.JobRun, int64, error)
    Status(ctx context.Context, id shared.JobRunID) (*task.JobRun, error)
    Cancel(ctx context.Context, id shared.JobRunID) (*task.JobRun, error)
}

type createArgs struct {
    Description             string `json:"description"`
    ScheduledAt            string `json:"scheduled_at"`              // RFC3339
    RecurringIntervalSeconds int64 `json:"recurring_interval_seconds,omitempty"`
}
type runRef struct { JobID string `json:"job_id"` } // job_id = JobRun ID string

// runResponse mirrors ticket {"job_id":..., "status":...}
type runResponse struct {
    JobID       string `json:"job_id"`
    Status      string `json:"status"`
    ScheduledAt string `json:"scheduled_at"`
}

// Register wires the 4 task tools into the registry. Each: unmarshal → call svc → map.
func Register(reg *Registry, svc ToolService) {
    reg.Register("task.create", func(ctx context.Context, raw json.RawMessage) (any, error) { ... })
    reg.Register("task.list",   func(ctx context.Context, raw json.RawMessage) (any, error) { ... })
    reg.Register("task.status", func(ctx context.Context, raw json.RawMessage) (any, error) { ... })
    reg.Register("task.cancel", func(ctx context.Context, raw json.RawMessage) (any, error) { ... })
}
```

> `task.create` parses `scheduled_at` with `time.Parse(time.RFC3339, ...)`; on parse error return a
> Go error (the SDK surfaces it as `isError`). Maps `task.CreateInput{Interval:
> time.Duration(RecurringIntervalSeconds)*time.Second}`. `status`/`cancel` parse `runRef.JobID` via
> `shared.ParseJobRunID`.

#### 12. MCP server entrypoint
**File**: `cmd/mcp/main.go`
**Action**: create — second binary, stdio transport. Builds only what CRUD needs (Postgres; no
HTTP, no Redis).

```go
func main() {
    cfg, err := config.Load(); if err != nil { log.Fatalf("config: %v", err) }
    ctx := context.Background()
    pool, err := postgres.New(ctx, postgres.Config{DSN: cfg.DB.DSN, MaxConns: cfg.DB.MaxConns, MinConns: cfg.DB.MinConns})
    if err != nil { log.Fatalf("postgres: %v", err) }
    defer pool.Close()

    svc := task.NewService(task.NewPostgresRepo(pool))
    reg := taskmcp.NewRegistry()
    taskmcp.Register(reg, svc)

    server := mcp.NewServer(&mcp.Implementation{Name: "task-scheduler", Version: "0.1.0"}, nil)
    // Bridge registry → SDK: register each registry entry as an SDK tool that delegates.
    bindRegistry(server, reg) // see below
    if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil { log.Fatalf("mcp serve: %v", err) }
}
```

`bindRegistry` adapts our registry to the SDK's generic `AddTool`. Use a `map[string]any` input so the
SDK passes raw args through, re-marshal to `json.RawMessage`, then call the registry handler:

```go
func bindRegistry(s *mcp.Server, reg *taskmcp.Registry) {
    for name, h := range reg.Handlers() {
        h := h
        mcp.AddTool(s, &mcp.Tool{Name: name, Description: descFor(name)},
            func(ctx context.Context, _ *mcp.CallToolRequest, args map[string]any) (*mcp.CallToolResult, any, error) {
                raw, _ := json.Marshal(args)
                out, err := h(ctx, raw)
                if err != nil { return nil, nil, err }
                b, _ := json.Marshal(out)
                return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(b)}}}, nil, nil
            })
    }
}
```

> Deviation from structure: structure described the registry as a bare `map[string]toolHandler` that
> "serves stdio" directly. The official SDK owns transport + per-tool registration, so the registry
> stays the source of truth for routing (ticket Q5 satisfied) and `bindRegistry` is a thin bridge to
> the SDK. No business logic in either (R1.3).

#### 13. go.mod
**File**: `go.mod`
**Action**: modify — `go get github.com/modelcontextprotocol/go-sdk@latest` then `go mod tidy`.
If the import path/version pins differently, record the exact version. Isolate all SDK imports to
`cmd/mcp/main.go` (the bridge); `internal/task/mcp/*` stays SDK-free.

#### 14. Cross-feature depguard block
**File**: `.golangci.yml`
**Action**: modify — add after `no-cross-feature-user`:

```yaml
      no-cross-feature-task:
        files:
          - "**/internal/task/**"
        deny:
          - pkg: "github.com/linkc0829/go-backend-template/internal/order"
            desc: "Cross-feature import forbidden."
          - pkg: "github.com/linkc0829/go-backend-template/internal/payment"
            desc: "Cross-feature import forbidden."
          - pkg: "github.com/linkc0829/go-backend-template/internal/user"
            desc: "Cross-feature import forbidden."
```

#### 15. Mocks
**File**: `internal/task/mocks/mock_ports.go`
**Action**: generate via `make mock-gen` (or `go generate ./internal/task/...`). If mockgen is
unavailable, hand-write a `MockRepo` implementing `Repo` for the service test.

### Verification
#### Automated
- [x] `make sqlc-generate` succeeds (or generated code hand-added) — `internal/platform/postgres/sqlc` has `Job`/`JobRun`/`RunEvent` + new query methods.
- [x] `make mock-gen` regenerates `internal/task/mocks`.
- [x] `go build ./...` compiles both `cmd/api` and `cmd/mcp`.
- [x] `make lint` passes (depguard: `domain.go` stdlib+shared only; `service.go` no drivers; new `no-cross-feature-task` block present).
- [x] `make test` passes — add `internal/task/service_test.go` (gomock): create→pending, cancel→cancelled, cancel-terminal→`ErrInvalidStatusTransition`, status-not-found→`ErrJobRunNotFound`.

#### Manual
- [ ] `migrate-up` against local Postgres creates `jobs`/`job_runs`/`run_events` (check `\d job_runs`, `idx_job_runs_due` exists).
- [ ] `npx @modelcontextprotocol/inspector go run ./cmd/mcp` → **Connect** shows 4 tools: `task.create`, `task.list`, `task.status`, `task.cancel`.
- [ ] `task.create` with `description="Summarize tech news"`, `scheduled_at="2025-01-01T00:00:00Z"` → response includes `{"job_id": "...", "status": "pending"}`.
- [ ] `task.status` with that `job_id` → still `"pending"` (no watcher yet).
- [ ] `task.create` future `scheduled_at="2099-12-31T00:00:00Z"` → `task.cancel` that `job_id` → `"cancelled"`. `task.list` shows both runs.

---

## Phase 2: Redis Streams queue + watcher (pending → queued)

Adds the queue adapter and a supervised watcher goroutine that scans due `pending` runs in the
current time bucket(s) and publishes them, marking them `queued`. Establishes the supervised-goroutine
lifecycle extension.

### Changes

#### 1. Queue port + message DTO
**File**: `internal/task/ports.go`
**Action**: modify — add (imports stay `context`+`time`+`shared`):

```go
type Queue interface {
    Enqueue(ctx context.Context, m JobRunMsg) error
}
```

**File**: `internal/task/dto_internal.go`
**Action**: modify — add the wire message (no redis types here):

```go
type JobRunMsg struct {
    JobRunID string `json:"job_run_id"` // idempotency key (ticket lines 31-32)
    Attempts int    `json:"attempts"`
}
```

#### 2. Redis queue adapter (NEW adapter pattern; mirrors repo boundary)
**File**: `internal/task/queue_redis.go`
**Action**: create — redis types confined here (R2 `cache_redis.go` slot).

```go
const (
    streamMain = "task:runs"
    streamDLQ  = "task:dlq"
    groupName  = "task-workers"
)

type RedisQueue struct{ rdb *redis.Client }
func NewRedisQueue(rdb *redis.Client) *RedisQueue { return &RedisQueue{rdb: rdb} }

func (q *RedisQueue) Enqueue(ctx context.Context, m JobRunMsg) error {
    b, err := json.Marshal(m); if err != nil { return fmt.Errorf("marshal msg: %w", err) }
    if err := q.rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamMain, Values: map[string]any{"data": b}}).Err(); err != nil {
        return fmt.Errorf("xadd: %w", err)
    }
    return nil
}
```

> Phase 3 adds `EnsureGroup`, `Read`, `Ack`, `Reclaim`, `DeadLetter` to this file.

#### 3. Watcher (supervised goroutine)
**File**: `internal/task/watcher.go`
**Action**: create

```go
// Runner is the supervised-goroutine contract (watcher, worker, recurring watcher all satisfy it).
type Runner interface { Run(ctx context.Context) error }

type Watcher struct {
    repo     Repo
    queue    Queue
    interval time.Duration
    horizon  time.Duration // due window, 5*time.Minute (ticket line 22)
    limit    int32
    log      *zap.Logger
}

func NewWatcher(repo Repo, queue Queue, interval time.Duration, log *zap.Logger) *Watcher

func (w *Watcher) Run(ctx context.Context) error {
    t := time.NewTicker(w.interval); defer t.Stop()
    for {
        select {
        case <-ctx.Done(): return nil
        case <-t.C:       w.scanOnce(ctx)
        }
    }
}

// scanOnce queries due pending runs in the buckets covering [now, now+horizon],
// enqueues each, then MarkQueued + UpdateRunStatus. Per-iteration context.WithTimeout (R3.2).
func (w *Watcher) scanOnce(ctx context.Context) {
    cctx, cancel := context.WithTimeout(ctx, 5*time.Second); defer cancel()
    now := time.Now().UTC(); before := now.Add(w.horizon)
    for _, bucket := range bucketsInRange(now, before) { // bucketOf(now) and bucketOf(before), deduped
        runs, err := w.repo.FindDueRuns(cctx, bucket, before, w.limit)
        if err != nil { w.log.Error("watcher find due", zap.Error(err)); continue }
        for _, r := range runs {
            if err := w.queue.Enqueue(cctx, JobRunMsg{JobRunID: r.ID().String(), Attempts: r.Attempts()}); err != nil {
                w.log.Error("watcher enqueue", zap.Error(err)); continue
            }
            if err := r.MarkQueued(); err != nil { continue }
            if err := w.repo.UpdateRunStatus(cctx, r); err != nil {
                w.log.Error("watcher mark queued", zap.Error(err))
            }
        }
    }
}
```

> `bucketsInRange` lives in `watcher.go` (or `domain.go` next to `bucketOf`): returns the distinct
> hour buckets touching `[from, to]` so a run scheduled just past an hour boundary is still found.

#### 4. Repo: implement `FindDueRuns`
**File**: `internal/task/repo_postgres.go`
**Action**: modify — implement using `q.FindDueJobRuns(ctx, sqlc.FindDueJobRunsParams{TimeBucket: bucket, DueBefore: postgres.TimeToPg(before), Lim: limit})`, map rows via `jobRunFromSqlc`.

#### 5. Lifecycle: supervise background runners
**File**: `internal/bootstrap/app.go`
**Action**: modify — add fields + launch logic.

```go
type App struct {
    // ...existing...
    runners  []task.Runner
    bgCancel context.CancelFunc
    bgWG     sync.WaitGroup
}

// In NewApp: capture runners returned by wireFeatures.
runners := wireFeatures(engine, pool, rdb, authMgr, lg)
// store on App: runners: runners,

func (a *App) Run() error {
    bgCtx, cancel := context.WithCancel(context.Background())
    a.bgCancel = cancel
    for _, r := range a.runners {
        a.bgWG.Add(1)
        go func(r task.Runner) {
            defer a.bgWG.Done()
            if err := r.Run(bgCtx); err != nil && !errors.Is(err, context.Canceled) {
                a.logger.Error("background runner stopped", zap.Error(err))
            }
        }(r)
    }
    return a.server.Start() // blocks
}
```

**File**: `internal/bootstrap/shutdown.go`
**Action**: modify — cancel + wait for background runners AFTER stopping HTTP, BEFORE closing
redis/PG (runners use them):

```go
func (a *App) Shutdown(ctx context.Context) {
    if err := a.server.Shutdown(ctx); err != nil { a.logger.Error("http server shutdown", zap.Error(err)) }
    if a.bgCancel != nil { a.bgCancel() }
    a.bgWG.Wait() // wait for watcher/workers to drain before tearing down their deps
    if err := a.otelShutdown(ctx); err != nil { a.logger.Error("otel shutdown", zap.Error(err)) }
    if err := a.rdb.Close(); err != nil { a.logger.Error("redis close", zap.Error(err)) }
    a.pool.Close()
    _ = a.logger.Sync()
}
```

#### 6. Wire the queue + watcher
**File**: `internal/bootstrap/wire.go`
**Action**: modify — change `wireFeatures` to build the task slice and **return** `[]task.Runner`.
Use the previously-blank `rdb` param.

```go
func wireFeatures(engine *gin.Engine, pool *pgxpool.Pool, rdb *redis.Client, authMgr *auth.Manager, lg *zap.Logger) []task.Runner {
    // ...existing user/payment/order wiring (unchanged)...

    // Task feature (queue + watcher; workers/recurring added in P3/P4).
    taskRepo := task.NewPostgresRepo(pool)
    taskQueue := task.NewRedisQueue(rdb)
    watcher := task.NewWatcher(taskRepo, taskQueue, 5*time.Second, lg)
    return []task.Runner{watcher}
}
```

> `task` has no HTTP routes (MCP-only inbound), so it is not registered on `engine`. Update the
> `app.go` call site to capture the return value.

### Verification
#### Automated
- [ ] `make test` passes — `internal/task/watcher_test.go` (gomock Repo+Queue): due pending run → `Enqueue` called once + `UpdateRunStatus` to `queued`; `FindDueRuns` error → no enqueue, no crash; ctx cancel → `Run` returns nil.
- [ ] `make lint` passes — `queue_redis.go` is the only task file importing `go-redis`; `watcher.go`/`service.go` driver-free.
- [ ] `go build ./...`.

#### Manual
- [ ] Local Postgres + Redis up. `make run`. Via inspector (`go run ./cmd/mcp`) `task.create` with past `scheduled_at` → within one watcher tick (~5s) `task.status` shows `"queued"`.
- [ ] `redis-cli XLEN task:runs` ≥ 1 (message published).
- [ ] `Ctrl+C` on `cmd/api` → process exits cleanly within shutdown timeout (no goroutine-leak hang; watcher stops on ctx cancel).

---

## Phase 3: Worker pool + executor + transitions + DLQ

Workers consume the stream, execute via stub `Executor`, drive
`queued → running → success | retry → failed→DLQ`, writing a `RunEvent` per transition. Satisfies the
ticket's core check (past-time job → completed ~10s) and at-least-once (ticket lines 28-32).

### Changes

#### 1. Executor port + stub
**File**: `internal/task/ports.go`
**Action**: modify — add:

```go
type Executor interface { Execute(ctx context.Context, r *JobRun) error }
```

**File**: `internal/task/executor.go`
**Action**: create

```go
type StubExecutor struct {
    log      *zap.Logger
    failFunc func(*JobRun) error // nil in prod; tests inject transient/permanent failures
}
func NewStubExecutor(log *zap.Logger) *StubExecutor { return &StubExecutor{log: log} }
func (e *StubExecutor) Execute(ctx context.Context, r *JobRun) error {
    if e.failFunc != nil { return e.failFunc(r) }
    e.log.Info("executed job run", zap.String("job_run_id", r.ID().String()))
    return nil
}
```

#### 2. Queue: consumer group, ack, claim, DLQ
**File**: `internal/task/queue_redis.go`
**Action**: modify — add to the `Queue` interface (ports.go) and implement:

```go
// ports.go additions:
type Queue interface {
    Enqueue(ctx context.Context, m JobRunMsg) error
    EnsureGroup(ctx context.Context) error
    Read(ctx context.Context, consumer string, count int64, block time.Duration) ([]QueuedMessage, error)
    Reclaim(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]QueuedMessage, error)
    Ack(ctx context.Context, streamID string) error
    DeadLetter(ctx context.Context, m JobRunMsg) error
}

type QueuedMessage struct { StreamID string; Msg JobRunMsg } // StreamID = redis entry ID for XACK
```

```go
// EnsureGroup: XGroupCreateMkStream(streamMain, groupName, "0"); ignore BUSYGROUP error.
// Read:    XReadGroup{Group, Consumer, Streams:[streamMain,">"], Count, Block} → decode "data" → []QueuedMessage.
// Reclaim: XAutoClaim{Stream, Group, Consumer, MinIdle, Start:"0", Count} → redeliver stuck msgs (visibility timeout).
// Ack:     XAck(streamMain, groupName, streamID).
// DeadLetter: XAdd(streamDLQ, {"data": json}).
```

> **Visibility timeout (design Open Risk):** `minIdle = 30s` for `Reclaim`; workers `Read` with
> `block = 5s`. Document these as the tunables.

#### 3. Worker (supervised goroutine)
**File**: `internal/task/worker.go`
**Action**: create

```go
const maxAttempts = 3 // ticket "exceed max retry → failed + DLQ"

type Worker struct {
    id    string
    repo  Repo
    queue Queue
    exec  Executor
    log   *zap.Logger
}
func NewWorker(id string, repo Repo, queue Queue, exec Executor, log *zap.Logger) *Worker

func (w *Worker) Run(ctx context.Context) error {
    if err := w.queue.EnsureGroup(ctx); err != nil { return fmt.Errorf("ensure group: %w", err) }
    for {
        select {
        case <-ctx.Done(): return nil
        default:
        }
        msgs, err := w.queue.Read(ctx, w.id, 10, 5*time.Second)
        if err != nil { if ctx.Err()!=nil { return nil }; w.log.Error("worker read", zap.Error(err)); continue }
        // also periodically Reclaim idle messages (visibility-timeout redelivery)
        for _, qm := range msgs { w.process(ctx, qm) }
    }
}

// process: load run by JobRunMsg.JobRunID (idempotency key). If run already terminal or
// not in queued/retry → Ack + skip (at-least-once dedup, ticket line 32). Else:
//   MarkRunning + UpdateRunStatus + AppendEvent(running)
//   exec.Execute:
//     success → MarkSuccess + UpdateRunStatus + AppendEvent(success) + Ack
//     failure & attempts < maxAttempts → MarkRetry + UpdateRunStatus + AppendEvent(retry)
//                                        + Enqueue(JobRunMsg{Attempts: attempts}) + Ack(old)
//     failure & attempts >= maxAttempts → MarkFailed + UpdateRunStatus + AppendEvent(failed)
//                                         + DeadLetter + Ack
func (w *Worker) process(ctx context.Context, qm QueuedMessage) { ... } // per-msg context.WithTimeout 10s
```

> The worker `Ack`s in every terminal path so the message leaves the pending-entries list; on crash
> before Ack, `Reclaim` redelivers (at-least-once). The idempotency guard makes redelivery safe.

#### 4. Repo: implement remaining methods used by worker
**File**: `internal/task/repo_postgres.go`
**Action**: modify — ensure `UpdateRunStatus` writes `attempts` (params already include it) and
`AppendEvent` is implemented (from P1). No new queries needed for P3.

#### 5. Wire the worker pool
**File**: `internal/bootstrap/wire.go`
**Action**: modify — add N workers to the returned runners.

```go
exec := task.NewStubExecutor(lg)
const workerCount = 3
runners := []task.Runner{watcher}
for i := 0; i < workerCount; i++ {
    runners = append(runners, task.NewWorker(fmt.Sprintf("worker-%d", i), taskRepo, taskQueue, exec, lg))
}
return runners
```

### Verification
#### Automated
- [ ] `make test` passes — `internal/task/worker_test.go` (gomock Queue+Repo, stub Executor):
  (a) success → `MarkSuccess` + `Ack` + `AppendEvent(success)`; (b) transient fail (attempts<max) →
  `MarkRetry` + re-`Enqueue` + `Ack`; (c) fail at max attempts → `MarkFailed` + `DeadLetter` + `Ack`;
  (d) already-terminal run → `Ack` only, no execute (idempotency).
- [ ] `make lint`, `go build ./...`.

#### Manual
- [ ] `make run` (Postgres+Redis). Inspector: `task.create` past `scheduled_at` → `task.status`
  becomes `"success"` within ~10s (ticket's primary check; treat `success` as the ticket's `completed`).
- [ ] Future-time create + `task.cancel` → `"cancelled"`; the cancelled run is not executed.
- [ ] Force failure: temporarily set `StubExecutor.failFunc` to always error → after `maxAttempts`
  the run is `failed` and `redis-cli XLEN task:dlq` ≥ 1.
- [ ] `redis-cli XPENDING task:runs task-workers` → 0 pending after processing (all Ack'd).

---

## Phase 4: Recurring-job watcher (next-run creation)

Polls terminal `run_events` for recurring jobs; creates the next `pending` JobRun (sequence+1) from
the interval, guarded against duplicates by the unique `(job_id, sequence)` constraint.

### Changes

#### 1. Recurring watcher (third supervised goroutine)
**File**: `internal/task/recurring_watcher.go`
**Action**: create

```go
type RecurringWatcher struct {
    repo     Repo
    interval time.Duration
    lookback time.Duration // how far back to scan terminal events each tick
    limit    int32
    log      *zap.Logger
}
func NewRecurringWatcher(repo Repo, interval time.Duration, log *zap.Logger) *RecurringWatcher

func (rw *RecurringWatcher) Run(ctx context.Context) error {
    t := time.NewTicker(rw.interval); defer t.Stop()
    for {
        select {
        case <-ctx.Done(): return nil
        case <-t.C:       rw.scanOnce(ctx)
        }
    }
}

// scanOnce: FindTerminalRecurringRuns(since=now-lookback) → for each spec, build next run:
//   next, _ := NewJobRun(spec.JobID, spec.Sequence+1, spec.ScheduledAt.Add(spec.Interval))
//   created, err := repo.InsertRunIfAbsent(ctx, next)  // ON CONFLICT DO NOTHING ⇒ created=false is a no-op
func (rw *RecurringWatcher) scanOnce(ctx context.Context) { ... } // context.WithTimeout 5s
```

#### 2. Repo: implement P4 methods
**File**: `internal/task/repo_postgres.go`
**Action**: modify — implement:
- `FindJob` → `q.GetJobByID`, map via `jobFromSqlc`, `pgx.ErrNoRows → ErrJobNotFound`.
- `InsertRunIfAbsent` → `q.InsertJobRunIfAbsent` (`:execrows`); `return rows > 0, nil`.
- `FindTerminalRecurringRuns` → `q.ListTerminalRecurringRuns`, map rows via `nextRunSpecFromSqlc`.

> Queries `GetJobByID`, `ListTerminalRecurringRuns`, `InsertJobRunIfAbsent` were already added to
> `sql/queries/task.sql` in Phase 1 — regenerate sqlc if not already done.

> **Deviation note:** the structure said the recurring watcher "polls `RunEvent`". The query does
> read `run_events` (terminal events) as the trigger, but also joins `job_runs`/`jobs` and uses
> `NOT EXISTS (sequence+1)` so detection + dedup are one statement. The unique `(job_id, sequence)`
> constraint is the final idempotency guard (design Open Risk). No separate "is next created?" flag.

#### 3. Wire the recurring watcher
**File**: `internal/bootstrap/wire.go`
**Action**: modify — append to runners:

```go
runners = append(runners, task.NewRecurringWatcher(taskRepo, 10*time.Second, lg))
```

### Verification
#### Automated
- [ ] `make test` passes — `internal/task/recurring_watcher_test.go` (gomock Repo): terminal recurring
  spec → `InsertRunIfAbsent` called with `sequence+1` and `scheduledAt+interval`; `InsertRunIfAbsent`
  returning `created=false` (conflict) is a silent no-op; ctx cancel → `Run` returns nil.
- [ ] `make lint`, `go build ./...`.

#### Manual
- [ ] Inspector: `task.create` with `recurring_interval_seconds=5` and a past `scheduled_at` → the
  first run executes (`success`), and within `interval` a NEW `pending` run appears in `task.list`
  with `sequence=2` and `scheduled_at` advanced by 5s.
- [ ] Confirm exactly one successor per terminated run (no duplicates) — repeated watcher ticks do
  not create extra rows (`SELECT job_id, sequence FROM job_runs ORDER BY 1,2` shows contiguous,
  unique sequences).

---

## Final acceptance (all phases)

- [ ] `make lint && make test` pass.
- [ ] `npx @modelcontextprotocol/inspector go run ./cmd/mcp` → 4 tools; full ticket flow (create past →
  completed ~10s; future + cancel → cancelled; list shows all).
- [ ] `cmd/api` runs watcher + worker pool + recurring watcher as supervised goroutines; `Ctrl+C`
  drains them (no leak) before closing Redis/Postgres.
- [ ] Forced-failure run dead-letters after `maxAttempts`; recurring job self-reschedules once per run.

## Out of scope (from design "What We're NOT Doing")

Python server (realized in Go); removing demo slices; real task side effects (stub `Executor`); MCP
auth; proving 10K jobs/sec (build the shape only); MCP-side cancel/status race protocol beyond the
idempotency key + status guards; native Postgres range partitioning (single table + `time_bucket`
index instead).
