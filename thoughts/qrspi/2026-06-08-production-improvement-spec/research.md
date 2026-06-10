# Research Findings

Factual map of the `chatgpt-tasks` scheduling/execution flow and repo conventions. Every claim carries a `file:line` reference. Describes what exists today — no recommendations.

---

## Q1: Data model & persistence layer for `internal/task/`

### Schema — `migrations/0001_init.up.sql`
**`jobs`** (lines 14–21): `id UUID PK`, `kind TEXT NOT NULL CHECK (kind IN ('one_off','recurring'))`, `description TEXT NOT NULL`, `interval_seconds BIGINT NOT NULL DEFAULT 0 CHECK (>= 0)`, `created_at TIMESTAMPTZ DEFAULT NOW()`, `updated_at TIMESTAMPTZ DEFAULT NOW()`. No explicit indexes (PK only).

**`job_runs`** (lines 24–36): `id UUID PK`, `job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT`, `sequence INT NOT NULL`, `status TEXT NOT NULL CHECK (status IN ('pending','queued','running','success','retry','failed','cancelled'))`, `scheduled_at TIMESTAMPTZ NOT NULL`, `time_bucket BIGINT NOT NULL`, `attempts INT NOT NULL DEFAULT 0`, `created_at`, `updated_at`, plus table constraint `UNIQUE (job_id, sequence)` (line 36). Indexes: `idx_job_runs_due (time_bucket, status, scheduled_at)` (line 38), `idx_job_runs_job (job_id)` (line 40).

**`run_events`** (lines 43–48): `id UUID PK`, `job_run_id UUID NOT NULL REFERENCES job_runs(id) ON DELETE RESTRICT`, `status TEXT NOT NULL`, `created_at TIMESTAMPTZ DEFAULT NOW()`. Append-only — no `updated_at`, no error/payload column. Indexes: `idx_run_events_run (job_run_id)` (line 50), `idx_run_events_created (created_at)` (line 51). Down migration drops `run_events, job_runs, jobs, users` in FK order (`0001_init.down.sql:1-4`).

### sqlc queries — `sql/queries/task.sql` (11 queries)
`InsertJob`, `InsertJobRun`, `UpdateJobRunStatus` (`:execrows`, updates status/attempts/updated_at by id), `GetJobRunByID`, `ListJobRuns` (`ORDER BY created_at DESC` + LIMIT/OFFSET), `CountJobRuns`, `FindDueJobRuns` (lines 31–37: `WHERE time_bucket <= $bucket AND status='pending' AND scheduled_at <= $due_before ORDER BY scheduled_at LIMIT $lim`), `InsertRunEvent`, `GetJobByID`, `ListTerminalRecurringRuns` (lines 47–58: 3-table join finding latest terminal runs of recurring jobs), `InsertJobRunIfAbsent` (lines 61–66: `ON CONFLICT (job_id, sequence) DO NOTHING`, hardcodes `status='pending', attempts=0`).

### Generated models — `internal/platform/postgres/sqlc/models.go`
`Job`, `JobRun`, `RunEvent` structs use `pgtype.UUID`/`pgtype.Timestamptz`/`int32`/`int64` (lines 11–37). `ListTerminalRecurringRunsRow` is a projection (id, job_id, sequence, scheduled_at, interval_seconds) in `task.sql.go:291-297`.

### Domain — `internal/task/domain.go`
All entity fields unexported; getters only. `Job` (30–37) holds `interval time.Duration`. `JobRun` (39–49) holds `timeBucket int64`, `attempts int`. `RunEvent` (51–56). `bucketOf` (58–60): `t.UTC().Truncate(time.Hour).Unix()`. Constructors: `NewJob` (62–82, validates description/kind/interval → `ErrInvalidDescription`/`ErrInvalidSchedule`), `NewJobRun` (84–101, sets `status=pending`, computes `timeBucket`), `NewRunEvent` (103–110, no validation). `rehydrateJob`/`rehydrateJobRun` (112–151) bypass validation for DB reads. IDs are UUIDv7 (`shared.NewJobID()` etc.).

### Repo mapping — `repo_postgres.go` + `dto_internal.go`
`PostgresRepo` wraps `*sqlc.Queries` via `sqlc.New(pool)` (`repo_postgres.go:18-25`); `var _ Repo = (*PostgresRepo)(nil)` (line 22). Mapping helpers in `dto_internal.go`: `jobFromSqlc`/`jobToInsertParams` convert `interval_seconds ↔ time.Duration` by `*/÷ time.Second` (29). int32↔int casts for sequence/attempts (49–61). Methods: `SaveJob`, `SaveRun`, `UpdateRunStatus` (rows==0 → `ErrJobRunNotFound`), `FindRunByID` (`pgx.ErrNoRows`→`ErrJobRunNotFound`), `ListRuns` (returns `[]*JobRun,int64`), `AppendEvent`, `FindDueRuns`, `FindJob`, `InsertRunIfAbsent` (returns bool), `FindTerminalRecurringRuns` (returns `[]NextRunSpec`). pgtype helpers: `internal/platform/postgres/pgtype.go:21-44`.

---

## Q2: Creation → persisted job → first run

### MCP layer — `internal/task/mcp/`
`Registry` is a `map[string]ToolHandler` where `ToolHandler = func(ctx, json.RawMessage) (any, error)` (`registry.go:10-24`). `Register(reg, svc)` wires four tools (`tools.go:49`). `ToolService` interface (`tools.go:13-18`): `Create`/`List`/`Status`/`Cancel`. The cmd entrypoint `cmd/mcp/main.go` builds `task.NewService(task.NewPostgresRepo(pool))` (line 40), registers via `taskmcp.Register`, and binds each to the go-sdk MCP server over stdio (`main.go:44-95`). Input structs with jsonschema tags at `main.go:97-110`.

**`task.create`** (`tools.go:20-24,50-69`): accepts `description`, `scheduled_at` (RFC3339, parsed line 55), `recurring_interval_seconds` (optional). Builds `CreateInput`, calls `svc.Create`. Returns `runResponse{JobID, Status, ScheduledAt, Sequence}` (`tools.go:136`). `Service.Create` (`service.go:25-47`): kind = recurring if `interval>0` else one_off (26–29) → `NewJob` → `repo.SaveJob` → `NewJobRun(j.ID(), 1, scheduledAt)` (sequence always 1) → `repo.SaveRun`. **No event emitted on create.**

**`task.list`** (`tools.go:27-29,71-87`): `limit`/`offset` → `shared.NewPagination` (clamps default 20, max 100). Returns `listResponse{Runs, Total, Limit, Offset}`. `Service.List` → `repo.ListRuns`.

**`task.status`** (`tools.go:31-33,89-99`): `{job_id}` → `shared.ParseJobRunID` → `svc.Status` → `repo.FindRunByID`. Returns `runResponse`.

**`task.cancel`** (`tools.go:101-111`): `{job_id}` → `Service.Cancel` (`service.go:61-74`): `FindRunByID` → `run.Cancel()` (domain, rejects terminal → `ErrInvalidStatusTransition`) → `repo.UpdateRunStatus` → `repo.AppendEvent(NewRunEvent(id, StatusCancelled))` (return discarded with `_=`).

---

## Q3: Watcher & RecurringWatcher

### Watcher — `internal/task/watcher.go`
Fields: `repo`, `queue`, `interval`, `horizon=5min` (line 28, hardcoded), `limit=100` (line 29). `Run` ticks on `interval` (constructed at 5s in wire.go). `scanOnce` (48–78): 5s ctx timeout (49); `now=UTC`, `before=now+5min` (52–53); `bucketsInRange(now, before)` (54); per bucket → `repo.FindDueRuns(bucket, before, limit)`; per run → `run.MarkQueued()` (62, skips on bad status) → `repo.UpdateRunStatus` (65) → `queue.Enqueue(JobRunMsg{JobRunID, Attempts})` (70). `bucketsInRange` (80–92) emits one Unix-second value per hour boundary from→to inclusive (truncated to hour) — usually 1 bucket, 2 when crossing the hour. `FindDueJobRuns` served by `idx_job_runs_due`.

### RecurringWatcher — `internal/task/recurring_watcher.go`
Fields: `repo`, `interval`, `lookback=1h` (line 22), `limit=100` (line 23). Constructed at 10s tick (`wire.go:50`). `scanOnce` (42–71): `repo.FindTerminalRecurringRuns(now-lookback, limit)`; per `NextRunSpec` → `nextScheduledAt = spec.ScheduledAt + spec.Interval`, `NewJobRun(spec.JobID, spec.Sequence+1, nextScheduledAt)` (53) → `repo.InsertRunIfAbsent` (58). `FindTerminalRecurringRuns` SQL (`task.sql:47-58`) finds `run_events` with status `success`/`failed` on recurring jobs within lookback where `sequence+1` does not yet exist (`NOT EXISTS`). `InsertJobRunIfAbsent` is idempotent via `UNIQUE(job_id,sequence)` + `ON CONFLICT DO NOTHING`; new run is `pending` and gets picked up by Watcher once within the 5-min horizon.

---

## Q4: Worker execution, retries, DLQ, lifecycle events

### Constants — `worker.go:12-18`
`maxAttempts=3`, `workerReadCount=10`, `workerReadBlock=5s`, `workerReclaimMinIdle=30s`, `workerMessageTimeout=10s`.

### Run loop — `worker.go:38-81`
`Run` calls `queue.EnsureGroup` first (39), then loops: `processReclaimed` (50, `queue.Reclaim(minIdle=30s)`) → `queue.Read(id, 10, 5s)` (57) → `process(msg)` per message. 3 workers wired (`worker-0..2`, `wire.go:45-49`).

### Per-message — `worker.go:83-129`
10s timeout (84); `ParseJobRunID` (87, on fail: log+ACK+drop); `FindRunByID` (95, on DB err: return **without ACK** → stays in PEL for reclaim); terminal/wrong-status guard (100–103, ACK+drop duplicates); `MarkRunning` (105) → `persistStatus` (110) → `appendEvent(StatusRunning)` (113) → `exec.Execute` (115); on success `MarkSuccess` (120) → persist (124) → `appendEvent(StatusSuccess)` (127) → `ack` (128).

### Failure / retry / DLQ — `worker.go:131-166`
Decision at line 132: `if run.Attempts()+1 >= maxAttempts` → **DLQ branch**: `MarkFailed` → persist → `appendEvent(StatusFailed)` → `queue.DeadLetter(msg)` (XADD `task:dlq`) → `ack`. Else **retry branch**: `MarkRetry` (increments `attempts`) → persist → `appendEvent(StatusRetry)` → `queue.Enqueue(JobRunMsg{..., Attempts: run.Attempts()})` (re-enqueue with incremented count) → `ack`. Any error inside `handleFailure` returns without ACK (left for reclaim).

### Status machine — `domain.go:154-211`
`pending|retry→queued` (MarkQueued), `queued|retry→running` (MarkRunning), `running→success`, `running→retry` (attempts++), `running|retry→failed`, non-terminal→cancelled. Terminal: success/failed/cancelled (`IsTerminal` 209–211).

### run_events — `worker.go:176-180`, `dto_internal.go:84-91`
`appendEvent` → `NewRunEvent(runID, status)` → `repo.AppendEvent` → `InsertRunEvent`. Each row stores only `id, job_run_id, status, created_at` — **no error message, no payload**. Emitted at Running, Success, Retry, Failed (and Cancelled from service).

### Executor — `ports.go:38-40`, `executor.go:9-31`
Port: `Execute(ctx, *JobRun) error`. Only implementation is `StubExecutor`: returns `ctx.Err()` if cancelled (23–25); calls injectable `failFunc` if set (26–28, used in tests via `SetFailFunc`); otherwise logs info + returns nil (no-op success). No real executor exists.

---

## Q5: Redis Streams queue — `internal/task/queue_redis.go`

Constants (14–18): `streamMain="task:runs"`, `streamDLQ="task:dlq"`, `groupName="task-workers"`. `RedisQueue` wraps `*redis.Client` (20–26). Port `Queue` (`ports.go:24-31`): `Enqueue`, `EnsureGroup`, `Read`, `Reclaim`, `Ack`, `DeadLetter`. `QueuedMessage{StreamID, Msg}` (`ports.go:33-36`). Payload `JobRunMsg{JobRunID string, Attempts int}` (`dto_internal.go:100-103`), JSON-marshalled into a single `data` field.

- `EnsureGroup` (32–40): `XGROUP CREATE task:runs task-workers 0 MKSTREAM`; swallows `BUSYGROUP`.
- `Enqueue`→`xadd(streamMain)` (28–30, 98–111): `XADD task:runs * data <json>`, no MaxLen trim.
- `Read` (42–62): `XREADGROUP GROUP task-workers <consumer> COUNT n BLOCK 5s STREAMS task:runs >`; `redis.Nil`→`nil,nil`.
- `Reclaim` (64–85): `XAUTOCLAIM task:runs task-workers <consumer> <minIdle> 0 COUNT n`; start cursor always `"0"` (next cursor discarded, rescans PEL each call).
- `Ack` (87–91): `XACK task:runs task-workers <streamID>`.
- `DeadLetter` (94–96): `xadd(streamDLQ)` — no consumer group on `task:dlq`, nothing reads it.
- Decode (125–153): `decodeJobRunMsg` accepts string or []byte for `data`, unmarshals; batch error on decode failure.

---

## Q6: User identity, auth, shared value objects

### User — `internal/user/`
`User` aggregate (`domain.go:27-34`) constructed via `NewUser` (37, lowercases/validates email, requires hash, UUIDv7 id) or package-private `rehydrate` (58, DB reads). Mutation: `Rename` (79). 6 sentinel errors (`errors.go:5-12`). Ports (`ports.go:12-29`): `Repo`, `Hasher` (BcryptHasher), `TokenIssuer.Issue(subject)`. `Service` (`service.go:12-16`): `Register` (32, FindByEmail→hash→NewUser→Save→Issue), `Login` (70, Compare→Issue), `GetByID` (89). Comment at `service.go:93-99` documents the cross-feature `UserLookup` capability-port contract.

### Auth — `internal/platform/auth/`
`Manager{secret, issuer, ttl}` (`jwt.go:25-29`). `Issue(subject)` → HS256 JWT with Subject=UserID (45). `Verify` maps expiry → `ErrExpiredToken` (64). `Middleware` (`middleware.go:17`): Bearer parse → Verify → `c.Set("auth.userID", claims.Subject)` (const `ctxUserIDKey="auth.userID"`, line 11). `UserIDFromContext` (37) reads it back. Routes: public `/auth/*`, protected `/users/me` group (`routes.go:21`). `me()` (`handler_http.go:80-85`) → `UserIDFromContext` → `ParseUserID` → `svc.GetByID`.

### shared — `internal/shared/`
4 ID types `type X uuid.UUID` (`ids.go:27-29`): `UserID`, `JobID`, `JobRunID`, `RunEventID`. All implement TextMarshaler/Unmarshaler + sql Scanner/Valuer + `String()`/`IsZero()`. Constructors call `newUUIDV7()` (`uuid.Must(uuid.NewV7())`, line 39). Parse funcs (151–181) return `ErrInvalidID`. `Pagination{Limit,Offset}` + `NewPagination` clamp (default 20, max 100) (`pagination.go:5-16`); generic `Page[T]{Items,Total,Limit,Offset}` (30).

### Identity propagation into task — **none**
No `owner_id`/`user_id` on `jobs`/`job_runs`/`run_events` (migration + domain). `Service.Create` takes only `CreateInput{Description, ScheduledAt, Interval}` (`service.go:25`). The task feature has **no `handler_http.go`** and never calls `auth.UserIDFromContext`. Tasks are driven purely through the MCP stdio server, which has no auth.

---

## Q7: Conventions & infrastructure

### Migrations
`golang-migrate`; single `0001_init.*` edited in place during template phase (CLAUDE.md R0.1). Makefile: `migrate-up`/`-down`/`-create` (lines 72–79); `DB_URL` default `postgres://postgres:pgadmin@localhost:5432/chatpgt-tasks?sslmode=disable` (line 10).

### sqlc — `sqlc.yaml`
engine postgresql, `queries: sql/queries`, `schema: migrations`, package `sqlc`, out `internal/platform/postgres/sqlc`, `sql_package: pgx/v5`, `emit_interface`, `emit_empty_slices`, `emit_pointers_for_null_types`, `emit_json_tags:false`. Regen via `make sqlc-generate` (`Makefile:63`). Both repos call `sqlc.New(pool)`.

### Observability — `internal/platform/`
**logger** (`logger/logger.go:19-69`): zap, fixed EncoderConfig (ts/msg/level/caller, ISO8601), stdout, stacktrace at Error. Built in `bootstrap/app.go:53`; injected into HTTP middleware, watcher, worker, recurring watcher, executor (`wire.go:42-50`). `ZapLoggerMiddleware` logs per-request (`httpserver/middleware.go:28-41`). **metrics** (`metrics/metrics.go:14-41`): private `prometheus.Registry` + Go/Process collectors; `/metrics` + `/healthz` mounted no-auth (`app.go:102-103`). **No feature-level custom metrics.** **otel** (`otel/tracer.go:27-60`): no-op when disabled; OTLP gRPC exporter + batch TracerProvider + TraceContext/Baggage propagator when enabled. **No feature instruments spans.**

### Config — `config/config.go:12-148`
Nested `App/HTTP/DB/Redis/JWT/OTel/Logger`. `Load()` (requireJWT true) vs `LoadMCP()` (false, used by `cmd/mcp`). viper defaults + AutomaticEnv (`.`→`_`) + 20 explicit `BindEnv` + optional `.env` (errors ignored). Validates DB.DSN always, JWT.Secret only when required. Key vars: `POSTGRES_DSN`, `JWT_SECRET`, `JWT_TTL`, `REDIS_ADDR`, `APP_PORT`, `OTEL_ENABLED`, `LOG_LEVEL`.

### Runtime composition
`wireFeatures` (`wire.go:22-52`) is the only multi-feature importer. Returns `[]task.Runner` = 1 watcher + 3 workers + 1 recurring watcher. `app.go:136-138` launches each `Runner.Run(bgCtx)` in its own goroutine.

### Testing
Two unit styles coexist: **hand-rolled fakes** (actually used — `task/service_test.go:15-40`, `watcher_test.go`, `worker_test.go`, `recurring_watcher_test.go`, `user/service_test.go:19-53`) and **gomock** (`internal/{task,user}/mocks/mock_ports.go`, generated via `//go:generate`, present but unused by current tests). Handler tests use `httptest` + hand-rolled `mockSvc` (`user/handler_http_test.go`). Table-driven, snake_case names. `make test-unit` = `go test -race -short -count=1 ./...`; `make mock-gen` = `go generate ./...`. **Integration** (`test/integration/order_repo_test.go`): build tag `//go:build integration`, run via `make test-integration` (`-tags=integration`); currently only a **skipped smoke stub** (line 50) — references `order_repo` though no order feature is wired in `wire.go`, and `POSTGRES_TEST_DSN` usage is documented in comments but not implemented.

---

## Cross-Cutting Observations
- **Hexagonal discipline holds**: domain fields unexported with `New*` constructors + `rehydrate*` for DB reads; ports are interfaces; `wire.go` is the sole composition root; compile-time `var _ Repo = ...` checks.
- **UUIDv7 everywhere** as PK / ID (`shared.ids.go:39`), time-ordered.
- **Hourly `time_bucket`** is the scan-partitioning key bridging domain (`bucketOf`) and the `idx_job_runs_due` index.
- **Idempotency & at-least-once**: `InsertRunIfAbsent` (`ON CONFLICT DO NOTHING`) guards recurring scheduling races; worker uses ACK-after-success + 30s reclaim for at-least-once; terminal-status guard drops duplicate deliveries.
- **Hardcoded tunables**: horizon 5min, lookback 1h, scan limits 100, maxAttempts 3, reclaim 30s, message timeout 10s, 3 workers, ticks 5s/10s — all literals, not config.

## Open Areas
- **DLQ has no consumer**: `task:dlq` is written but never read (`queue_redis.go:94-96`); no reprocessing/alerting path exists.
- **run_events carry no failure detail** — status + timestamp only; root-cause of a failure is not persisted.
- **No real executor** — `StubExecutor` is the only implementation; what a "task" actually does is unspecified in code.
- **No auth/ownership on tasks** — MCP server is unauthenticated; jobs are not scoped to a user.
- **Integration test is a stub** referencing a non-existent `order` feature; no live repo coverage.
