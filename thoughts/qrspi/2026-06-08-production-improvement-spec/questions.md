# Research Questions

## Context
This is a Go hexagonal-architecture backend (feature-first packages under `internal/`). Focus on the `internal/task/` feature package end to end — its domain, service, ports, repository, queue adapter, watchers, worker, and executor — plus the `internal/user/` feature, the `internal/shared/` and `internal/platform/` packages, the migrations and sqlc layers, the MCP server, and the test suites. The goal is a factual map of how the scheduling and execution flow works today and what conventions the repo follows.

## Questions

1. What is the full data model and persistence layer for the `internal/task/` feature? Trace the schema in `migrations/0001_init.*.sql`, the sqlc queries in `sql/queries/task.sql`, the generated models, the domain entities and their `New*` constructors in `domain.go`, and how `repo_postgres.go` maps between domain types and sqlc rows. Note every column on `jobs`, `job_runs`, and `run_events`, and any existing indexes.

2. How does a task move from creation to a persisted job and (if applicable) first run? Trace the flow from the MCP tool handlers (`internal/task/mcp/tools.go`, `registry.go`) through the service (`service.go`) into the repository, including any validation, event emission, and what fields each MCP tool (`task.create`, `task.list`, `task.status`, `task.cancel`) accepts and returns today.

3. How do the watcher and recurring watcher work? Trace `watcher.go` and `recurring_watcher.go`: how due runs are found (the `time_bucket` / status / scheduled_at scan), how `interval_seconds` recurrence computes the next run, how `time_bucket` values are generated, and how/when new `job_runs` are created and enqueued.

4. How does the worker execute a run, and how are retries, dead-lettering, and lifecycle events handled? Trace `worker.go` and `executor.go`: the executor port interface, what the stub executor does, how `max_attempts`/retry and DLQ decisions are made, what status transitions occur, and where/how `run_events` rows are written (including what data each event currently carries).

5. How does the Redis Streams queue adapter (`queue_redis.go`) work? Document the consumer-group setup, the XADD/XREAD/XACK/XCLAIM usage, how claim/reclaim (min-idle) recovers stuck messages, the message payload shape, and the port interface the queue satisfies in `ports.go`.

6. How does the `internal/user/` feature model identity and authentication, and what reusable value objects exist in `internal/shared/`? Trace the user domain, auth in `internal/platform/auth`, how user/owner identity flows through requests today, the ID value objects in `internal/shared/ids.go`, pagination helpers, and whether any identity context is propagated into the task feature.

7. What are the repo's conventions and infrastructure for migrations, sqlc, observability, configuration, and testing? Cover: how migrations are structured and run, the `sqlc.yaml` config and codegen workflow, the `internal/platform/metrics`/`otel`/`logger` packages and how they're used, config loading, and the unit/integration test patterns (gomock usage, `mocks/` packages, build tags, and what `test/integration/` contains).
