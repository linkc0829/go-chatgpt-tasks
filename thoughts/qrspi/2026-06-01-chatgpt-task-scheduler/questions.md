# Research Questions

## Context
Focus on this Go hexagonal-architecture backend: the per-feature packages under `internal/`, the composition root in `internal/bootstrap/`, the infrastructure utilities in `internal/platform/`, the database layer (`migrations/`, `sql/queries/`, generated sqlc), and the process entry points under `cmd/`. The goal is to understand the existing conventions, lifecycle management, and integration seams.

## Questions
1. How is a single feature package under `internal/` structured internally — how do `domain.go`, `service.go`, `ports.go`, the adapters (`repo_postgres.go`, etc.), and the HTTP layer relate, and how does a request flow from handler through service to a port implementation?
2. How does `internal/bootstrap/wire.go` compose features together, and how are process entry points (`cmd/`), server startup, signal handling, and graceful shutdown implemented?
3. What patterns exist for defining database schema and queries — how are migrations in `migrations/` written and versioned, how are `sql/queries/*.sql` files turned into sqlc-generated code, and how does a `repo_postgres.go` map domain entities to and from generated row structs?
4. How are outbound dependencies and cross-feature capabilities expressed — how are interfaces defined in a feature's `ports.go`, how does another feature or an external adapter satisfy them, and how does `bootstrap` inject them?
5. What infrastructure adapters and clients already exist in `internal/platform/` (e.g. Postgres pool, Redis, logger, config), and how are these clients constructed, configured, and given timeouts/context?
6. Are there any existing patterns in the codebase for long-running background processes, periodic/polling loops, queues, or context-driven cancellation, and if so where do they live and how are they started and stopped?
