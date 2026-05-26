# 0002. Use sqlc instead of an ORM

**Status:** Accepted
**Date:** 2026-05-12
**Deciders:** core team

## Context

Every Go backend project eventually picks a persistence approach. The realistic options are:

1. **An ORM** (GORM, ent, Bun) — Go structs as the source of truth, generated SQL, lifecycle hooks.
2. **A query builder** (Squirrel, goqu) — programmatic SQL construction.
3. **`database/sql` + hand-written queries** — full control, full boilerplate.
4. **sqlc** — SQL is the source of truth; Go types and methods are generated from `.sql` files at build time.

We have three constraints that drive the choice:

- The codebase enforces hexagonal architecture; `domain.go` must contain pure business types with **zero infrastructure dependencies** (R1.1, enforced by depguard). ORM struct tags + lifecycle hooks would either pollute `domain.go` or force a parallel "persistence struct" layer.
- We want SQL to be readable and reviewable in PRs. Migration and query authors are often DBAs or senior engineers who think in SQL, not in struct tags.
- AI-assisted development benefits from explicit, mechanical mappings. Generated code keeps the AI from "creatively interpreting" a query.

## Decision

All SQL goes through **sqlc**. Queries live in `sql/queries/<feature>.sql`. Generated code lands in `internal/platform/postgres/sqlc/`. Hand-written `db.Query(...)` is forbidden outside `internal/platform/` (R3.5).

Each feature's `repo_postgres.go` maps between sqlc's generated row types and the domain entity (via `dto_internal.go`), preserving the layer boundary.

## Consequences

**Positive:**
- SQL is reviewable in PRs as SQL, not as a Go struct decoration.
- `domain.go` stays infrastructure-free — no ORM tags to leak.
- Compile-time safety: a column rename in a migration breaks the build at the call site.
- AI scaffolding is predictable: "add a query" means "edit `sql/queries/foo.sql` and run `make sqlc-generate`."
- The generated layer is a clear stop-the-bus boundary for `dto_internal.go` mappings.

**Negative:**
- Two-step workflow: change SQL → regenerate → compile. Forgetting `make sqlc-generate` produces stale errors.
- No runtime query composition (no "build a WHERE clause from a filter map"). Dynamic queries either become multiple named queries or fall through to a documented `sqlc.arg` pattern.
- Generated code under version control increases diff noise on schema changes. We accept this in exchange for review visibility.

## Alternatives considered

**GORM** — rejected. Reflection-heavy, schema-from-struct conflicts with R1.1 (would force ORM tags into `domain.go`), and the lifecycle-hook surface area is exactly what hexagonal architecture is trying to *not* have.

**ent** — closer to acceptable. Code generation from a schema file is in the same spirit as sqlc, but the schema is a Go DSL rather than SQL, so reviewers must read DSL rather than SQL. We optimize for SQL reviewability.

**Squirrel / goqu** — rejected. Programmatic query building obscures the SQL in code review and offers no compile-time safety against schema drift.

**Plain `database/sql`** — rejected. Boilerplate per query is high; the row-scanning code is exactly what sqlc generates.

## References

- [sqlc documentation](https://docs.sqlc.dev/)
- ADR [0001-hexagonal-feature-first](0001-hexagonal-feature-first.md) — the architecture this decision serves
- `sqlc.yaml` — generator configuration
