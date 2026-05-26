# ADR 0001 — Hexagonal Architecture with Feature-first Package Layout

- **Status**: Accepted
- **Date**: 2026-04-29

## Context

We need a Go backend layout that:

1. Keeps domain logic isolated from infrastructure (DB, cache, HTTP).
2. Lets us swap adapters (e.g., Stripe → Adyen, Postgres → MySQL) without touching business code.
3. Reads naturally to AI assistants doing vibe-coding — minimal cross-folder hops.
4. Stays cheap to maintain for a side-project / single-developer scale.

Two well-known options were on the table:

- **Clean Architecture** (Uncle Bob): Entity / UseCase / Interface Adapter / Framework, packaged as separate folders.
- **Hexagonal Architecture** (Cockburn): Inside-the-hexagon vs outside-the-hexagon, with ports and adapters at the boundary.

## Decision

Use **Hexagonal Architecture** combined with a **feature-first (vertical-slice) package layout**:

- Each feature (`order`, `payment`, `user`) is one Go package under `internal/`.
- Inside the package, files are split by responsibility:
  `domain.go`, `service.go`, `ports.go`, `errors.go`, `dto_*.go`,
  `handler_*.go`, `routes.go`, `repo_*.go`, `cache_*.go`.
- Cross-feature ports live in the *consumer's* `ports.go`; the bootstrap layer
  injects the provider's `Service` as a structural implementation.
- Shared zero-dependency value objects live in `internal/shared/`.
- All composition happens in `internal/bootstrap/wire.go`.

## Rationale

### Why not Clean Architecture's layered folders?

- Go's `package` is the natural unit of encapsulation. Splitting one feature
  across `domain/order/`, `application/order/`, `interface/order/` forces
  domain entities to be exported just so neighboring layers can see them —
  which leaks the "hexagon's interior" to the rest of the world.
- AI vibe-coding pays a tax for every additional folder it must read to
  understand a single feature.
- The Hexagonal paper itself never mandates layered folders — that's a
  Clean-Architecture-specific convention.

### Why feature-first packages?

- One feature = one folder = one Go package.
- Unexported helpers stay unexported. The compiler enforces the hexagon edge.
- Cross-feature isolation is enforced by `depguard` rules in `.golangci.yml` —
  AI gets a hard fail if it imports a sibling feature.

### Why ports defined in the consumer?

- Matches Go's idiom: "accept interfaces, return structs."
- Consumers express *what they need* (capability), not *who provides it*.
- Provider has zero knowledge of consumer; testability and replaceability
  improve in lock-step.

## Consequences

**Positive**

- AI tools can implement an entire feature touching one folder.
- Provider/consumer wiring is explicit, lives in exactly one place (`bootstrap`),
  and is reviewable in a single diff.
- Linter (`depguard`) catches violations *before* the change reaches review.

**Negative**

- Newcomers expecting Clean Architecture's folder layout need a brief tour.
- "All Postgres code in one place" requires a global search instead of one folder.
- The `bootstrap` layer slowly accumulates wiring — must be kept readable as
  features grow (extract per-feature `Wire*` helpers if it gets long).

## Alternatives considered

- **Clean Architecture**: rejected per above (Go ergonomics, AI usability).
- **Pure layered (`handler/`, `service/`, `repo/`)**: rejected — fails the
  "swap an adapter without touching business code" test, and encourages
  god-package growth.
- **Single-file features**: too small for system-design demos.
