# Example: AI builds the `inventory` feature

This walkthrough shows how Claude Code adds a new feature end-to-end in this repo. The goal is to make the AI workflow concrete: what the human types, what the AI does, where the rails (CLAUDE.md, skills, depguard) push back, and what the diff looks like.

> Scenario: we want an `inventory` feature that tracks SKU stock levels. Endpoints: `POST /inventory` (add SKU), `GET /inventory/:sku`, `PATCH /inventory/:sku/adjust` (delta stock).

## Phase 0 — Setup (one-time)

Open the repo in Claude Code. The `.claude/settings.json` allowlist auto-approves `make`, `go test`, `sqlc`, `migrate`, and read-only git/file tools — so the loop doesn't stall on permission prompts.

The model loads `CLAUDE.md` automatically. The `new-feature`, `go-hex-antipatterns`, and `go-hex-recipes` skills will fire on demand based on what we ask.

## Phase 1 — Prompt

**Human:**

> Add an `inventory` feature. SKUs are 8-char alphanumeric. Stock is a non-negative int. Endpoints:
> - `POST /inventory` body `{sku, initial_stock}` → 201
> - `GET /inventory/:sku` → `{sku, stock}` or 404
> - `PATCH /inventory/:sku/adjust` body `{delta}` → 200 with new stock; reject if result < 0
>
> Persist in Postgres. No cache yet. No cross-feature deps.

## Phase 2 — AI plans

The model recognizes "add a feature" and invokes the `new-feature` skill. That skill is a checklist, not a script — the model uses it to enumerate the 11 files + sql + migration + wire + openapi, then writes a plan with the file list and the domain invariants it extracted from the prompt.

It asks one clarifying question:

> Should `adjust` be idempotent (request-id keyed) or naive? Naive is simpler; idempotent costs one extra column + a unique index.

**Human:** naive for now.

## Phase 3 — AI scaffolds

The model:

1. Copies `internal/order/` mentally as the template (it doesn't actually `cp -r`; it writes each file fresh because the bodies need to differ).
2. Writes 11 files under `internal/inventory/`:
   - `domain.go` defines `SKU` (validated 8-char alphanumeric) and `Item{SKU, Stock}` with `NewItem(sku, stock)` rejecting empty SKU and negative stock. `Adjust(delta int) error` returns `ErrStockUnderflow` if result < 0.
   - `errors.go` — `ErrItemNotFound`, `ErrStockUnderflow`, `ErrInvalidSKU`.
   - `ports.go` — `InventoryRepo` interface with `Get`, `Create`, `UpdateStock`.
   - `service.go` — three methods, each taking `ctx` first, calling the repo, wrapping errors with `%w`.
   - `dto_http.go` — request/response structs + `toItemResponse`.
   - `dto_internal.go` — `itemRow` mapping to/from sqlc-generated row.
   - `handler_http.go` — Gin handlers; maps `ErrItemNotFound` → 404, `ErrStockUnderflow` / `ErrInvalidSKU` → 400.
   - `routes.go` — `RegisterRoutes`.
   - `repo_postgres.go` — implements `InventoryRepo` via sqlc.
   - `service_test.go` — table-driven with mocked repo: happy path, not-found, underflow.
   - `handler_http_test.go` — `httptest` cases covering each status code.

3. Writes `sql/queries/inventory.sql`:
   ```sql
   -- name: GetInventoryItem :one
   SELECT sku, stock FROM inventory_items WHERE sku = $1;
   -- name: CreateInventoryItem :exec
   INSERT INTO inventory_items (sku, stock) VALUES ($1, $2);
   -- name: UpdateInventoryStock :exec
   UPDATE inventory_items SET stock = $2 WHERE sku = $1;
   ```

4. Creates migration `migrations/0002_create_inventory_items.up.sql` and `.down.sql`.

5. Edits `internal/bootstrap/wire.go` — five lines: construct repo, service, handler, register routes.

6. Edits `api/openapi.yaml` — adds the three endpoints under an `inventory` tag.

## Phase 4 — AI verifies

```bash
make sqlc-generate
make mock-gen
make lint
make test
make migrate-up
```

Three things to notice in this loop:

1. **`make lint` was the rail.** First run failed: the model accidentally imported `internal/user` to get `UserID` for an audit field. depguard's `no-cross-feature` rule (glob over `internal/*/**` with allow for `shared/platform/bootstrap`) flagged it with the right message: *"Cross-feature import forbidden. Define a capability port..."* The model dropped the audit field as out of scope, re-ran lint, green.

2. **No file was created that wasn't on the checklist.** The skill is a closed list; the model didn't invent `service_helpers.go` or `validators.go`.

3. **The model didn't touch the depguard config.** Glob rules covered the new feature automatically — exactly the benefit of `internal/*/domain.go` over enumeration.

## Phase 5 — Human reviews

Diff stats: **+612 / -0 across 14 files** (11 feature files + sql + migration up/down + 2 wire/openapi edits). Tests pass; lint green; OpenAPI updated.

The reviewer spot-checks against `go-hex-antipatterns`:
- ✅ No `c.JSON(200, item)` — mapped via `toItemResponse`.
- ✅ Service constructor accepts `InventoryRepo`, not `*pgxpool.Pool`.
- ✅ Errors wrapped with `%w`.
- ✅ All external calls have `context.WithTimeout`.
- ✅ `domain.NewItem` validates SKU + stock.

Merge.

## What made this work

| Lever | Why it mattered |
|---|---|
| **CLAUDE.md (slim)** | R1–R3, R5 + file responsibility table loaded every turn. Architecture isn't relearned. |
| **`new-feature` skill** | Closed checklist of 11 files + sql + wire + openapi. Stops the model from inventing structure. |
| **`go-hex-antipatterns` skill** | Loaded when the model second-guessed a handler. Caught the "marshal domain entity" temptation. |
| **depguard globs** | New feature is enforced without editing lint config. The cross-feature rail fires in seconds. |
| **`.claude/settings.json` allowlist** | `make lint` / `make test` / `make sqlc-generate` ran without prompting, so the verify loop was tight. |
| **Three working features** | `order` as the implicit template — the model has concrete code to imitate. |

## Try it yourself

```text
# In Claude Code, with this repo open:
add an inventory feature for tracking SKU stock levels. POST /inventory creates, GET /inventory/:sku reads, PATCH /inventory/:sku/adjust applies a delta and rejects if stock would go negative.
```

Expected outcome: ~15-minute end-to-end, one clarifying question, one lint failure caught by depguard, green on second pass.
