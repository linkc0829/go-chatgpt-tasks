# Research Findings

Backend: Go hexagonal architecture, feature-first packages. Three demo slices (`user`, `order`, `payment`), composition root in `internal/bootstrap/`, infra in `internal/platform/`, single entry point `cmd/api/main.go`.

## Q1: Feature package internal structure & request flow

### Findings (example: `internal/order/`)
- **File roles** — `domain.go` (entity + pure rules), `ports.go` (outbound interfaces only), `service.go` (use-case orchestration), `handler_http.go` (parse/validate/respond), `routes.go` (route registration), `dto_http.go` (HTTP structs/mappers), `dto_internal.go` (persistence-row conversion), `repo_postgres.go` (`Repo` adapter), `errors.go` (sentinels).
- **Domain** — `Order` struct holds unexported fields `domain.go:20-27`; only imports `time` + `internal/shared`. Constructor `NewOrder(userID, amount) (*Order, error)` validates invariants `domain.go:31`. State transitions are pure methods: `MarkPaid()` `domain.go:60`, `Cancel()` `domain.go:69` — no `context`. `rehydrate(...)` `domain.go:50` rebuilds from trusted storage, skipping validation.
- **Ports** — `Repo` (`Save`/`Update`/`FindByID`/`ListByUser`) `ports.go:14`; `UserLookup.Exists` `ports.go:31`; `PaymentCharger.Charge` `ports.go:38`. All methods take `ctx context.Context` first. Imports only `context` + `shared`.
- **Service** — `Service` holds 3 interface fields `service.go:10-14`; `NewService(repo Repo, users UserLookup, payment PaymentCharger) *Service` `service.go:16` (interfaces only). `Create` `service.go:29-52`, `Pay` `service.go:57-77`. Errors wrapped with `fmt.Errorf("...: %w", err)`. Ownership check returns `ErrOrderNotFound` to avoid leaking existence `service.go:62`.
- **Handler** — stores a local `service` interface, not `*Service` `handler_http.go:15-21`; `NewHandler(svc *Service) *Handler` `handler_http.go:27`. Each handler: `mustUserID(c)` → `auth.UserIDFromContext` + `shared.ParseUserID`; `c.ShouldBindJSON`; **per-handler `context.WithTimeout`** (create/cancel/list 5s, pay 10s, get 3s — `handler_http.go:45,73,99,125,150`); call service; `writeError` or map+respond.
- **Error→HTTP mapping** — `writeError` `handler_http.go:184-199` uses `errors.Is` (works through `%w`): NotFound→404, UserNotFound/Invalid*→400, PaymentFailed→402, default→500.
- **Repo adapter** — `PostgresRepo{q *sqlc.Queries}` `repo_postgres.go:22`; `NewPostgresRepo(pool *pgxpool.Pool)` wraps `sqlc.New(pool)` `repo_postgres.go:26`. `pgx.ErrNoRows`→`ErrOrderNotFound`.
- **End-to-end `POST /orders/:id/pay`**: `routes.go:17` → `pay` `handler_http.go:62` → `mustUserID`/`ParseOrderID` → 10s ctx → `svc.Pay` → `repo.FindByID` (`r.q.GetOrderByID`) → ownership check → `payment.Charge` port → `o.MarkPaid()` domain → `repo.Update` → `toOrderResponse` 200.

## Q2: Bootstrap composition & process lifecycle

### Findings
- **Entry** `cmd/api/main.go:15`: (1) `config.Load()` `main.go:17` (stdlib `log.Fatalf` only logger available); (2) `signal.NotifyContext(ctx, SIGINT, SIGTERM)` `main.go:21` + `defer stop()`; (3) `bootstrap.NewApp(ctx, cfg)` `main.go:24`.
- **`NewApp`** `app.go:44` constructs graph top-down, each stored on `App` struct `app.go:31-40`: logger `app.go:46` → OTel (`otelShutdown` func) `app.go:55` → pgx pool `app.go:65` → redis `app.go:74` (on fail `pool.Close()` `app.go:80`) → auth Manager `app.go:85` → gin engine + metrics `app.go:92-93` → health/metrics routes `app.go:96-97` → `wireFeatures(...)` `app.go:101` → `httpserver.Server` `app.go:104`.
- **HTTP server** `httpserver/server.go`: `New(logger)` `server.go:27` = `gin.New()` ReleaseMode + Recovery/RequestID/ZapLogger middleware. `Wrap(engine, cfg, logger)` `server.go:39` builds `http.Server` w/ ReadHeader 5s, Read 30s, Write 30s, Idle 120s `server.go:44-47`. `Start()` `server.go:54` blocks on `ListenAndServe`, normalizes `ErrServerClosed`→nil.
- **Goroutine + signal wait** `main.go:29-45`: buffered `errs` chan (cap 1); `app.Run()` in goroutine `main.go:31-36`; `select` `main.go:38` on `<-ctx.Done()` (signal) vs `<-errs` (unexpected crash → `Fatalf` `main.go:43`).
- **Graceful shutdown** — fresh `context.WithTimeout(cfg.App.ShutdownTimeout)` `main.go:47` (independent of cancelled signal ctx); `app.Shutdown` `shutdown.go:13` runs fixed order, every step attempted regardless of prior error: HTTP server `shutdown.go:15` → OTel `shutdown.go:20` → redis `shutdown.go:25` → pgx pool `shutdown.go:30` → `logger.Sync()` `shutdown.go:33`.

## Q3: DB schema, sqlc, repo mapping

### Findings
- **Migrations** — single pair `migrations/0001_init.up.sql` / `.down.sql`. Tables `users` (`up.sql:2-9`), `orders` (`up.sql:14-22`), `payments` (`up.sql:28-37`). Conventions: app-assigned `UUID PRIMARY KEY` (no serial), money as `BIGINT CHECK (amount>0)` + separate `currency TEXT`, `status TEXT CHECK (status IN (...))`, `TIMESTAMPTZ NOT NULL DEFAULT NOW()`, FKs `ON DELETE RESTRICT`, indexes `idx_<table>_<col>` incl. composite `idx_orders_user_created (user_id, created_at DESC)` `up.sql:25`. Down drops in reverse dependency order.
- **sqlc config** `sqlc.yaml`: v2, postgresql, queries `sql/queries`, schema `migrations`, out `internal/platform/postgres/sqlc`, `sql_package: pgx/v5`, `emit_interface: true` (→ `Querier`), `emit_empty_slices: true`, `emit_pointers_for_null_types: true`, json tags off.
- **Query files** use named `sqlc.arg(name)` params (convention noted `order.sql:1-3`). Annotations: `:exec`→`error`, `:execrows`→`(int64,error)` (e.g. `UpdateOrderStatus`), `:one`→`(Row,error)`, `:many`→`([]Row,error)`. `order.sql:5-36` defines Insert/UpdateStatus/GetByID/ListByUser(+pagination args)/CountByUser. `user.sql`, `payment.sql` analogous.
- **Generated** — `DBTX` interface (Exec/Query/QueryRow) accepts pool or tx `db.go:14-18`; `New(db DBTX)` `db.go:20`, `WithTx(tx)` `db.go:28`. Row structs `Order`/`Payment`/`User` use `pgtype.UUID`, `pgtype.Timestamptz`, `int64`, `string` `models.go:11-39`. `Querier` interface (11 methods) + compile assertion `var _ Querier = (*Queries)(nil)` `querier.go:32`. `*Params` structs for multi-arg queries.
- **Pool construction** `postgres.New(ctx, Config)` `postgres.go:19`: `ParseConfig(DSN)`, apply MaxConns/MinConns, `HealthCheckPeriod 30s`/`MaxConnLifetime 1h` `postgres.go:30-31`, `NewWithConfig`, then 5s ping `postgres.go:38-43` (close on fail).
- **Type helpers** `pgtype.go:21-44`: `UUIDToPg`/`PgToUUID` (nil-safe), `TimeToPg`/`PgToTime` (zero-safe). Domain never carries NULL semantics; nullability mapped at repo boundary via sentinels `pgtype.go:11-18`.
- **Repo mapping** `dto_internal.go`: `orderFromSqlc(r) (*Order, error)` `:14` → `shared.NewMoney` + `rehydrate(...)`; `orderToInsertParams` `:29` full insert; `orderToUpdateStatusParams` `:41` only ID/Status/UpdatedAt (matches partial UPDATE). `ListByUser` issues second `CountOrdersByUser` for total `repo_postgres.go:59-83`.

## Q4: Outbound deps & cross-feature capabilities

### Findings
- **Capability interface, named after capability not provider** — `order.PaymentCharger.Charge(ctx, userID, orderID, amount) error` `order/ports.go:38-40`. `order` imports only `context` + `shared`; zero knowledge of `payment`.
- **Structural satisfaction** — `payment.Service.Charge(...)` `payment/service.go:26` exactly matches the signature; `*payment.Service` satisfies `order.PaymentCharger` via Go duck typing, no explicit assertion.
- **Shape-mismatch adapter** — `user.Service` exposes `GetByID (*User, error)`, not `Exists (bool, error)`. Resolved by `userLookupAdapter{svc *user.Service}` `bootstrap/wire.go:78-90`: its `Exists` calls `GetByID`, maps `user.ErrUserNotFound`→`(false,nil)`, passes other errors through. Lives only in bootstrap.
- **Wiring** `wireFeatures` `wire.go:24` (only function importing >1 feature, imports `wire.go:13-17`): user slice `wire.go:36-40` → payment slice `wire.go:48-52` (keeps `paymentSvc`) → order slice `wire.go:65-69`: `order.NewService(orderRepo, userLookupAdapter{userSvc}, paymentSvc)` `wire.go:67`. Compiler verifies port satisfaction at this call site.
- **Satisfaction table**: `Repo`→`*order.PostgresRepo` (direct); `UserLookup`→`userLookupAdapter` (adapter); `PaymentCharger`→`*payment.Service` (structural). `redis.Client` passed to `wireFeatures` but unused (`_`) `wire.go:27`, reserved for cache adapters.
- `payment.Service` itself uses ports `payment.Repo` + `payment.Gateway` `payment/ports.go:12,22`; `Gateway` satisfied by `payment.NewStubGateway()`.

## Q5: Platform infrastructure adapters

### Findings (each package = narrow constructor)
- **config** `config.go`: single `Config` w/ 7 nested sub-structs, `mapstructure` tags `config.go:12-59`. `Load() (*Config, error)` `config.go:63` uses viper: defaults `config.go:67-81` (shutdown 10s, port 8080, db max/min 20/2, jwt ttl 24h, otel off), `AutomaticEnv` + `.`→`_` replacer `config.go:84-85`, 18 explicit `BindEnv` `config.go:89-111` (e.g. `db.dsn`→`POSTGRES_DSN`, `http.port`→`APP_PORT`), optional `.env` read `config.go:114-117`, `Unmarshal` + `validate()`. `validate` `config.go:130-138` requires only `DB.DSN` + `JWT.Secret`.
- **postgres** — see Q3. `New(ctx, Config)` w/ 5s ping timeout.
- **redis** `redis.New(ctx, Config) (*redis.Client, error)` `redis.go:19`: `NewClient{Addr,Password,DB}` (no pool/dial opts), 5s ping `redis.go:26`, close-on-fail.
- **logger** `logger.New(cfg) (*zap.Logger, error)` `logger.go:19`: zap, `parseLevel` (debug/info/warn/error), default encoding json, output stdout/stderr, ISO8601 time key `ts`, `AddStacktrace(ErrorLevel)`. Caller owns `Sync()`.
- **httpserver** — see Q2. Middleware `middleware.go`: `RequestIDMiddleware` (`X-Request-Id` or new UUID, ctx key `request_id`) `:15`; `ZapLoggerMiddleware` logs method/path/status/latency/ip/request_id `:28`.
- **auth** `jwt.go`: `Config{Secret,Issuer,TTL}` `:17-21`; `NewManager(cfg)` `:31`; `Issue(subject)` HS256 `:45-61`; `Verify(raw)` asserts HMAC method, maps expired→`ErrExpiredToken` `:64-82`. `Middleware(m)` `middleware.go:17` parses `Bearer`, stores `claims.Subject` under gin key `auth.userID`; `UserIDFromContext` `:37-42`.
- **httperr** `httperr.go`: `Response{Error, Code}` `:38-41`; `JSON`/`JSONWithCode` via `AbortWithStatusJSON`; wrappers `BadRequest`/`Unauthorized`/`Forbidden`/`NotFound`/`Conflict`/`PaymentRequired`/`Internal` `:54-60`.
- **metrics** `metrics.New() *Registry` `:17`: non-global `prometheus.NewRegistry` + Go/Process collectors. `Prometheus()`, `Handler()` (promhttp), `Health()` (`{"status":"ok"}`).
- **otel** `Setup(ctx, Config) (ShutdownFunc, error)` `tracer.go:27`: no-op if `!Enabled` `:28`; else OTLP gRPC exporter `WithInsecure`, resource w/ service name, `NewTracerProvider(WithBatcher)`, sets global provider + TraceContext/Baggage propagators; returns `tp.Shutdown`.

## Q6: Background processes, loops, queues, cancellation

### Findings
- **None exist.** No long-running background processes, periodic/polling loops, `time.Ticker`, queues, worker pools, or application-level `context.WithCancel` anywhere in the codebase.
- The **only goroutine** is the HTTP server runner `cmd/api/main.go:31`; unsupervised, exits when `server.Shutdown` is called from main.
- All `context.WithTimeout` usage is short-lived: per-handler request timeouts (`order/handler_http.go:45,73,99,125,150`, `user/handler_http.go:42,64,91`, `payment/handler_http.go:32,50`); startup pings (`redis.go:26`, `postgres.go:38`); shutdown timeout (`main.go:47`); one integration test (`test/integration/order_repo_test.go:55`).
- `shutdown.go` defines only synchronous sequential `(*App).Shutdown(ctx)` `:13` — no goroutines/channels/cancellation of its own.

## Cross-Cutting Observations
- **Strict layer boundaries**: `domain.go` (stdlib + shared), `ports.go` (context + shared), services take interfaces only, handlers hold service *interface*. Cross-feature imports live exclusively in `bootstrap/wire.go`.
- **Constructor injection everywhere**: `New*(deps...)` accepting interfaces; bootstrap is the sole wiring site.
- **Error discipline**: sentinel errors per feature (`errors.go`), `%w` wrapping, `errors.Is` at HTTP boundary, `pgx.ErrNoRows`→domain sentinel at repo boundary.
- **Context first param**, per-handler timeouts chosen per operation cost; all external calls (DB/redis ping) timed out.
- **Type isolation**: domain uses `shared.UserID/OrderID/Money`; `pgtype.*` confined to repo boundary via `pgtype.go` helpers.
- **Scaffolding exists**: `scripts/new-feature/main.go`, `.claude/skills/new-feature`, ADRs `docs/adr/0001`, `0002`.

## Open Areas
- **No existing scheduler/cron/worker pattern** — Q6 confirms the codebase has no precedent for periodic execution, background job loops, or supervised long-running goroutines beyond the HTTP server. Any such capability would be greenfield relative to current conventions.
- Redis is wired but unused by feature code (`wire.go:27` blank import) — no cache adapter precedent yet.
