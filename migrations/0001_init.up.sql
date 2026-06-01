-- Users -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    id            UUID PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    display_name  TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);

-- Orders ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    id         UUID PRIMARY KEY,
    user_id    UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    amount     BIGINT NOT NULL CHECK (amount > 0),
    currency   TEXT NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('pending', 'paid', 'canceled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id    ON orders (user_id);
CREATE INDEX IF NOT EXISTS idx_orders_user_created ON orders (user_id, created_at DESC);

-- Payments --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments (
    id         UUID PRIMARY KEY,
    user_id    UUID NOT NULL REFERENCES users(id)  ON DELETE RESTRICT,
    order_id   UUID NOT NULL REFERENCES orders(id) ON DELETE RESTRICT,
    amount     BIGINT NOT NULL CHECK (amount > 0),
    currency   TEXT NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments (order_id);
CREATE INDEX IF NOT EXISTS idx_payments_user_id  ON payments (user_id);

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
    time_bucket  BIGINT NOT NULL,
    attempts     INT  NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (job_id, sequence)
);

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
