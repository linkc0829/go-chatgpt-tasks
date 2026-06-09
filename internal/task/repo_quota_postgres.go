package task

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/linkc0829/go-chatgpt-tasks/internal/platform/postgres"
	"github.com/linkc0829/go-chatgpt-tasks/internal/platform/postgres/sqlc"
	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

type PostgresQuotaRepo struct {
	q        *sqlc.Queries
	defaults Quota
}

var _ QuotaRepo = (*PostgresQuotaRepo)(nil)

func NewQuotaRepo(pool *pgxpool.Pool, defaults Quota) *PostgresQuotaRepo {
	return &PostgresQuotaRepo{q: sqlc.New(pool), defaults: defaults}
}

func (r *PostgresQuotaRepo) Get(ctx context.Context, tenantID shared.TenantID) (Quota, error) {
	row, err := r.q.GetTenantQuota(ctx, postgres.UUIDToPg(uuid.UUID(tenantID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return r.defaults, nil
	}
	if err != nil {
		return Quota{}, fmt.Errorf("get tenant quota: %w", err)
	}
	return Quota{
		MaxJobsPerHour:       int(row.MaxJobsPerHour),
		MaxActiveRecurring:   int(row.MaxActiveRecurringJobs),
		MaxConcurrentRuns:    int(row.MaxConcurrentRuns),
		MaxDailyLLMCostCents: int(row.MaxDailyLlmCostCents),
	}, nil
}

func (r *PostgresQuotaRepo) CountJobsSince(ctx context.Context, tenantID shared.TenantID, since time.Time) (int64, error) {
	count, err := r.q.CountJobsCreatedSince(ctx, sqlc.CountJobsCreatedSinceParams{
		TenantID: postgres.UUIDToPg(uuid.UUID(tenantID)),
		Since:    postgres.TimeToPg(since),
	})
	if err != nil {
		return 0, fmt.Errorf("count jobs created since: %w", err)
	}
	return count, nil
}

func (r *PostgresQuotaRepo) CountActiveRecurring(ctx context.Context, tenantID shared.TenantID) (int64, error) {
	count, err := r.q.CountActiveRecurringJobs(ctx, postgres.UUIDToPg(uuid.UUID(tenantID)))
	if err != nil {
		return 0, fmt.Errorf("count active recurring jobs: %w", err)
	}
	return count, nil
}
