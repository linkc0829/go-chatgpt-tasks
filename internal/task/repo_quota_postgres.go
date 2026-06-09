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

func (r *PostgresQuotaRepo) CountActiveRuns(ctx context.Context, tenantID shared.TenantID) (int64, error) {
	count, err := r.q.CountActiveRuns(ctx, postgres.UUIDToPg(uuid.UUID(tenantID)))
	if err != nil {
		return 0, fmt.Errorf("count active runs: %w", err)
	}
	return count, nil
}

func (r *PostgresQuotaRepo) ReserveDailyCost(ctx context.Context, tenantID shared.TenantID, costCents, limitCents int) (bool, error) {
	// A brand-new row inserts unconditionally (no ON CONFLICT), so the WHERE
	// guard cannot reject a first-of-day reservation that alone exceeds the
	// limit; reject that case here before touching the table.
	if costCents > limitCents {
		return false, nil
	}
	_, err := r.q.ReserveDailyLLMCost(ctx, sqlc.ReserveDailyLLMCostParams{
		TenantID:   postgres.UUIDToPg(uuid.UUID(tenantID)),
		CostDate:   time.Now().UTC().Format(time.DateOnly),
		CostCents:  int32(costCents),  //nolint:gosec // cost in cents derives from token budgets, bounded well within int32.
		LimitCents: int32(limitCents), //nolint:gosec // limit in cents comes from config quota, bounded well within int32.
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reserve daily llm cost: %w", err)
	}
	return true, nil
}

func (r *PostgresQuotaRepo) AdjustDailyCost(ctx context.Context, tenantID shared.TenantID, deltaCents int) error {
	if deltaCents == 0 {
		return nil
	}
	if err := r.q.AdjustDailyLLMCost(ctx, sqlc.AdjustDailyLLMCostParams{
		DeltaCents: int32(deltaCents), //nolint:gosec // signed cost delta in cents, bounded well within int32.
		TenantID:   postgres.UUIDToPg(uuid.UUID(tenantID)),
		CostDate:   time.Now().UTC().Format(time.DateOnly),
	}); err != nil {
		return fmt.Errorf("adjust daily llm cost: %w", err)
	}
	return nil
}
