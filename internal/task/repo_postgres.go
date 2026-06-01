package task

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/linkc0829/go-backend-template/internal/platform/postgres"
	"github.com/linkc0829/go-backend-template/internal/platform/postgres/sqlc"
	"github.com/linkc0829/go-backend-template/internal/shared"
)

type PostgresRepo struct {
	q *sqlc.Queries
}

var _ Repo = (*PostgresRepo)(nil)

func NewPostgresRepo(pool *pgxpool.Pool) *PostgresRepo {
	return &PostgresRepo{q: sqlc.New(pool)}
}

func (r *PostgresRepo) SaveJob(ctx context.Context, j *Job) error {
	if err := r.q.InsertJob(ctx, jobToInsertParams(j)); err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

func (r *PostgresRepo) SaveRun(ctx context.Context, run *JobRun) error {
	if err := r.q.InsertJobRun(ctx, jobRunToInsertParams(run)); err != nil {
		return fmt.Errorf("insert job run: %w", err)
	}
	return nil
}

func (r *PostgresRepo) UpdateRunStatus(ctx context.Context, run *JobRun) error {
	rows, err := r.q.UpdateJobRunStatus(ctx, jobRunToUpdateStatusParams(run))
	if err != nil {
		return fmt.Errorf("update job run: %w", err)
	}
	if rows == 0 {
		return ErrJobRunNotFound
	}
	return nil
}

func (r *PostgresRepo) FindRunByID(ctx context.Context, id shared.JobRunID) (*JobRun, error) {
	row, err := r.q.GetJobRunByID(ctx, postgres.UUIDToPg(uuid.UUID(id)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrJobRunNotFound
		}
		return nil, fmt.Errorf("query job run: %w", err)
	}
	return jobRunFromSqlc(row), nil
}

func (r *PostgresRepo) ListRuns(ctx context.Context, p shared.Pagination) ([]*JobRun, int64, error) {
	rows, err := r.q.ListJobRuns(ctx, sqlc.ListJobRunsParams{
		PageLimit:  int32(p.Limit),  //nolint:gosec // shared pagination clamps to maxLimit=100.
		PageOffset: int32(p.Offset), //nolint:gosec // shared pagination normalizes to non-negative API bounds.
	})
	if err != nil {
		return nil, 0, fmt.Errorf("list job runs: %w", err)
	}

	out := make([]*JobRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, jobRunFromSqlc(row))
	}

	total, err := r.q.CountJobRuns(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("count job runs: %w", err)
	}
	return out, total, nil
}

func (r *PostgresRepo) AppendEvent(ctx context.Context, e *RunEvent) error {
	if err := r.q.InsertRunEvent(ctx, runEventToInsertParams(e)); err != nil {
		return fmt.Errorf("insert run event: %w", err)
	}
	return nil
}

func (r *PostgresRepo) FindDueRuns(
	ctx context.Context,
	bucket int64,
	before time.Time,
	limit int32,
) ([]*JobRun, error) {
	rows, err := r.q.FindDueJobRuns(ctx, sqlc.FindDueJobRunsParams{
		TimeBucket: bucket,
		DueBefore:  postgres.TimeToPg(before),
		Lim:        limit,
	})
	if err != nil {
		return nil, fmt.Errorf("find due job runs: %w", err)
	}

	out := make([]*JobRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, jobRunFromSqlc(row))
	}
	return out, nil
}

func (r *PostgresRepo) FindJob(ctx context.Context, id shared.JobID) (*Job, error) {
	row, err := r.q.GetJobByID(ctx, postgres.UUIDToPg(uuid.UUID(id)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("query job: %w", err)
	}
	return jobFromSqlc(row), nil
}

func (r *PostgresRepo) InsertRunIfAbsent(ctx context.Context, run *JobRun) (bool, error) {
	rows, err := r.q.InsertJobRunIfAbsent(ctx, jobRunToInsertIfAbsentParams(run))
	if err != nil {
		return false, fmt.Errorf("insert job run if absent: %w", err)
	}
	return rows > 0, nil
}

func (r *PostgresRepo) FindTerminalRecurringRuns(
	ctx context.Context,
	since time.Time,
	limit int32,
) ([]NextRunSpec, error) {
	rows, err := r.q.ListTerminalRecurringRuns(ctx, sqlc.ListTerminalRecurringRunsParams{
		Since: postgres.TimeToPg(since),
		Lim:   limit,
	})
	if err != nil {
		return nil, fmt.Errorf("list terminal recurring runs: %w", err)
	}

	out := make([]NextRunSpec, 0, len(rows))
	for _, row := range rows {
		out = append(out, nextRunSpecFromSqlc(row))
	}
	return out, nil
}
