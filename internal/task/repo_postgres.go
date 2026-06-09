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

type PostgresRepo struct {
	pool *pgxpool.Pool
	q    *sqlc.Queries
}

var _ Repo = (*PostgresRepo)(nil)

func NewPostgresRepo(pool *pgxpool.Pool) *PostgresRepo {
	return &PostgresRepo{pool: pool, q: sqlc.New(pool)}
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

func (r *PostgresRepo) CreateJobWithRun(ctx context.Context, j *Job, run *JobRun, events []*RunEvent) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin create job: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := r.q.WithTx(tx)
	if err := q.InsertJob(ctx, jobToInsertParams(j)); err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	if err := q.InsertJobRun(ctx, jobRunToInsertParams(run)); err != nil {
		return fmt.Errorf("insert job run: %w", err)
	}
	for _, event := range events {
		params, err := runEventToInsertParams(event)
		if err != nil {
			return err
		}
		if err := q.InsertRunEvent(ctx, params); err != nil {
			return fmt.Errorf("insert run event: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit create job: %w", err)
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

func (r *PostgresRepo) PersistRunTransition(ctx context.Context, run *JobRun, event *RunEvent) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin run transition: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := r.q.WithTx(tx)
	rows, err := q.UpdateJobRunStatus(ctx, jobRunToUpdateStatusParams(run))
	if err != nil {
		return fmt.Errorf("update job run transition: %w", err)
	}
	if rows == 0 {
		return ErrJobRunNotFound
	}
	params, err := runEventToInsertParams(event)
	if err != nil {
		return err
	}
	if err := q.InsertRunEvent(ctx, params); err != nil {
		return fmt.Errorf("insert run transition event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit run transition: %w", err)
	}
	return nil
}

func (r *PostgresRepo) TryMarkRunRunning(ctx context.Context, run *JobRun, event *RunEvent, limit int) (bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin run slot claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := r.q.WithTx(tx)
	acquired, err := q.TryMarkJobRunRunning(ctx, sqlc.TryMarkJobRunRunningParams{
		TenantID:  postgres.UUIDToPg(uuid.UUID(run.TenantID())),
		StartedAt: postgres.TimeToPg(run.StartedAt()),
		UpdatedAt: postgres.TimeToPg(run.UpdatedAt()),
		ID:        postgres.UUIDToPg(uuid.UUID(run.ID())),
		RunLimit:  int64(limit),
	})
	if err != nil {
		return false, fmt.Errorf("try mark job run running: %w", err)
	}
	if !acquired {
		return false, nil
	}
	params, err := runEventToInsertParams(event)
	if err != nil {
		return false, err
	}
	if err := q.InsertRunEvent(ctx, params); err != nil {
		return false, fmt.Errorf("insert job run started event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit run slot claim: %w", err)
	}
	return acquired, nil
}

func (r *PostgresRepo) FindRunByID(ctx context.Context, id shared.JobRunID) (*JobRun, error) {
	row, err := r.q.GetJobRunByID(ctx, postgres.UUIDToPg(uuid.UUID(id)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrJobRunNotFound
		}
		return nil, fmt.Errorf("query job run: %w", err)
	}
	return jobRunFromGetByIDRow(row), nil
}

func (r *PostgresRepo) ListRuns(ctx context.Context, tenantID shared.TenantID, p shared.Pagination) ([]*JobRun, int64, error) {
	rows, err := r.q.ListJobRuns(ctx, sqlc.ListJobRunsParams{
		TenantID:   postgres.UUIDToPg(uuid.UUID(tenantID)),
		PageLimit:  int32(p.Limit),  //nolint:gosec // shared pagination clamps to maxLimit=100.
		PageOffset: int32(p.Offset), //nolint:gosec // shared pagination normalizes to non-negative API bounds.
	})
	if err != nil {
		return nil, 0, fmt.Errorf("list job runs: %w", err)
	}

	out := make([]*JobRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, jobRunFromListRow(row))
	}

	total, err := r.q.CountJobRuns(ctx, postgres.UUIDToPg(uuid.UUID(tenantID)))
	if err != nil {
		return nil, 0, fmt.Errorf("count job runs: %w", err)
	}
	return out, total, nil
}

func (r *PostgresRepo) ListRunsByJob(
	ctx context.Context,
	tenantID shared.TenantID,
	jobID shared.JobID,
	p shared.Pagination,
) ([]*JobRun, int64, error) {
	rows, err := r.q.ListJobRunsByJob(ctx, sqlc.ListJobRunsByJobParams{
		TenantID:   postgres.UUIDToPg(uuid.UUID(tenantID)),
		JobID:      postgres.UUIDToPg(uuid.UUID(jobID)),
		PageLimit:  int32(p.Limit),  //nolint:gosec // shared pagination clamps to maxLimit=100.
		PageOffset: int32(p.Offset), //nolint:gosec // shared pagination normalizes to non-negative API bounds.
	})
	if err != nil {
		return nil, 0, fmt.Errorf("list job runs by job: %w", err)
	}

	out := make([]*JobRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, jobRunFromJobRow(row))
	}

	total, err := r.q.CountJobRunsByJob(ctx, sqlc.CountJobRunsByJobParams{
		TenantID: postgres.UUIDToPg(uuid.UUID(tenantID)),
		JobID:    postgres.UUIDToPg(uuid.UUID(jobID)),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("count job runs by job: %w", err)
	}
	return out, total, nil
}

func (r *PostgresRepo) AppendEvent(ctx context.Context, e *RunEvent) error {
	params, err := runEventToInsertParams(e)
	if err != nil {
		return err
	}
	if err := r.q.InsertRunEvent(ctx, params); err != nil {
		return fmt.Errorf("insert run event: %w", err)
	}
	return nil
}

func (r *PostgresRepo) ListEvents(ctx context.Context, tenantID shared.TenantID, runID shared.JobRunID) ([]*RunEvent, error) {
	rows, err := r.q.ListRunEventsByRun(ctx, sqlc.ListRunEventsByRunParams{
		TenantID: postgres.UUIDToPg(uuid.UUID(tenantID)),
		JobRunID: postgres.UUIDToPg(uuid.UUID(runID)),
	})
	if err != nil {
		return nil, fmt.Errorf("list run events: %w", err)
	}

	out := make([]*RunEvent, 0, len(rows))
	for _, row := range rows {
		event, err := runEventFromSqlc(row)
		if err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, nil
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
		out = append(out, jobRunFromDueRow(row))
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

func (r *PostgresRepo) CancelPendingRunsByJob(
	ctx context.Context,
	tenantID shared.TenantID,
	jobID shared.JobID,
) ([]*JobRun, error) {
	now := time.Now().UTC()
	rows, err := r.q.CancelPendingJobRuns(ctx, sqlc.CancelPendingJobRunsParams{
		CompletedAt: postgres.TimeToPg(now),
		UpdatedAt:   postgres.TimeToPg(now),
		TenantID:    postgres.UUIDToPg(uuid.UUID(tenantID)),
		JobID:       postgres.UUIDToPg(uuid.UUID(jobID)),
	})
	if err != nil {
		return nil, fmt.Errorf("cancel pending job runs: %w", err)
	}
	out := make([]*JobRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, jobRunFromCancelPendingRow(row))
	}
	return out, nil
}

func (r *PostgresRepo) FindChildren(ctx context.Context, jobID shared.JobID, status Status) ([]*Job, error) {
	rows, err := r.q.FindChildJobs(ctx, sqlc.FindChildJobsParams{
		ParentJobID:           postgres.UUIDToPg(uuid.UUID(jobID)),
		TriggerOnParentStatus: stringPtr(string(status)),
	})
	if err != nil {
		return nil, fmt.Errorf("find child jobs: %w", err)
	}
	out := make([]*Job, 0, len(rows))
	for _, row := range rows {
		out = append(out, jobFromChildRow(row))
	}
	return out, nil
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
