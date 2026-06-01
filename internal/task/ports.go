package task

import (
	"context"
	"time"

	"github.com/linkc0829/go-backend-template/internal/shared"
)

//go:generate mockgen -source=ports.go -destination=mocks/mock_ports.go -package=mocks
type Repo interface {
	SaveJob(ctx context.Context, j *Job) error
	SaveRun(ctx context.Context, r *JobRun) error
	UpdateRunStatus(ctx context.Context, r *JobRun) error
	FindRunByID(ctx context.Context, id shared.JobRunID) (*JobRun, error)
	ListRuns(ctx context.Context, p shared.Pagination) ([]*JobRun, int64, error)
	AppendEvent(ctx context.Context, e *RunEvent) error
	FindDueRuns(ctx context.Context, bucket int64, before time.Time, limit int32) ([]*JobRun, error)
	FindJob(ctx context.Context, id shared.JobID) (*Job, error)
	InsertRunIfAbsent(ctx context.Context, r *JobRun) (created bool, err error)
	FindTerminalRecurringRuns(ctx context.Context, since time.Time, limit int32) ([]NextRunSpec, error)
}

type Queue interface {
	Enqueue(ctx context.Context, m JobRunMsg) error
}
