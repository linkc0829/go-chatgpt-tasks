package task

import (
	"context"
	"time"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

//go:generate mockgen -source=ports.go -destination=mocks/mock_ports.go -package=mocks
type Repo interface {
	SaveJob(ctx context.Context, j *Job) error
	SaveRun(ctx context.Context, r *JobRun) error
	UpdateRunStatus(ctx context.Context, r *JobRun) error
	FindRunByID(ctx context.Context, id shared.JobRunID) (*JobRun, error)
	ListRuns(ctx context.Context, tenantID shared.TenantID, p shared.Pagination) ([]*JobRun, int64, error)
	ListRunsByJob(ctx context.Context, tenantID shared.TenantID, jobID shared.JobID, p shared.Pagination) ([]*JobRun, int64, error)
	ListEvents(ctx context.Context, tenantID shared.TenantID, runID shared.JobRunID) ([]*RunEvent, error)
	AppendEvent(ctx context.Context, e *RunEvent) error
	FindDueRuns(ctx context.Context, bucket int64, before time.Time, limit int32) ([]*JobRun, error)
	FindJob(ctx context.Context, id shared.JobID) (*Job, error)
	InsertRunIfAbsent(ctx context.Context, r *JobRun) (created bool, err error)
	FindTerminalRecurringRuns(ctx context.Context, since time.Time, limit int32) ([]NextRunSpec, error)
}

type QuotaRepo interface {
	Get(ctx context.Context, tenantID shared.TenantID) (Quota, error)
	CountJobsSince(ctx context.Context, tenantID shared.TenantID, since time.Time) (int64, error)
	CountActiveRecurring(ctx context.Context, tenantID shared.TenantID) (int64, error)
}

type QuotaRejectionRecorder interface {
	RecordQuotaRejection(tenantID shared.TenantID, reason string)
}

type IdempotencyStore interface {
	Lookup(ctx context.Context, key string) (rec IdempotencyRecord, found bool, err error)
	Begin(ctx context.Context, key, handler string, runID shared.JobRunID) (acquired bool, err error)
	Complete(ctx context.Context, key, responseHash string) error
}

type Queue interface {
	Enqueue(ctx context.Context, m JobRunMsg) error
	EnsureGroup(ctx context.Context) error
	Read(ctx context.Context, consumer string, count int64, block time.Duration) ([]QueuedMessage, error)
	Reclaim(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]QueuedMessage, error)
	Ack(ctx context.Context, streamID string) error
	DeadLetter(ctx context.Context, m JobRunMsg) error
}

type QueuedMessage struct {
	StreamID string
	Msg      JobRunMsg
}

type Executor interface {
	Execute(ctx context.Context, r *JobRun) error
}

type TenantResolver interface {
	ResolveTenant(ctx context.Context, userID shared.UserID) (shared.TenantID, error)
}

type TenantResolverFunc func(ctx context.Context, userID shared.UserID) (shared.TenantID, error)

func (f TenantResolverFunc) ResolveTenant(ctx context.Context, userID shared.UserID) (shared.TenantID, error) {
	return f(ctx, userID)
}
