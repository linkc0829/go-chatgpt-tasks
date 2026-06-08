// Package task is a feature slice for scheduled task management.
package task

import (
	"strings"
	"time"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

type Kind string

const (
	KindOneOff    Kind = "one_off"
	KindRecurring Kind = "recurring"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSuccess   Status = "success"
	StatusRetry     Status = "retry"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

type Job struct {
	id          shared.JobID
	tenantID    shared.TenantID
	userID      shared.UserID
	kind        Kind
	description string
	interval    time.Duration
	createdAt   time.Time
	updatedAt   time.Time
}

type JobRun struct {
	id          shared.JobRunID
	tenantID    shared.TenantID
	jobID       shared.JobID
	sequence    int
	status      Status
	scheduledAt time.Time
	timeBucket  int64
	attempts    int
	createdAt   time.Time
	updatedAt   time.Time
}

type RunEvent struct {
	id        shared.RunEventID
	tenantID  shared.TenantID
	jobID     shared.JobID
	jobRunID  shared.JobRunID
	status    Status
	createdAt time.Time
}

func bucketOf(t time.Time) int64 {
	return t.UTC().Truncate(time.Hour).Unix()
}

var (
	LegacyTenantID = mustTenantID("00000000-0000-0000-0000-0000000000aa")
	LegacyUserID   = mustUserID("00000000-0000-0000-0000-0000000000bb")
)

func mustTenantID(s string) shared.TenantID {
	id, err := shared.ParseTenantID(s)
	if err != nil {
		panic(err)
	}
	return id
}

func mustUserID(s string) shared.UserID {
	id, err := shared.ParseUserID(s)
	if err != nil {
		panic(err)
	}
	return id
}

func NewJob(tenantID shared.TenantID, userID shared.UserID, kind Kind, description string, interval time.Duration) (*Job, error) {
	if tenantID.IsZero() || userID.IsZero() {
		return nil, ErrInvalidOwner
	}
	if strings.TrimSpace(description) == "" {
		return nil, ErrInvalidDescription
	}
	if kind == KindRecurring && interval <= 0 {
		return nil, ErrInvalidSchedule
	}
	if kind != KindOneOff && kind != KindRecurring {
		return nil, ErrInvalidSchedule
	}

	now := time.Now().UTC()
	return &Job{
		id:          shared.NewJobID(),
		tenantID:    tenantID,
		userID:      userID,
		kind:        kind,
		description: description,
		interval:    interval,
		createdAt:   now,
		updatedAt:   now,
	}, nil
}

func NewJobRun(tenantID shared.TenantID, jobID shared.JobID, sequence int, scheduledAt time.Time) (*JobRun, error) {
	if tenantID.IsZero() || jobID.IsZero() || sequence < 1 || scheduledAt.IsZero() {
		return nil, ErrInvalidSchedule
	}

	now := time.Now().UTC()
	scheduledAt = scheduledAt.UTC()
	return &JobRun{
		id:          shared.NewJobRunID(),
		tenantID:    tenantID,
		jobID:       jobID,
		sequence:    sequence,
		status:      StatusPending,
		scheduledAt: scheduledAt,
		timeBucket:  bucketOf(scheduledAt),
		createdAt:   now,
		updatedAt:   now,
	}, nil
}

func NewRunEvent(tenantID shared.TenantID, jobID shared.JobID, runID shared.JobRunID, s Status) *RunEvent {
	return &RunEvent{
		id:        shared.NewRunEventID(),
		tenantID:  tenantID,
		jobID:     jobID,
		jobRunID:  runID,
		status:    s,
		createdAt: time.Now().UTC(),
	}
}

func rehydrateJob(
	id shared.JobID,
	tenantID shared.TenantID,
	userID shared.UserID,
	kind Kind,
	description string,
	interval time.Duration,
	createdAt time.Time,
	updatedAt time.Time,
) *Job {
	return &Job{
		id:          id,
		tenantID:    tenantID,
		userID:      userID,
		kind:        kind,
		description: description,
		interval:    interval,
		createdAt:   createdAt,
		updatedAt:   updatedAt,
	}
}

func rehydrateJobRun(
	id shared.JobRunID,
	tenantID shared.TenantID,
	jobID shared.JobID,
	sequence int,
	status Status,
	scheduledAt time.Time,
	timeBucket int64,
	attempts int,
	createdAt time.Time,
	updatedAt time.Time,
) *JobRun {
	return &JobRun{
		id:          id,
		tenantID:    tenantID,
		jobID:       jobID,
		sequence:    sequence,
		status:      status,
		scheduledAt: scheduledAt,
		timeBucket:  timeBucket,
		attempts:    attempts,
		createdAt:   createdAt,
		updatedAt:   updatedAt,
	}
}

func (r *JobRun) MarkQueued() error {
	if r.status != StatusPending && r.status != StatusRetry {
		return ErrInvalidStatusTransition
	}
	r.status = StatusQueued
	r.updatedAt = time.Now().UTC()
	return nil
}

func (r *JobRun) MarkRunning() error {
	if r.status != StatusQueued && r.status != StatusRetry {
		return ErrInvalidStatusTransition
	}
	r.status = StatusRunning
	r.updatedAt = time.Now().UTC()
	return nil
}

func (r *JobRun) MarkSuccess() error {
	if r.status != StatusRunning {
		return ErrInvalidStatusTransition
	}
	r.status = StatusSuccess
	r.updatedAt = time.Now().UTC()
	return nil
}

func (r *JobRun) MarkRetry() error {
	if r.status != StatusRunning {
		return ErrInvalidStatusTransition
	}
	r.status = StatusRetry
	r.attempts++
	r.updatedAt = time.Now().UTC()
	return nil
}

func (r *JobRun) MarkFailed() error {
	if r.status != StatusRunning && r.status != StatusRetry {
		return ErrInvalidStatusTransition
	}
	r.status = StatusFailed
	r.updatedAt = time.Now().UTC()
	return nil
}

func (r *JobRun) Cancel() error {
	if r.IsTerminal() {
		return ErrInvalidStatusTransition
	}
	r.status = StatusCancelled
	r.updatedAt = time.Now().UTC()
	return nil
}

func (r *JobRun) IsTerminal() bool {
	return r.status == StatusSuccess || r.status == StatusFailed || r.status == StatusCancelled
}

func (j *Job) ID() shared.JobID          { return j.id }
func (j *Job) TenantID() shared.TenantID { return j.tenantID }
func (j *Job) UserID() shared.UserID     { return j.userID }
func (j *Job) Kind() Kind                { return j.kind }
func (j *Job) Description() string       { return j.description }
func (j *Job) Interval() time.Duration   { return j.interval }
func (j *Job) CreatedAt() time.Time      { return j.createdAt }
func (j *Job) UpdatedAt() time.Time      { return j.updatedAt }
func (r *JobRun) ID() shared.JobRunID    { return r.id }
func (r *JobRun) TenantID() shared.TenantID {
	return r.tenantID
}
func (r *JobRun) JobID() shared.JobID    { return r.jobID }
func (r *JobRun) Sequence() int          { return r.sequence }
func (r *JobRun) Status() Status         { return r.status }
func (r *JobRun) ScheduledAt() time.Time { return r.scheduledAt }
func (r *JobRun) TimeBucket() int64      { return r.timeBucket }
func (r *JobRun) Attempts() int          { return r.attempts }
func (r *JobRun) CreatedAt() time.Time   { return r.createdAt }
func (r *JobRun) UpdatedAt() time.Time   { return r.updatedAt }
func (e *RunEvent) ID() shared.RunEventID {
	return e.id
}
func (e *RunEvent) TenantID() shared.TenantID { return e.tenantID }
func (e *RunEvent) JobID() shared.JobID       { return e.jobID }
func (e *RunEvent) JobRunID() shared.JobRunID { return e.jobRunID }
func (e *RunEvent) Status() Status            { return e.status }
func (e *RunEvent) CreatedAt() time.Time      { return e.createdAt }
