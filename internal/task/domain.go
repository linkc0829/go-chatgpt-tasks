// Package task is a feature slice for scheduled task management.
package task

import (
	"strings"
	"time"

	"github.com/linkc0829/go-backend-template/internal/shared"
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
	kind        Kind
	description string
	interval    time.Duration
	createdAt   time.Time
	updatedAt   time.Time
}

type JobRun struct {
	id          shared.JobRunID
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
	jobRunID  shared.JobRunID
	status    Status
	createdAt time.Time
}

func bucketOf(t time.Time) int64 {
	return t.UTC().Truncate(time.Hour).Unix()
}

func NewJob(kind Kind, description string, interval time.Duration) (*Job, error) {
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
		kind:        kind,
		description: description,
		interval:    interval,
		createdAt:   now,
		updatedAt:   now,
	}, nil
}

func NewJobRun(jobID shared.JobID, sequence int, scheduledAt time.Time) (*JobRun, error) {
	if jobID.IsZero() || sequence < 1 || scheduledAt.IsZero() {
		return nil, ErrInvalidSchedule
	}

	now := time.Now().UTC()
	scheduledAt = scheduledAt.UTC()
	return &JobRun{
		id:          shared.NewJobRunID(),
		jobID:       jobID,
		sequence:    sequence,
		status:      StatusPending,
		scheduledAt: scheduledAt,
		timeBucket:  bucketOf(scheduledAt),
		createdAt:   now,
		updatedAt:   now,
	}, nil
}

func NewRunEvent(runID shared.JobRunID, s Status) *RunEvent {
	return &RunEvent{
		id:        shared.NewRunEventID(),
		jobRunID:  runID,
		status:    s,
		createdAt: time.Now().UTC(),
	}
}

func rehydrateJob(
	id shared.JobID,
	kind Kind,
	description string,
	interval time.Duration,
	createdAt time.Time,
	updatedAt time.Time,
) *Job {
	return &Job{
		id:          id,
		kind:        kind,
		description: description,
		interval:    interval,
		createdAt:   createdAt,
		updatedAt:   updatedAt,
	}
}

func rehydrateJobRun(
	id shared.JobRunID,
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
func (j *Job) Kind() Kind                { return j.kind }
func (j *Job) Description() string       { return j.description }
func (j *Job) Interval() time.Duration   { return j.interval }
func (j *Job) CreatedAt() time.Time      { return j.createdAt }
func (j *Job) UpdatedAt() time.Time      { return j.updatedAt }
func (r *JobRun) ID() shared.JobRunID    { return r.id }
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
func (e *RunEvent) JobRunID() shared.JobRunID { return e.jobRunID }
func (e *RunEvent) Status() Status            { return e.status }
func (e *RunEvent) CreatedAt() time.Time      { return e.createdAt }
