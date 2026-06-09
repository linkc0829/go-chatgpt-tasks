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

type EventType string

const (
	EventJobRunCreated   EventType = "job_run.created"
	EventJobRunEnqueued  EventType = "job_run.enqueued"
	EventJobRunStarted   EventType = "job_run.started"
	EventJobRunSucceeded EventType = "job_run.succeeded"
	EventJobRunFailed    EventType = "job_run.failed"
	EventJobRunRetry     EventType = "job_run.retry_scheduled"
	EventJobRunDLQ       EventType = "job_run.dlq"
	EventJobCancelled    EventType = "job.cancelled"
)

type Quota struct {
	MaxJobsPerHour       int
	MaxActiveRecurring   int
	MaxConcurrentRuns    int
	MaxDailyLLMCostCents int
}

type ScheduleSpec struct {
	Type             Kind
	ScheduledAtUTC   time.Time
	RecurrenceRule   string
	LocalTime        string
	TimezoneID       string
	OriginalUserText string
	LegacyInterval   time.Duration
}

type Job struct {
	id               shared.JobID
	tenantID         shared.TenantID
	userID           shared.UserID
	kind             Kind
	description      string
	interval         time.Duration
	scheduleType     Kind
	scheduledAtUTC   time.Time
	recurrenceRule   string
	localTime        string
	timezoneID       string
	originalUserText string
	createdAt        time.Time
	updatedAt        time.Time
}

type JobRun struct {
	id             shared.JobRunID
	tenantID       shared.TenantID
	jobID          shared.JobID
	sequence       int
	status         Status
	scheduledAt    time.Time
	timeBucket     int64
	attempts       int
	errorCode      string
	errorMsg       string
	startedAt      time.Time
	completedAt    time.Time
	failedAt       time.Time
	createdAt      time.Time
	updatedAt      time.Time
	scheduleType   Kind
	recurrenceRule string
	localTime      string
	timezoneID     string
}

type RunEvent struct {
	id        shared.RunEventID
	tenantID  shared.TenantID
	jobID     shared.JobID
	jobRunID  shared.JobRunID
	status    Status
	eventType EventType
	payload   map[string]any
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

func NewJob(tenantID shared.TenantID, userID shared.UserID, description string, schedule ScheduleSpec) (*Job, error) {
	if tenantID.IsZero() || userID.IsZero() {
		return nil, ErrInvalidOwner
	}
	if strings.TrimSpace(description) == "" {
		return nil, ErrInvalidDescription
	}
	if schedule.Type != KindOneOff && schedule.Type != KindRecurring {
		return nil, ErrInvalidSchedule
	}
	if schedule.TimezoneID == "" {
		schedule.TimezoneID = "UTC"
	}
	if _, err := time.LoadLocation(schedule.TimezoneID); err != nil {
		return nil, ErrInvalidTimezone
	}
	if schedule.Type == KindRecurring {
		if _, err := ParseRule(schedule.RecurrenceRule); err != nil {
			return nil, err
		}
		if _, _, err := parseLocalTime(schedule.LocalTime); err != nil {
			return nil, err
		}
	}

	now := time.Now().UTC()
	return &Job{
		id:               shared.NewJobID(),
		tenantID:         tenantID,
		userID:           userID,
		kind:             schedule.Type,
		description:      description,
		interval:         schedule.LegacyInterval,
		scheduleType:     schedule.Type,
		scheduledAtUTC:   schedule.ScheduledAtUTC.UTC(),
		recurrenceRule:   schedule.RecurrenceRule,
		localTime:        schedule.LocalTime,
		timezoneID:       schedule.TimezoneID,
		originalUserText: schedule.OriginalUserText,
		createdAt:        now,
		updatedAt:        now,
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

func NewRunEvent(
	tenantID shared.TenantID,
	jobID shared.JobID,
	runID shared.JobRunID,
	s Status,
	eventType EventType,
	payload map[string]any,
) *RunEvent {
	return &RunEvent{
		id:        shared.NewRunEventID(),
		tenantID:  tenantID,
		jobID:     jobID,
		jobRunID:  runID,
		status:    s,
		eventType: eventType,
		payload:   clonePayload(payload),
		createdAt: time.Now().UTC(),
	}
}

func clonePayload(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	out := make(map[string]any, len(payload))
	for k, v := range payload {
		out[k] = v
	}
	return out
}

func rehydrateJob(
	id shared.JobID,
	tenantID shared.TenantID,
	userID shared.UserID,
	kind Kind,
	description string,
	interval time.Duration,
	scheduleType Kind,
	scheduledAtUTC time.Time,
	recurrenceRule string,
	localTime string,
	timezoneID string,
	originalUserText string,
	createdAt time.Time,
	updatedAt time.Time,
) *Job {
	return &Job{
		id:               id,
		tenantID:         tenantID,
		userID:           userID,
		kind:             kind,
		description:      description,
		interval:         interval,
		scheduleType:     scheduleType,
		scheduledAtUTC:   scheduledAtUTC,
		recurrenceRule:   recurrenceRule,
		localTime:        localTime,
		timezoneID:       timezoneID,
		originalUserText: originalUserText,
		createdAt:        createdAt,
		updatedAt:        updatedAt,
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
	errorCode string,
	errorMsg string,
	startedAt time.Time,
	completedAt time.Time,
	failedAt time.Time,
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
		errorCode:   errorCode,
		errorMsg:    errorMsg,
		startedAt:   startedAt,
		completedAt: completedAt,
		failedAt:    failedAt,
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
	now := time.Now().UTC()
	r.startedAt = now
	r.updatedAt = now
	return nil
}

func (r *JobRun) MarkSuccess() error {
	if r.status != StatusRunning {
		return ErrInvalidStatusTransition
	}
	r.status = StatusSuccess
	now := time.Now().UTC()
	r.completedAt = now
	r.updatedAt = now
	return nil
}

func (r *JobRun) MarkRetry() error {
	if r.status != StatusRunning {
		return ErrInvalidStatusTransition
	}
	r.status = StatusRetry
	r.attempts++
	now := time.Now().UTC()
	r.failedAt = now
	r.updatedAt = now
	return nil
}

func (r *JobRun) MarkFailed() error {
	if r.status != StatusRunning && r.status != StatusRetry {
		return ErrInvalidStatusTransition
	}
	r.status = StatusFailed
	now := time.Now().UTC()
	r.failedAt = now
	r.updatedAt = now
	return nil
}

func (r *JobRun) Cancel() error {
	if r.IsTerminal() {
		return ErrInvalidStatusTransition
	}
	r.status = StatusCancelled
	now := time.Now().UTC()
	r.completedAt = now
	r.updatedAt = now
	return nil
}

func (r *JobRun) setError(code, msg string) {
	r.errorCode = code
	r.errorMsg = msg
}

func (r *JobRun) IsTerminal() bool {
	return r.status == StatusSuccess || r.status == StatusFailed || r.status == StatusCancelled
}

func (r *JobRun) setSchedule(j *Job) {
	r.scheduleType = j.ScheduleType()
	r.recurrenceRule = j.RecurrenceRule()
	r.localTime = j.LocalTime()
	r.timezoneID = j.TimezoneID()
}

func (j *Job) ID() shared.JobID          { return j.id }
func (j *Job) TenantID() shared.TenantID { return j.tenantID }
func (j *Job) UserID() shared.UserID     { return j.userID }
func (j *Job) Kind() Kind                { return j.kind }
func (j *Job) Description() string       { return j.description }
func (j *Job) Interval() time.Duration   { return j.interval }
func (j *Job) ScheduleType() Kind        { return j.scheduleType }
func (j *Job) ScheduledAtUTC() time.Time { return j.scheduledAtUTC }
func (j *Job) RecurrenceRule() string    { return j.recurrenceRule }
func (j *Job) LocalTime() string         { return j.localTime }
func (j *Job) TimezoneID() string        { return j.timezoneID }
func (j *Job) OriginalUserText() string  { return j.originalUserText }
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
func (r *JobRun) ErrorCode() string      { return r.errorCode }
func (r *JobRun) ErrorMessage() string   { return r.errorMsg }
func (r *JobRun) StartedAt() time.Time   { return r.startedAt }
func (r *JobRun) CompletedAt() time.Time { return r.completedAt }
func (r *JobRun) FailedAt() time.Time    { return r.failedAt }
func (r *JobRun) CreatedAt() time.Time   { return r.createdAt }
func (r *JobRun) UpdatedAt() time.Time   { return r.updatedAt }
func (r *JobRun) ScheduleType() Kind     { return r.scheduleType }
func (r *JobRun) RecurrenceRule() string { return r.recurrenceRule }
func (r *JobRun) LocalTime() string      { return r.localTime }
func (r *JobRun) TimezoneID() string     { return r.timezoneID }
func (e *RunEvent) ID() shared.RunEventID {
	return e.id
}
func (e *RunEvent) TenantID() shared.TenantID { return e.tenantID }
func (e *RunEvent) JobID() shared.JobID       { return e.jobID }
func (e *RunEvent) JobRunID() shared.JobRunID { return e.jobRunID }
func (e *RunEvent) Status() Status            { return e.status }
func (e *RunEvent) EventType() EventType      { return e.eventType }
func (e *RunEvent) Payload() map[string]any   { return clonePayload(e.payload) }
func (e *RunEvent) CreatedAt() time.Time      { return e.createdAt }
