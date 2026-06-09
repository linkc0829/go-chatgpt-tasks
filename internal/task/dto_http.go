package task

import (
	"time"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

type CreateJobRequest struct {
	Description              string `json:"description" binding:"required,min=1"`
	ScheduledAt              string `json:"scheduled_at,omitempty"`
	RecurringIntervalSeconds int64  `json:"recurring_interval_seconds,omitempty" binding:"omitempty,min=1"`
	ScheduleType             Kind   `json:"schedule_type,omitempty"`
	RecurrenceRule           string `json:"recurrence_rule,omitempty"`
	LocalTime                string `json:"local_time,omitempty"`
	TimezoneID               string `json:"timezone_id,omitempty"`
	OriginalUserText         string `json:"original_user_text,omitempty"`
}

func (r CreateJobRequest) toInput() (CreateInput, error) {
	var scheduledAt time.Time
	if r.ScheduledAt != "" {
		var err error
		scheduledAt, err = time.Parse(time.RFC3339, r.ScheduledAt)
		if err != nil {
			return CreateInput{}, ErrInvalidSchedule
		}
	}
	return CreateInput{
		Description:      r.Description,
		ScheduledAt:      scheduledAt,
		Interval:         time.Duration(r.RecurringIntervalSeconds) * time.Second,
		ScheduleType:     r.ScheduleType,
		RecurrenceRule:   r.RecurrenceRule,
		LocalTime:        r.LocalTime,
		TimezoneID:       r.TimezoneID,
		OriginalUserText: r.OriginalUserText,
	}, nil
}

type JobResponse struct {
	ID          shared.JobID    `json:"id"`
	TenantID    shared.TenantID `json:"tenant_id"`
	UserID      shared.UserID   `json:"user_id"`
	Kind        string          `json:"kind"`
	Description string          `json:"description"`
	Interval    int64           `json:"interval_seconds"`
	CreatedAt   string          `json:"created_at"`
	UpdatedAt   string          `json:"updated_at"`
}

type RunResponse struct {
	ID             shared.JobRunID `json:"id"`
	TenantID       shared.TenantID `json:"tenant_id"`
	JobID          shared.JobID    `json:"job_id"`
	Status         string          `json:"status"`
	ScheduledAt    string          `json:"scheduled_at"`
	Sequence       int             `json:"sequence"`
	Attempts       int             `json:"attempts"`
	ErrorCode      string          `json:"error_code,omitempty"`
	ErrorMessage   string          `json:"error_message,omitempty"`
	StartedAt      string          `json:"started_at,omitempty"`
	CompletedAt    string          `json:"completed_at,omitempty"`
	FailedAt       string          `json:"failed_at,omitempty"`
	CreatedAt      string          `json:"created_at"`
	UpdatedAt      string          `json:"updated_at"`
	ScheduleType   string          `json:"schedule_type,omitempty"`
	RecurrenceRule string          `json:"recurrence_rule,omitempty"`
	LocalTime      string          `json:"local_time,omitempty"`
	TimezoneID     string          `json:"timezone_id,omitempty"`
	NextRunAtUTC   string          `json:"next_run_at_utc,omitempty"`
	ParentJobID    *shared.JobID   `json:"parent_job_id,omitempty"`
	ChildJobIDs    []shared.JobID  `json:"children,omitempty"`
}

type ListRunsResponse struct {
	Runs   []RunResponse `json:"runs"`
	Total  int64         `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

type RunEventResponse struct {
	ID        shared.RunEventID `json:"id"`
	TenantID  shared.TenantID   `json:"tenant_id"`
	JobID     shared.JobID      `json:"job_id"`
	JobRunID  shared.JobRunID   `json:"job_run_id"`
	Status    string            `json:"status"`
	EventType string            `json:"event_type"`
	Payload   map[string]any    `json:"payload"`
	CreatedAt string            `json:"created_at"`
}

type ListEventsResponse struct {
	Events []RunEventResponse `json:"events"`
}

func runToHTTPResponse(run *JobRun) RunResponse {
	return RunResponse{
		ID:             run.ID(),
		TenantID:       run.TenantID(),
		JobID:          run.JobID(),
		Status:         string(run.Status()),
		ScheduledAt:    run.ScheduledAt().Format(time.RFC3339),
		Sequence:       run.Sequence(),
		Attempts:       run.Attempts(),
		ErrorCode:      run.ErrorCode(),
		ErrorMessage:   run.ErrorMessage(),
		StartedAt:      formatTime(run.StartedAt()),
		CompletedAt:    formatTime(run.CompletedAt()),
		FailedAt:       formatTime(run.FailedAt()),
		CreatedAt:      run.CreatedAt().Format(time.RFC3339),
		UpdatedAt:      run.UpdatedAt().Format(time.RFC3339),
		ScheduleType:   string(run.ScheduleType()),
		RecurrenceRule: run.RecurrenceRule(),
		LocalTime:      run.LocalTime(),
		TimezoneID:     run.TimezoneID(),
		NextRunAtUTC:   run.ScheduledAt().Format(time.RFC3339),
		ParentJobID:    run.ParentJobID(),
		ChildJobIDs:    run.ChildJobIDs(),
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func eventToHTTPResponse(event *RunEvent) RunEventResponse {
	return RunEventResponse{
		ID:        event.ID(),
		TenantID:  event.TenantID(),
		JobID:     event.JobID(),
		JobRunID:  event.JobRunID(),
		Status:    string(event.Status()),
		EventType: string(event.EventType()),
		Payload:   event.Payload(),
		CreatedAt: event.CreatedAt().Format(time.RFC3339),
	}
}
