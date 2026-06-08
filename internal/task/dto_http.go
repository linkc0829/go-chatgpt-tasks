package task

import (
	"time"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

type CreateJobRequest struct {
	Description              string `json:"description" binding:"required,min=1"`
	ScheduledAt              string `json:"scheduled_at" binding:"required"`
	RecurringIntervalSeconds int64  `json:"recurring_interval_seconds,omitempty" binding:"omitempty,min=1"`
}

func (r CreateJobRequest) toInput() (CreateInput, error) {
	scheduledAt, err := time.Parse(time.RFC3339, r.ScheduledAt)
	if err != nil {
		return CreateInput{}, ErrInvalidSchedule
	}
	return CreateInput{
		Description: r.Description,
		ScheduledAt: scheduledAt,
		Interval:    time.Duration(r.RecurringIntervalSeconds) * time.Second,
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
	ID          shared.JobRunID `json:"id"`
	TenantID    shared.TenantID `json:"tenant_id"`
	JobID       shared.JobID    `json:"job_id"`
	Status      string          `json:"status"`
	ScheduledAt string          `json:"scheduled_at"`
	Sequence    int             `json:"sequence"`
	Attempts    int             `json:"attempts"`
	CreatedAt   string          `json:"created_at"`
	UpdatedAt   string          `json:"updated_at"`
}

type ListRunsResponse struct {
	Runs   []RunResponse `json:"runs"`
	Total  int64         `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

func runToHTTPResponse(run *JobRun) RunResponse {
	return RunResponse{
		ID:          run.ID(),
		TenantID:    run.TenantID(),
		JobID:       run.JobID(),
		Status:      string(run.Status()),
		ScheduledAt: run.ScheduledAt().Format(time.RFC3339),
		Sequence:    run.Sequence(),
		Attempts:    run.Attempts(),
		CreatedAt:   run.CreatedAt().Format(time.RFC3339),
		UpdatedAt:   run.UpdatedAt().Format(time.RFC3339),
	}
}
