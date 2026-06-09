package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
	taskdomain "github.com/linkc0829/go-chatgpt-tasks/internal/task"
)

type ToolService interface {
	Create(ctx context.Context, id taskdomain.Identity, in taskdomain.CreateInput) (*taskdomain.JobRun, error)
	List(ctx context.Context, id taskdomain.Identity, p shared.Pagination) ([]*taskdomain.JobRun, int64, error)
	Status(ctx context.Context, id taskdomain.Identity, jobID shared.JobID) (*taskdomain.JobRun, error)
	Cancel(ctx context.Context, id taskdomain.Identity, jobID shared.JobID) ([]*taskdomain.JobRun, error)
	RunsForJob(ctx context.Context, id taskdomain.Identity, jobID shared.JobID, p shared.Pagination) ([]*taskdomain.JobRun, int64, error)
	EventsForRun(ctx context.Context, id taskdomain.Identity, runID shared.JobRunID) ([]*taskdomain.RunEvent, error)
}

// CreateArgs is the argument schema for task.create. The jsonschema tags are
// the single source of truth for the MCP tool's advertised input schema (read
// reflectively by the MCP SDK in cmd/mcp); do not duplicate these fields in a
// transport-layer struct.
type CreateArgs struct {
	Description             string             `json:"description" jsonschema:"Task description"`
	ScheduledAt             string             `json:"scheduled_at,omitempty" jsonschema:"RFC3339 scheduled time for one-off jobs"`
	RecurringIntervalSecond int64              `json:"recurring_interval_seconds,omitempty" jsonschema:"Optional recurring interval in seconds"`
	ScheduleType            taskdomain.Kind    `json:"schedule_type,omitempty" jsonschema:"one_off or recurring"`
	RecurrenceRule          string             `json:"recurrence_rule,omitempty" jsonschema:"FREQ=DAILY or FREQ=WEEKLY with optional INTERVAL"`
	LocalTime               string             `json:"local_time,omitempty" jsonschema:"Local wall-clock time in HH:MM format"`
	TimezoneID              string             `json:"timezone_id,omitempty" jsonschema:"IANA timezone identifier"`
	OriginalUserText        string             `json:"original_user_text,omitempty" jsonschema:"Original scheduling request text"`
	SideEffecting           bool               `json:"side_effecting,omitempty" jsonschema:"True if the task performs external side effects; enables idempotent execution so retries and duplicate runs are not re-applied"`
	IdempotencyScope        string             `json:"idempotency_scope,omitempty" jsonschema:"Idempotency granularity, defaults to job_run"`
	ParentJobID             string             `json:"parent_job_id,omitempty" jsonschema:"Job ID of a parent task; this task runs only after the parent reaches trigger_on_parent_status (linear job chain)"`
	TriggerOnParentStatus   taskdomain.Status  `json:"trigger_on_parent_status,omitempty" jsonschema:"Parent terminal status that triggers this task: success, failed, or cancelled. Required when parent_job_id is set"`
	JobType                 taskdomain.JobType `json:"job_type,omitempty" jsonschema:"Task handler type, defaults to generic_llm"`
}

// ListArgs is the argument schema for task.list.
type ListArgs struct {
	Limit  int `json:"limit,omitempty" jsonschema:"Page size, default 20"`
	Offset int `json:"offset,omitempty" jsonschema:"Page offset, default 0"`
}

// RunRef is the argument schema for tools that reference a single run/job by id
// (task.status, task.cancel, task.events).
type RunRef struct {
	JobID string `json:"job_id" jsonschema:"Job run ID returned by task.create or task.list"`
}

// JobRef is the argument schema for task.runs: a job id plus pagination.
type JobRef struct {
	JobID  string `json:"job_id" jsonschema:"Job ID returned as job_id by task.create or task.list"`
	Limit  int    `json:"limit,omitempty" jsonschema:"Page size, default 20"`
	Offset int    `json:"offset,omitempty" jsonschema:"Page offset, default 0"`
}

type runResponse struct {
	JobID          string   `json:"job_id"`
	Status         string   `json:"status"`
	ScheduledAt    string   `json:"scheduled_at"`
	Sequence       int      `json:"sequence,omitempty"`
	ScheduleType   string   `json:"schedule_type,omitempty"`
	RecurrenceRule string   `json:"recurrence_rule,omitempty"`
	LocalTime      string   `json:"local_time,omitempty"`
	TimezoneID     string   `json:"timezone_id,omitempty"`
	NextRunAtUTC   string   `json:"next_run_at_utc,omitempty"`
	ParentJobID    string   `json:"parent_job_id,omitempty"`
	ChildJobIDs    []string `json:"children,omitempty"`
}

type listResponse struct {
	Runs   []runResponse `json:"runs"`
	Total  int64         `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

type eventResponse struct {
	JobID     string         `json:"job_id"`
	JobRunID  string         `json:"job_run_id"`
	Status    string         `json:"status"`
	EventType string         `json:"event_type"`
	Payload   map[string]any `json:"payload"`
	CreatedAt string         `json:"created_at"`
}

type eventsResponse struct {
	Events []eventResponse `json:"events"`
}

func Register(reg *Registry, svc ToolService, ident taskdomain.Identity) {
	reg.Register("task.create", func(ctx context.Context, raw json.RawMessage) (any, error) {
		var args CreateArgs
		if err := decodeArgs(raw, &args); err != nil {
			return nil, err
		}
		var scheduledAt time.Time
		if args.ScheduledAt != "" {
			var err error
			scheduledAt, err = time.Parse(time.RFC3339, args.ScheduledAt)
			if err != nil {
				return nil, fmt.Errorf("parse scheduled_at: %w", err)
			}
		}
		var parentJobID *shared.JobID
		if args.ParentJobID != "" {
			id, err := shared.ParseJobID(args.ParentJobID)
			if err != nil {
				return nil, fmt.Errorf("parse parent_job_id: %w", err)
			}
			parentJobID = &id
		}

		run, err := svc.Create(ctx, ident, taskdomain.CreateInput{
			Description:           args.Description,
			ScheduledAt:           scheduledAt,
			Interval:              time.Duration(args.RecurringIntervalSecond) * time.Second,
			ScheduleType:          args.ScheduleType,
			RecurrenceRule:        args.RecurrenceRule,
			LocalTime:             args.LocalTime,
			TimezoneID:            args.TimezoneID,
			OriginalUserText:      args.OriginalUserText,
			SideEffecting:         args.SideEffecting,
			IdempotencyScope:      args.IdempotencyScope,
			ParentJobID:           parentJobID,
			TriggerOnParentStatus: args.TriggerOnParentStatus,
			JobType:               args.JobType,
		})
		if err != nil {
			return nil, err
		}
		return runToResponse(run), nil
	})

	reg.Register("task.list", func(ctx context.Context, raw json.RawMessage) (any, error) {
		var args ListArgs
		if err := decodeArgs(raw, &args); err != nil {
			return nil, err
		}
		p := shared.NewPagination(args.Limit, args.Offset)
		runs, total, err := svc.List(ctx, ident, p)
		if err != nil {
			return nil, err
		}

		out := make([]runResponse, 0, len(runs))
		for _, run := range runs {
			out = append(out, runToResponse(run))
		}
		return listResponse{Runs: out, Total: total, Limit: p.Limit, Offset: p.Offset}, nil
	})

	reg.Register("task.status", func(ctx context.Context, raw json.RawMessage) (any, error) {
		var args JobRef
		if err := decodeArgs(raw, &args); err != nil {
			return nil, err
		}
		id, err := shared.ParseJobID(args.JobID)
		if err != nil {
			return nil, err
		}
		run, err := svc.Status(ctx, ident, id)
		if err != nil {
			return nil, err
		}
		return runToResponse(run), nil
	})

	reg.Register("task.cancel", func(ctx context.Context, raw json.RawMessage) (any, error) {
		var args struct {
			JobID string `json:"job_id"`
		}
		if err := decodeArgs(raw, &args); err != nil {
			return nil, err
		}
		id, err := shared.ParseJobID(args.JobID)
		if err != nil {
			return nil, err
		}
		runs, err := svc.Cancel(ctx, ident, id)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"job_id":                  id.String(),
			"cancelled_runs":          len(runs),
			"running_runs_unaffected": true,
		}, nil
	})

	reg.Register("task.runs", func(ctx context.Context, raw json.RawMessage) (any, error) {
		var ref JobRef
		if err := decodeArgs(raw, &ref); err != nil {
			return nil, err
		}
		jobID, err := shared.ParseJobID(ref.JobID)
		if err != nil {
			return nil, fmt.Errorf("parse job_id: %w", err)
		}
		p := shared.NewPagination(ref.Limit, ref.Offset)
		runs, total, err := svc.RunsForJob(ctx, ident, jobID, p)
		if err != nil {
			return nil, err
		}
		out := make([]runResponse, 0, len(runs))
		for _, run := range runs {
			out = append(out, runToResponse(run))
		}
		return listResponse{Runs: out, Total: total, Limit: p.Limit, Offset: p.Offset}, nil
	})

	reg.Register("task.events", func(ctx context.Context, raw json.RawMessage) (any, error) {
		id, err := runIDFromArgs(raw)
		if err != nil {
			return nil, err
		}
		events, err := svc.EventsForRun(ctx, ident, id)
		if err != nil {
			return nil, err
		}
		out := make([]eventResponse, 0, len(events))
		for _, event := range events {
			out = append(out, eventToResponse(event))
		}
		return eventsResponse{Events: out}, nil
	})
}

func decodeArgs(raw json.RawMessage, v any) error {
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("decode args: %w", err)
	}
	return nil
}

func runIDFromArgs(raw json.RawMessage) (shared.JobRunID, error) {
	var ref RunRef
	if err := decodeArgs(raw, &ref); err != nil {
		return shared.JobRunID{}, err
	}
	id, err := shared.ParseJobRunID(ref.JobID)
	if err != nil {
		return shared.JobRunID{}, fmt.Errorf("parse job_id: %w", err)
	}
	return id, nil
}

func runToResponse(run *taskdomain.JobRun) runResponse {
	out := runResponse{
		JobID:          run.ID().String(),
		Status:         string(run.Status()),
		ScheduledAt:    run.ScheduledAt().Format(time.RFC3339),
		Sequence:       run.Sequence(),
		ScheduleType:   string(run.ScheduleType()),
		RecurrenceRule: run.RecurrenceRule(),
		LocalTime:      run.LocalTime(),
		TimezoneID:     run.TimezoneID(),
		NextRunAtUTC:   run.ScheduledAt().Format(time.RFC3339),
	}
	if parentID := run.ParentJobID(); parentID != nil {
		out.ParentJobID = parentID.String()
	}
	for _, childID := range run.ChildJobIDs() {
		out.ChildJobIDs = append(out.ChildJobIDs, childID.String())
	}
	return out
}

func eventToResponse(event *taskdomain.RunEvent) eventResponse {
	return eventResponse{
		JobID:     event.JobID().String(),
		JobRunID:  event.JobRunID().String(),
		Status:    string(event.Status()),
		EventType: string(event.EventType()),
		Payload:   event.Payload(),
		CreatedAt: event.CreatedAt().Format(time.RFC3339),
	}
}
