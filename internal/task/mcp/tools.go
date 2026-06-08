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
	Status(ctx context.Context, id taskdomain.Identity, runID shared.JobRunID) (*taskdomain.JobRun, error)
	Cancel(ctx context.Context, id taskdomain.Identity, runID shared.JobRunID) (*taskdomain.JobRun, error)
}

type createArgs struct {
	Description             string `json:"description"`
	ScheduledAt             string `json:"scheduled_at"`
	RecurringIntervalSecond int64  `json:"recurring_interval_seconds,omitempty"`
}

type listArgs struct {
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`
}

type runRef struct {
	JobID string `json:"job_id"`
}

type runResponse struct {
	JobID       string `json:"job_id"`
	Status      string `json:"status"`
	ScheduledAt string `json:"scheduled_at"`
	Sequence    int    `json:"sequence,omitempty"`
}

type listResponse struct {
	Runs   []runResponse `json:"runs"`
	Total  int64         `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

func Register(reg *Registry, svc ToolService, ident taskdomain.Identity) {
	reg.Register("task.create", func(ctx context.Context, raw json.RawMessage) (any, error) {
		var args createArgs
		if err := decodeArgs(raw, &args); err != nil {
			return nil, err
		}
		scheduledAt, err := time.Parse(time.RFC3339, args.ScheduledAt)
		if err != nil {
			return nil, fmt.Errorf("parse scheduled_at: %w", err)
		}

		run, err := svc.Create(ctx, ident, taskdomain.CreateInput{
			Description: args.Description,
			ScheduledAt: scheduledAt,
			Interval:    time.Duration(args.RecurringIntervalSecond) * time.Second,
		})
		if err != nil {
			return nil, err
		}
		return runToResponse(run), nil
	})

	reg.Register("task.list", func(ctx context.Context, raw json.RawMessage) (any, error) {
		var args listArgs
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
		id, err := runIDFromArgs(raw)
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
		id, err := runIDFromArgs(raw)
		if err != nil {
			return nil, err
		}
		run, err := svc.Cancel(ctx, ident, id)
		if err != nil {
			return nil, err
		}
		return runToResponse(run), nil
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
	var ref runRef
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
	return runResponse{
		JobID:       run.ID().String(),
		Status:      string(run.Status()),
		ScheduledAt: run.ScheduledAt().Format(time.RFC3339),
		Sequence:    run.Sequence(),
	}
}
