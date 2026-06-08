package task

import (
	"context"
	"fmt"
	"time"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

type Service struct {
	repo Repo
}

func NewService(repo Repo) *Service {
	return &Service{repo: repo}
}

type CreateInput struct {
	Description string
	ScheduledAt time.Time
	Interval    time.Duration
}

type Identity struct {
	TenantID shared.TenantID
	UserID   shared.UserID
}

func (id Identity) valid() bool {
	return !id.TenantID.IsZero() && !id.UserID.IsZero()
}

func (s *Service) Create(ctx context.Context, id Identity, in CreateInput) (*JobRun, error) {
	if !id.valid() {
		return nil, ErrInvalidOwner
	}

	kind := KindOneOff
	if in.Interval > 0 {
		kind = KindRecurring
	}

	j, err := NewJob(id.TenantID, id.UserID, kind, in.Description, in.Interval)
	if err != nil {
		return nil, err
	}
	if err := s.repo.SaveJob(ctx, j); err != nil {
		return nil, fmt.Errorf("save job: %w", err)
	}

	run, err := NewJobRun(id.TenantID, j.ID(), 1, in.ScheduledAt)
	if err != nil {
		return nil, err
	}
	if err := s.repo.SaveRun(ctx, run); err != nil {
		return nil, fmt.Errorf("save run: %w", err)
	}
	return run, nil
}

func (s *Service) List(ctx context.Context, id Identity, p shared.Pagination) ([]*JobRun, int64, error) {
	if !id.valid() {
		return nil, 0, ErrInvalidOwner
	}

	runs, total, err := s.repo.ListRuns(ctx, id.TenantID, p)
	if err != nil {
		return nil, 0, fmt.Errorf("list runs: %w", err)
	}
	return runs, total, nil
}

func (s *Service) Status(ctx context.Context, id Identity, runID shared.JobRunID) (*JobRun, error) {
	if !id.valid() {
		return nil, ErrInvalidOwner
	}

	run, err := s.repo.FindRunByID(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run.TenantID() != id.TenantID {
		return nil, ErrJobRunNotFound
	}
	return run, nil
}

func (s *Service) Cancel(ctx context.Context, id Identity, runID shared.JobRunID) (*JobRun, error) {
	if !id.valid() {
		return nil, ErrInvalidOwner
	}

	run, err := s.repo.FindRunByID(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run.TenantID() != id.TenantID {
		return nil, ErrJobRunNotFound
	}
	if err := run.Cancel(); err != nil {
		return nil, err
	}
	if err := s.repo.UpdateRunStatus(ctx, run); err != nil {
		return nil, fmt.Errorf("update run: %w", err)
	}
	_ = s.repo.AppendEvent(ctx, NewRunEvent(run.TenantID(), run.JobID(), run.ID(), StatusCancelled))
	return run, nil
}
