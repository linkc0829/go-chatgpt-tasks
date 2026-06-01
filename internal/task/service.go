package task

import (
	"context"
	"fmt"
	"time"

	"github.com/linkc0829/go-backend-template/internal/shared"
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

func (s *Service) Create(ctx context.Context, in CreateInput) (*JobRun, error) {
	kind := KindOneOff
	if in.Interval > 0 {
		kind = KindRecurring
	}

	j, err := NewJob(kind, in.Description, in.Interval)
	if err != nil {
		return nil, err
	}
	if err := s.repo.SaveJob(ctx, j); err != nil {
		return nil, fmt.Errorf("save job: %w", err)
	}

	run, err := NewJobRun(j.ID(), 1, in.ScheduledAt)
	if err != nil {
		return nil, err
	}
	if err := s.repo.SaveRun(ctx, run); err != nil {
		return nil, fmt.Errorf("save run: %w", err)
	}
	return run, nil
}

func (s *Service) List(ctx context.Context, p shared.Pagination) ([]*JobRun, int64, error) {
	runs, total, err := s.repo.ListRuns(ctx, p)
	if err != nil {
		return nil, 0, fmt.Errorf("list runs: %w", err)
	}
	return runs, total, nil
}

func (s *Service) Status(ctx context.Context, id shared.JobRunID) (*JobRun, error) {
	return s.repo.FindRunByID(ctx, id)
}

func (s *Service) Cancel(ctx context.Context, id shared.JobRunID) (*JobRun, error) {
	run, err := s.repo.FindRunByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := run.Cancel(); err != nil {
		return nil, err
	}
	if err := s.repo.UpdateRunStatus(ctx, run); err != nil {
		return nil, fmt.Errorf("update run: %w", err)
	}
	_ = s.repo.AppendEvent(ctx, NewRunEvent(run.ID(), StatusCancelled))
	return run, nil
}
