package task

import (
	"context"
	"fmt"
	"time"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

type Service struct {
	repo            Repo
	quota           QuotaRepo
	quotaRejections QuotaRejectionRecorder
}

func NewService(repo Repo, quota QuotaRepo, recorders ...QuotaRejectionRecorder) *Service {
	s := &Service{repo: repo, quota: quota}
	if len(recorders) > 0 {
		s.quotaRejections = recorders[0]
	}
	return s
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
	if err := s.checkQuota(ctx, id.TenantID, kind); err != nil {
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
	_ = s.repo.AppendEvent(ctx, NewRunEvent(run.TenantID(), run.JobID(), run.ID(), StatusCancelled, EventJobCancelled, nil))
	return run, nil
}

func (s *Service) RunsForJob(ctx context.Context, id Identity, jobID shared.JobID, p shared.Pagination) ([]*JobRun, int64, error) {
	if !id.valid() {
		return nil, 0, ErrInvalidOwner
	}

	runs, total, err := s.repo.ListRunsByJob(ctx, id.TenantID, jobID, p)
	if err != nil {
		return nil, 0, fmt.Errorf("list runs for job: %w", err)
	}
	return runs, total, nil
}

func (s *Service) EventsForRun(ctx context.Context, id Identity, runID shared.JobRunID) ([]*RunEvent, error) {
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
	events, err := s.repo.ListEvents(ctx, id.TenantID, runID)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	return events, nil
}

func (s *Service) checkQuota(ctx context.Context, tenantID shared.TenantID, kind Kind) error {
	if s.quota == nil {
		return nil
	}

	quota, err := s.quota.Get(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant quota: %w", err)
	}
	jobs, err := s.quota.CountJobsSince(ctx, tenantID, time.Now().UTC().Add(-time.Hour))
	if err != nil {
		return fmt.Errorf("count jobs for quota: %w", err)
	}
	if jobs >= int64(quota.MaxJobsPerHour) {
		return s.rejectQuota(tenantID, "max_jobs_per_hour")
	}
	if kind != KindRecurring {
		return nil
	}

	active, err := s.quota.CountActiveRecurring(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("count active recurring jobs for quota: %w", err)
	}
	if active >= int64(quota.MaxActiveRecurring) {
		return s.rejectQuota(tenantID, "max_active_recurring_jobs")
	}
	return nil
}

func (s *Service) rejectQuota(tenantID shared.TenantID, reason string) error {
	if s.quotaRejections != nil {
		s.quotaRejections.RecordQuotaRejection(tenantID, reason)
	}
	return fmt.Errorf("%w: %s", ErrQuotaExceeded, reason)
}
