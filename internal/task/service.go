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
	Description      string
	ScheduledAt      time.Time
	Interval         time.Duration
	ScheduleType     Kind
	RecurrenceRule   string
	LocalTime        string
	TimezoneID       string
	OriginalUserText string
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

	schedule, scheduledAt, err := scheduleFromInput(in)
	if err != nil {
		return nil, err
	}

	j, err := NewJob(id.TenantID, id.UserID, in.Description, schedule)
	if err != nil {
		return nil, err
	}
	if err := s.checkQuota(ctx, id.TenantID, schedule.Type); err != nil {
		return nil, err
	}
	if err := s.repo.SaveJob(ctx, j); err != nil {
		return nil, fmt.Errorf("save job: %w", err)
	}

	run, err := NewJobRun(id.TenantID, j.ID(), 1, scheduledAt)
	if err != nil {
		return nil, err
	}
	if err := s.repo.SaveRun(ctx, run); err != nil {
		return nil, fmt.Errorf("save run: %w", err)
	}
	run.setSchedule(j)
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
	job, err := s.repo.FindJob(ctx, run.JobID())
	if err != nil {
		return nil, fmt.Errorf("find job schedule: %w", err)
	}
	run.setSchedule(job)
	children, err := s.findAllChildren(ctx, run.JobID())
	if err != nil {
		return nil, err
	}
	run.setChildren(children)
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
	if err := s.cancelPendingChildren(ctx, run); err != nil {
		return nil, err
	}
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

func scheduleFromInput(in CreateInput) (ScheduleSpec, time.Time, error) {
	kind := in.ScheduleType
	if kind == "" {
		kind = KindOneOff
		if in.Interval > 0 || in.RecurrenceRule != "" {
			kind = KindRecurring
		}
	}
	timezoneID := in.TimezoneID
	if timezoneID == "" {
		timezoneID = "UTC"
	}

	spec := ScheduleSpec{
		Type:             kind,
		ScheduledAtUTC:   in.ScheduledAt,
		RecurrenceRule:   in.RecurrenceRule,
		LocalTime:        in.LocalTime,
		TimezoneID:       timezoneID,
		OriginalUserText: in.OriginalUserText,
		LegacyInterval:   in.Interval,
	}
	if kind == KindOneOff {
		if in.ScheduledAt.IsZero() {
			return ScheduleSpec{}, time.Time{}, ErrInvalidSchedule
		}
		return spec, in.ScheduledAt.UTC(), nil
	}
	if spec.RecurrenceRule == "" {
		spec.RecurrenceRule = "FREQ=DAILY"
	}
	if spec.LocalTime == "" && !in.ScheduledAt.IsZero() {
		spec.LocalTime = in.ScheduledAt.In(time.UTC).Format("15:04")
	}
	rule, err := ParseRule(spec.RecurrenceRule)
	if err != nil {
		return ScheduleSpec{}, time.Time{}, err
	}
	tz, err := time.LoadLocation(spec.TimezoneID)
	if err != nil {
		return ScheduleSpec{}, time.Time{}, ErrInvalidTimezone
	}
	next, _, err := NextOccurrence(rule, spec.LocalTime, tz, time.Now().UTC())
	if err != nil {
		return ScheduleSpec{}, time.Time{}, err
	}
	spec.ScheduledAtUTC = next
	return spec, next, nil
}

func (s *Service) cancelPendingChildren(ctx context.Context, parent *JobRun) error {
	children, err := s.findAllChildren(ctx, parent.JobID())
	if err != nil {
		return err
	}
	for _, child := range children {
		runs, _, err := s.repo.ListRunsByJob(ctx, parent.TenantID(), child.ID(), shared.NewPagination(100, 0))
		if err != nil {
			return fmt.Errorf("list child runs for cancellation: %w", err)
		}
		for _, childRun := range runs {
			if childRun.Status() != StatusPending && childRun.Status() != StatusQueued {
				continue
			}
			if err := childRun.Cancel(); err != nil {
				return err
			}
			if err := s.repo.UpdateRunStatus(ctx, childRun); err != nil {
				return fmt.Errorf("cancel child run: %w", err)
			}
		}
	}
	return nil
}

func (s *Service) findAllChildren(ctx context.Context, parentJobID shared.JobID) ([]*Job, error) {
	seen := make(map[shared.JobID]struct{})
	var out []*Job
	for _, trigger := range []Status{StatusSuccess, StatusFailed, StatusCancelled} {
		children, err := s.repo.FindChildren(ctx, parentJobID, trigger)
		if err != nil {
			return nil, fmt.Errorf("find child jobs: %w", err)
		}
		for _, child := range children {
			if _, ok := seen[child.ID()]; ok {
				continue
			}
			seen[child.ID()] = struct{}{}
			out = append(out, child)
		}
	}
	return out, nil
}
