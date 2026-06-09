package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

type fakeRepo struct {
	saveJobCalls      int
	saveRunCalls      int
	updateRunCalls    int
	appendEventCalls  int
	saveJobErr        error
	saveRunErr        error
	updateRunErr      error
	findRunErr        error
	findRun           *JobRun
	listRuns          []*JobRun
	listTotal         int64
	listErr           error
	findDueRuns       []*JobRun
	findDueErr        error
	findJob           *Job
	findJobErr        error
	insertRunIfAbsent bool
	insertRunErr      error
	terminalRuns      []NextRunSpec
	terminalRunsErr   error
	lastSavedJob      *Job
	lastSavedRun      *JobRun
	lastUpdatedRun    *JobRun
	lastAppendedEvent *RunEvent
}

func (f *fakeRepo) SaveJob(_ context.Context, j *Job) error {
	f.saveJobCalls++
	f.lastSavedJob = j
	return f.saveJobErr
}

func (f *fakeRepo) SaveRun(_ context.Context, r *JobRun) error {
	f.saveRunCalls++
	f.lastSavedRun = r
	return f.saveRunErr
}

func (f *fakeRepo) UpdateRunStatus(_ context.Context, r *JobRun) error {
	f.updateRunCalls++
	f.lastUpdatedRun = r
	return f.updateRunErr
}

func (f *fakeRepo) FindRunByID(_ context.Context, _ shared.JobRunID) (*JobRun, error) {
	return f.findRun, f.findRunErr
}

func (f *fakeRepo) ListRuns(_ context.Context, _ shared.TenantID, _ shared.Pagination) ([]*JobRun, int64, error) {
	return f.listRuns, f.listTotal, f.listErr
}

func (f *fakeRepo) ListRunsByJob(_ context.Context, _ shared.TenantID, _ shared.JobID, _ shared.Pagination) ([]*JobRun, int64, error) {
	return f.listRuns, f.listTotal, f.listErr
}

func (f *fakeRepo) ListEvents(_ context.Context, _ shared.TenantID, _ shared.JobRunID) ([]*RunEvent, error) {
	if f.lastAppendedEvent == nil {
		return nil, nil
	}
	return []*RunEvent{f.lastAppendedEvent}, nil
}

func (f *fakeRepo) AppendEvent(_ context.Context, e *RunEvent) error {
	f.appendEventCalls++
	f.lastAppendedEvent = e
	return nil
}

func (f *fakeRepo) FindDueRuns(_ context.Context, _ int64, _ time.Time, _ int32) ([]*JobRun, error) {
	return f.findDueRuns, f.findDueErr
}

func (f *fakeRepo) FindJob(_ context.Context, _ shared.JobID) (*Job, error) {
	return f.findJob, f.findJobErr
}

func (f *fakeRepo) InsertRunIfAbsent(_ context.Context, _ *JobRun) (bool, error) {
	return f.insertRunIfAbsent, f.insertRunErr
}

func (f *fakeRepo) FindTerminalRecurringRuns(_ context.Context, _ time.Time, _ int32) ([]NextRunSpec, error) {
	return f.terminalRuns, f.terminalRunsErr
}

func TestService_Create(t *testing.T) {
	ident := testIdentity()
	scheduledAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("create_one_off_pending_run", func(t *testing.T) {
		repo := &fakeRepo{}
		svc := NewService(repo, nil)

		run, err := svc.Create(context.Background(), ident, CreateInput{
			Description: "Summarize tech news",
			ScheduledAt: scheduledAt,
		})

		require.NoError(t, err)
		assert.Equal(t, 1, repo.saveJobCalls)
		assert.Equal(t, 1, repo.saveRunCalls)
		assert.Equal(t, KindOneOff, repo.lastSavedJob.Kind())
		assert.Equal(t, ident.TenantID, repo.lastSavedJob.TenantID())
		assert.Equal(t, ident.UserID, repo.lastSavedJob.UserID())
		assert.Equal(t, ident.TenantID, run.TenantID())
		assert.Equal(t, StatusPending, run.Status())
		assert.Equal(t, scheduledAt, run.ScheduledAt())
	})

	t.Run("create_recurring_job", func(t *testing.T) {
		repo := &fakeRepo{}
		svc := NewService(repo, nil)

		_, err := svc.Create(context.Background(), ident, CreateInput{
			Description: "Summarize tech news",
			ScheduledAt: scheduledAt,
			Interval:    5 * time.Second,
		})

		require.NoError(t, err)
		assert.Equal(t, KindRecurring, repo.lastSavedJob.Kind())
		assert.Equal(t, 5*time.Second, repo.lastSavedJob.Interval())
	})

	t.Run("invalid_description", func(t *testing.T) {
		repo := &fakeRepo{}
		svc := NewService(repo, nil)

		_, err := svc.Create(context.Background(), ident, CreateInput{
			Description: "",
			ScheduledAt: scheduledAt,
		})

		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidDescription), "Create() error = %v, want %v", err, ErrInvalidDescription)
		assert.Equal(t, 0, repo.saveJobCalls)
	})
}

func TestService_TenantIsolation(t *testing.T) {
	tenantA := testIdentity()
	tenantB := Identity{TenantID: shared.NewTenantID(), UserID: shared.NewUserID()}
	scheduledAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("list_is_scoped_by_tenant", func(t *testing.T) {
		repo := &fakeRepo{listRuns: []*JobRun{}, listTotal: 0}
		svc := NewService(repo, nil)

		runs, total, err := svc.List(context.Background(), tenantB, shared.NewPagination(20, 0))

		require.NoError(t, err)
		assert.Empty(t, runs)
		assert.Equal(t, int64(0), total)
	})

	t.Run("status_hides_cross_tenant_run", func(t *testing.T) {
		run, err := NewJobRun(tenantA.TenantID, shared.NewJobID(), 1, scheduledAt)
		require.NoError(t, err)
		repo := &fakeRepo{findRun: run}
		svc := NewService(repo, nil)

		_, err = svc.Status(context.Background(), tenantB, run.ID())

		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrJobRunNotFound), "Status() error = %v, want %v", err, ErrJobRunNotFound)
	})
}

func TestService_Cancel(t *testing.T) {
	t.Run("cancel_pending_run", func(t *testing.T) {
		run := newTestRun(t)
		repo := &fakeRepo{findRun: run}
		svc := NewService(repo, nil)

		got, err := svc.Cancel(context.Background(), identityForRun(run), run.ID())

		require.NoError(t, err)
		assert.Equal(t, StatusCancelled, got.Status())
		assert.Equal(t, 1, repo.updateRunCalls)
		assert.Equal(t, 1, repo.appendEventCalls)
		assert.Equal(t, StatusCancelled, repo.lastAppendedEvent.Status())
		assert.Equal(t, EventJobCancelled, repo.lastAppendedEvent.EventType())
	})

	t.Run("cancel_terminal_rejected", func(t *testing.T) {
		run := newTestRun(t)
		require.NoError(t, run.MarkQueued())
		require.NoError(t, run.MarkRunning())
		require.NoError(t, run.MarkSuccess())
		repo := &fakeRepo{findRun: run}
		svc := NewService(repo, nil)

		_, err := svc.Cancel(context.Background(), identityForRun(run), run.ID())

		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidStatusTransition), "Cancel() error = %v, want %v", err, ErrInvalidStatusTransition)
		assert.Equal(t, 0, repo.updateRunCalls)
	})

	t.Run("status_not_found", func(t *testing.T) {
		repo := &fakeRepo{findRunErr: ErrJobRunNotFound}
		svc := NewService(repo, nil)

		_, err := svc.Status(context.Background(), testIdentity(), shared.NewJobRunID())

		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrJobRunNotFound), "Status() error = %v, want %v", err, ErrJobRunNotFound)
	})
}

func newTestRun(t *testing.T) *JobRun {
	t.Helper()

	run, err := NewJobRun(
		testIdentity().TenantID,
		shared.NewJobID(),
		1,
		time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err)
	return run
}

func testIdentity() Identity {
	return Identity{TenantID: shared.NewTenantID(), UserID: shared.NewUserID()}
}

func identityForRun(run *JobRun) Identity {
	return Identity{TenantID: run.TenantID(), UserID: shared.NewUserID()}
}
