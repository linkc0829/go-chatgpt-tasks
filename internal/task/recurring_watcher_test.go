package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
	"go.uber.org/zap"
)

type recurringWatcherRepo struct {
	specs       []NextRunSpec
	findErr     error
	inserted    []*JobRun
	insertedErr error
	created     bool
}

func (r *recurringWatcherRepo) SaveJob(context.Context, *Job) error    { return nil }
func (r *recurringWatcherRepo) SaveRun(context.Context, *JobRun) error { return nil }
func (r *recurringWatcherRepo) UpdateRunStatus(context.Context, *JobRun) error {
	return nil
}
func (r *recurringWatcherRepo) FindRunByID(context.Context, shared.JobRunID) (*JobRun, error) {
	return nil, ErrJobRunNotFound
}
func (r *recurringWatcherRepo) ListRuns(context.Context, shared.TenantID, shared.Pagination) ([]*JobRun, int64, error) {
	return nil, 0, nil
}
func (r *recurringWatcherRepo) AppendEvent(context.Context, *RunEvent) error { return nil }
func (r *recurringWatcherRepo) FindDueRuns(context.Context, int64, time.Time, int32) ([]*JobRun, error) {
	return nil, nil
}
func (r *recurringWatcherRepo) FindJob(context.Context, shared.JobID) (*Job, error) {
	return nil, ErrJobNotFound
}
func (r *recurringWatcherRepo) InsertRunIfAbsent(_ context.Context, run *JobRun) (bool, error) {
	if r.insertedErr != nil {
		return false, r.insertedErr
	}
	r.inserted = append(r.inserted, run)
	return r.created, nil
}
func (r *recurringWatcherRepo) FindTerminalRecurringRuns(
	context.Context,
	time.Time,
	int32,
) ([]NextRunSpec, error) {
	return r.specs, r.findErr
}

func TestRecurringWatcher_scanOnceCreatesNextRun(t *testing.T) {
	scheduledAt := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	spec := NextRunSpec{
		TenantID:    shared.NewTenantID(),
		JobID:       shared.NewJobID(),
		Sequence:    1,
		ScheduledAt: scheduledAt,
		Interval:    5 * time.Second,
	}
	repo := &recurringWatcherRepo{specs: []NextRunSpec{spec}, created: true}
	watcher := NewRecurringWatcher(repo, time.Hour, zap.NewNop())

	watcher.scanOnce(context.Background())

	if got := len(repo.inserted); got != 1 {
		t.Fatalf("RecurringWatcher.scanOnce() inserted %d runs, want 1", got)
	}
	run := repo.inserted[0]
	if got, want := run.JobID(), spec.JobID; got != want {
		t.Errorf("RecurringWatcher.scanOnce() job_id = %s, want %s", got, want)
	}
	if got, want := run.Sequence(), 2; got != want {
		t.Errorf("RecurringWatcher.scanOnce() sequence = %d, want %d", got, want)
	}
	if got, want := run.ScheduledAt(), scheduledAt.Add(5*time.Second); !got.Equal(want) {
		t.Errorf("RecurringWatcher.scanOnce() scheduled_at = %s, want %s", got, want)
	}
	if got, want := run.Status(), StatusPending; got != want {
		t.Errorf("RecurringWatcher.scanOnce() status = %q, want %q", got, want)
	}
}

func TestRecurringWatcher_scanOnceConflictIsNoop(t *testing.T) {
	spec := NextRunSpec{
		TenantID:    shared.NewTenantID(),
		JobID:       shared.NewJobID(),
		Sequence:    1,
		ScheduledAt: time.Now().UTC(),
		Interval:    5 * time.Second,
	}
	repo := &recurringWatcherRepo{specs: []NextRunSpec{spec}, created: false}
	watcher := NewRecurringWatcher(repo, time.Hour, zap.NewNop())

	watcher.scanOnce(context.Background())

	if got := len(repo.inserted); got != 1 {
		t.Errorf("RecurringWatcher.scanOnce(conflict) inserted attempts = %d, want 1", got)
	}
}

func TestRecurringWatcher_scanOnceFindErrorDoesNotInsert(t *testing.T) {
	repo := &recurringWatcherRepo{findErr: errors.New("db down")}
	watcher := NewRecurringWatcher(repo, time.Hour, zap.NewNop())

	watcher.scanOnce(context.Background())

	if got := len(repo.inserted); got != 0 {
		t.Errorf("RecurringWatcher.scanOnce(find error) inserted %d runs, want 0", got)
	}
}

func TestRecurringWatcher_RunReturnsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	watcher := NewRecurringWatcher(&recurringWatcherRepo{}, time.Hour, zap.NewNop())

	if err := watcher.Run(ctx); err != nil {
		t.Fatalf("RecurringWatcher.Run(cancelled context) error = %v, want nil", err)
	}
}
