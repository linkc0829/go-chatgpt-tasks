package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

type watcherRepo struct {
	findDueRuns  []*JobRun
	findDueErr   error
	findDueCalls int
	updatedRuns  []*JobRun
	updateRunErr error
}

func (r *watcherRepo) SaveJob(context.Context, *Job) error    { return nil }
func (r *watcherRepo) SaveRun(context.Context, *JobRun) error { return nil }
func (r *watcherRepo) UpdateRunStatus(_ context.Context, run *JobRun) error {
	r.updatedRuns = append(r.updatedRuns, run)
	return r.updateRunErr
}
func (r *watcherRepo) FindRunByID(context.Context, shared.JobRunID) (*JobRun, error) {
	return nil, ErrJobRunNotFound
}
func (r *watcherRepo) ListRuns(context.Context, shared.TenantID, shared.Pagination) ([]*JobRun, int64, error) {
	return nil, 0, nil
}
func (r *watcherRepo) ListRunsByJob(context.Context, shared.TenantID, shared.JobID, shared.Pagination) ([]*JobRun, int64, error) {
	return nil, 0, nil
}
func (r *watcherRepo) ListEvents(context.Context, shared.TenantID, shared.JobRunID) ([]*RunEvent, error) {
	return nil, nil
}
func (r *watcherRepo) AppendEvent(context.Context, *RunEvent) error { return nil }
func (r *watcherRepo) FindDueRuns(context.Context, int64, time.Time, int32) ([]*JobRun, error) {
	r.findDueCalls++
	if r.findDueCalls > 1 {
		return nil, r.findDueErr
	}
	return r.findDueRuns, r.findDueErr
}
func (r *watcherRepo) FindJob(context.Context, shared.JobID) (*Job, error) {
	return nil, ErrJobNotFound
}
func (r *watcherRepo) FindChildren(context.Context, shared.JobID, Status) ([]*Job, error) {
	return nil, nil
}
func (r *watcherRepo) InsertRunIfAbsent(context.Context, *JobRun) (bool, error) {
	return false, nil
}
func (r *watcherRepo) FindTerminalRecurringRuns(context.Context, time.Time, int32) ([]NextRunSpec, error) {
	return nil, nil
}

type watcherQueue struct {
	t        *testing.T
	runs     map[string]*JobRun
	enqueued []JobRunMsg
	err      error
}

func (q *watcherQueue) Enqueue(_ context.Context, msg JobRunMsg) error {
	if q.err != nil {
		return q.err
	}
	if q.t != nil {
		q.t.Helper()
		run := q.runs[msg.JobRunID]
		if run == nil {
			q.t.Fatalf("Watcher.scanOnce() enqueued unknown job_run_id %q", msg.JobRunID)
		}
		if got, want := run.Status(), StatusQueued; got != want {
			q.t.Fatalf("Watcher.scanOnce() enqueued status = %q, want %q", got, want)
		}
	}
	q.enqueued = append(q.enqueued, msg)
	return nil
}

func (q *watcherQueue) EnsureGroup(context.Context) error { return nil }
func (q *watcherQueue) Read(context.Context, string, int64, time.Duration) ([]QueuedMessage, error) {
	return nil, nil
}
func (q *watcherQueue) Reclaim(context.Context, string, time.Duration, int64) ([]QueuedMessage, error) {
	return nil, nil
}
func (q *watcherQueue) Ack(context.Context, string) error { return nil }
func (q *watcherQueue) DeadLetter(context.Context, JobRunMsg) error {
	return nil
}

func TestWatcher_scanOnceQueuesDuePendingRun(t *testing.T) {
	run := newWatcherRun(t)
	repo := &watcherRepo{findDueRuns: []*JobRun{run}}
	queue := &watcherQueue{
		t:    t,
		runs: map[string]*JobRun{run.ID().String(): run},
	}
	watcher := NewWatcher(repo, queue, time.Hour, zap.NewNop())

	watcher.scanOnce(context.Background())

	if len(queue.enqueued) != 1 {
		t.Fatalf("Watcher.scanOnce() enqueued %d messages, want 1", len(queue.enqueued))
	}
	if got, want := queue.enqueued[0].JobRunID, run.ID().String(); got != want {
		t.Errorf("Watcher.scanOnce() enqueued job_run_id = %q, want %q", got, want)
	}
	if len(repo.updatedRuns) != 1 {
		t.Fatalf("Watcher.scanOnce() updated %d runs, want 1", len(repo.updatedRuns))
	}
	if got, want := repo.updatedRuns[0].Status(), StatusQueued; got != want {
		t.Errorf("Watcher.scanOnce() status = %q, want %q", got, want)
	}
}

func TestWatcher_scanOnceFindDueErrorDoesNotEnqueue(t *testing.T) {
	repo := &watcherRepo{findDueErr: errors.New("db down")}
	queue := &watcherQueue{}
	watcher := NewWatcher(repo, queue, time.Hour, zap.NewNop())

	watcher.scanOnce(context.Background())

	if got := len(queue.enqueued); got != 0 {
		t.Errorf("Watcher.scanOnce() enqueued %d messages, want 0", got)
	}
	if got := len(repo.updatedRuns); got != 0 {
		t.Errorf("Watcher.scanOnce() updated %d runs, want 0", got)
	}
}

func TestWatcher_RunReturnsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	watcher := NewWatcher(&watcherRepo{}, &watcherQueue{}, time.Hour, zap.NewNop())

	if err := watcher.Run(ctx); err != nil {
		t.Fatalf("Watcher.Run(cancelled context) error = %v, want nil", err)
	}
}

func newWatcherRun(t *testing.T) *JobRun {
	t.Helper()

	run, err := NewJobRun(
		shared.NewTenantID(),
		shared.NewJobID(),
		1,
		time.Now().UTC().Add(-time.Minute),
	)
	if err != nil {
		t.Fatalf("NewJobRun() error = %v, want nil", err)
	}
	return run
}
