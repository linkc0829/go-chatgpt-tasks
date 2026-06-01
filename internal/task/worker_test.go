package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/linkc0829/go-backend-template/internal/shared"
	"go.uber.org/zap"
)

type workerRepo struct {
	run           *JobRun
	updatedStatus []Status
	events        []Status
}

func (r *workerRepo) SaveJob(context.Context, *Job) error    { return nil }
func (r *workerRepo) SaveRun(context.Context, *JobRun) error { return nil }
func (r *workerRepo) UpdateRunStatus(_ context.Context, run *JobRun) error {
	r.updatedStatus = append(r.updatedStatus, run.Status())
	return nil
}
func (r *workerRepo) FindRunByID(context.Context, shared.JobRunID) (*JobRun, error) {
	if r.run == nil {
		return nil, ErrJobRunNotFound
	}
	return r.run, nil
}
func (r *workerRepo) ListRuns(context.Context, shared.Pagination) ([]*JobRun, int64, error) {
	return nil, 0, nil
}
func (r *workerRepo) AppendEvent(_ context.Context, e *RunEvent) error {
	r.events = append(r.events, e.Status())
	return nil
}
func (r *workerRepo) FindDueRuns(context.Context, int64, time.Time, int32) ([]*JobRun, error) {
	return nil, nil
}
func (r *workerRepo) FindJob(context.Context, shared.JobID) (*Job, error) {
	return nil, ErrJobNotFound
}
func (r *workerRepo) InsertRunIfAbsent(context.Context, *JobRun) (bool, error) {
	return false, nil
}
func (r *workerRepo) FindTerminalRecurringRuns(context.Context, time.Time, int32) ([]NextRunSpec, error) {
	return nil, nil
}

type workerQueue struct {
	enqueued    []JobRunMsg
	deadLetters []JobRunMsg
	acked       []string
}

func (q *workerQueue) Enqueue(_ context.Context, msg JobRunMsg) error {
	q.enqueued = append(q.enqueued, msg)
	return nil
}
func (q *workerQueue) EnsureGroup(context.Context) error { return nil }
func (q *workerQueue) Read(context.Context, string, int64, time.Duration) ([]QueuedMessage, error) {
	return nil, nil
}
func (q *workerQueue) Reclaim(context.Context, string, time.Duration, int64) ([]QueuedMessage, error) {
	return nil, nil
}
func (q *workerQueue) Ack(_ context.Context, streamID string) error {
	q.acked = append(q.acked, streamID)
	return nil
}
func (q *workerQueue) DeadLetter(_ context.Context, msg JobRunMsg) error {
	q.deadLetters = append(q.deadLetters, msg)
	return nil
}

type workerExecutor struct {
	err   error
	calls int
}

func (e *workerExecutor) Execute(context.Context, *JobRun) error {
	e.calls++
	return e.err
}

func TestWorker_processSuccessMarksSuccessAndAcks(t *testing.T) {
	run := newQueuedWorkerRun(t)
	repo := &workerRepo{run: run}
	queue := &workerQueue{}
	exec := &workerExecutor{}
	worker := NewWorker("worker-test", repo, queue, exec, zap.NewNop())

	worker.process(context.Background(), queuedMessageFor(run, "1-0"))

	if got, want := run.Status(), StatusSuccess; got != want {
		t.Errorf("Worker.process(success) status = %q, want %q", got, want)
	}
	wantStatuses := []Status{StatusRunning, StatusSuccess}
	if !statusesEqual(repo.updatedStatus, wantStatuses) {
		t.Errorf("Worker.process(success) updated statuses = %v, want %v", repo.updatedStatus, wantStatuses)
	}
	if !statusesEqual(repo.events, wantStatuses) {
		t.Errorf("Worker.process(success) events = %v, want %v", repo.events, wantStatuses)
	}
	if got, want := queue.acked, []string{"1-0"}; !stringsEqual(got, want) {
		t.Errorf("Worker.process(success) acked = %v, want %v", got, want)
	}
	if got := exec.calls; got != 1 {
		t.Errorf("Worker.process(success) Execute calls = %d, want 1", got)
	}
}

func TestWorker_processTransientFailureRetriesAndAcks(t *testing.T) {
	run := newQueuedWorkerRun(t)
	repo := &workerRepo{run: run}
	queue := &workerQueue{}
	exec := &workerExecutor{err: errors.New("transient")}
	worker := NewWorker("worker-test", repo, queue, exec, zap.NewNop())

	worker.process(context.Background(), queuedMessageFor(run, "2-0"))

	if got, want := run.Status(), StatusRetry; got != want {
		t.Errorf("Worker.process(transient failure) status = %q, want %q", got, want)
	}
	if got, want := run.Attempts(), 1; got != want {
		t.Errorf("Worker.process(transient failure) attempts = %d, want %d", got, want)
	}
	if got := len(queue.enqueued); got != 1 {
		t.Fatalf("Worker.process(transient failure) enqueued %d retries, want 1", got)
	}
	if got, want := queue.enqueued[0].Attempts, 1; got != want {
		t.Errorf("Worker.process(transient failure) retry attempts = %d, want %d", got, want)
	}
	if got, want := queue.acked, []string{"2-0"}; !stringsEqual(got, want) {
		t.Errorf("Worker.process(transient failure) acked = %v, want %v", got, want)
	}
}

func TestWorker_processMaxFailureDeadLettersAndAcks(t *testing.T) {
	run := newQueuedWorkerRun(t)
	run.attempts = maxAttempts - 1
	repo := &workerRepo{run: run}
	queue := &workerQueue{}
	exec := &workerExecutor{err: errors.New("permanent")}
	worker := NewWorker("worker-test", repo, queue, exec, zap.NewNop())

	worker.process(context.Background(), queuedMessageFor(run, "3-0"))

	if got, want := run.Status(), StatusFailed; got != want {
		t.Errorf("Worker.process(max failure) status = %q, want %q", got, want)
	}
	if got := len(queue.deadLetters); got != 1 {
		t.Fatalf("Worker.process(max failure) dead letters = %d, want 1", got)
	}
	if got, want := queue.acked, []string{"3-0"}; !stringsEqual(got, want) {
		t.Errorf("Worker.process(max failure) acked = %v, want %v", got, want)
	}
}

func TestWorker_processTerminalRunAcksWithoutExecute(t *testing.T) {
	run := newQueuedWorkerRun(t)
	if err := run.MarkRunning(); err != nil {
		t.Fatalf("MarkRunning() error = %v, want nil", err)
	}
	if err := run.MarkSuccess(); err != nil {
		t.Fatalf("MarkSuccess() error = %v, want nil", err)
	}
	repo := &workerRepo{run: run}
	queue := &workerQueue{}
	exec := &workerExecutor{}
	worker := NewWorker("worker-test", repo, queue, exec, zap.NewNop())

	worker.process(context.Background(), queuedMessageFor(run, "4-0"))

	if got := exec.calls; got != 0 {
		t.Errorf("Worker.process(terminal) Execute calls = %d, want 0", got)
	}
	if got := len(repo.updatedStatus); got != 0 {
		t.Errorf("Worker.process(terminal) updated statuses = %d, want 0", got)
	}
	if got, want := queue.acked, []string{"4-0"}; !stringsEqual(got, want) {
		t.Errorf("Worker.process(terminal) acked = %v, want %v", got, want)
	}
}

func newQueuedWorkerRun(t *testing.T) *JobRun {
	t.Helper()

	run, err := NewJobRun(shared.NewJobID(), 1, time.Now().UTC().Add(-time.Minute))
	if err != nil {
		t.Fatalf("NewJobRun() error = %v, want nil", err)
	}
	if err := run.MarkQueued(); err != nil {
		t.Fatalf("MarkQueued() error = %v, want nil", err)
	}
	return run
}

func queuedMessageFor(run *JobRun, streamID string) QueuedMessage {
	return QueuedMessage{
		StreamID: streamID,
		Msg: JobRunMsg{
			JobRunID: run.ID().String(),
			Attempts: run.Attempts(),
		},
	}
}

func statusesEqual(a, b []Status) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
