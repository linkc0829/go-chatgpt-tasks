package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
	"go.uber.org/zap"
)

type workerRepo struct {
	run           *JobRun
	updatedStatus []Status
	events        []*RunEvent
	children      []*Job
	savedRuns     []*JobRun
}

func (r *workerRepo) SaveJob(context.Context, *Job) error { return nil }
func (r *workerRepo) SaveRun(_ context.Context, run *JobRun) error {
	r.savedRuns = append(r.savedRuns, run)
	return nil
}
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
func (r *workerRepo) ListRuns(context.Context, shared.TenantID, shared.Pagination) ([]*JobRun, int64, error) {
	return nil, 0, nil
}
func (r *workerRepo) ListRunsByJob(context.Context, shared.TenantID, shared.JobID, shared.Pagination) ([]*JobRun, int64, error) {
	return nil, 0, nil
}
func (r *workerRepo) ListEvents(context.Context, shared.TenantID, shared.JobRunID) ([]*RunEvent, error) {
	return nil, nil
}
func (r *workerRepo) AppendEvent(_ context.Context, e *RunEvent) error {
	r.events = append(r.events, e)
	return nil
}
func (r *workerRepo) FindDueRuns(context.Context, int64, time.Time, int32) ([]*JobRun, error) {
	return nil, nil
}
func (r *workerRepo) FindJob(context.Context, shared.JobID) (*Job, error) {
	return nil, ErrJobNotFound
}
func (r *workerRepo) FindChildren(context.Context, shared.JobID, Status) ([]*Job, error) {
	return r.children, nil
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
	worker := NewWorker("worker-test", repo, queue, exec, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(run, "1-0"))

	if got, want := run.Status(), StatusSuccess; got != want {
		t.Errorf("Worker.process(success) status = %q, want %q", got, want)
	}
	wantStatuses := []Status{StatusRunning, StatusSuccess}
	if !statusesEqual(repo.updatedStatus, wantStatuses) {
		t.Errorf("Worker.process(success) updated statuses = %v, want %v", repo.updatedStatus, wantStatuses)
	}
	wantEvents := []EventType{EventJobRunStarted, EventJobRunSucceeded}
	if !eventTypesEqual(repo.events, wantEvents) {
		t.Errorf("Worker.process(success) event types = %v, want %v", eventTypes(repo.events), wantEvents)
	}
	if got, want := queue.acked, []string{"1-0"}; !stringsEqual(got, want) {
		t.Errorf("Worker.process(success) acked = %v, want %v", got, want)
	}
	if got := exec.calls; got != 1 {
		t.Errorf("Worker.process(success) Execute calls = %d, want 1", got)
	}
}

func TestWorker_processSuccessCreatesAndEnqueuesMatchingChild(t *testing.T) {
	parent := newQueuedWorkerRun(t)
	parentID := parent.JobID()
	child, err := NewJob(parent.TenantID(), shared.NewUserID(), "child", ScheduleSpec{
		Type:                  KindOneOff,
		ScheduledAtUTC:        time.Now().UTC(),
		TimezoneID:            "UTC",
		ParentJobID:           &parentID,
		TriggerOnParentStatus: StatusSuccess,
	})
	if err != nil {
		t.Fatalf("NewJob(child) error = %v", err)
	}
	repo := &workerRepo{run: parent, children: []*Job{child}}
	queue := &workerQueue{}
	worker := NewWorker("worker-test", repo, queue, &workerExecutor{}, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(parent, "child-1"))

	if got := len(repo.savedRuns); got != 1 {
		t.Fatalf("Worker.process(success) saved child runs = %d, want 1", got)
	}
	childRun := repo.savedRuns[0]
	if got, want := childRun.JobID(), child.ID(); got != want {
		t.Errorf("child run job_id = %s, want %s", got, want)
	}
	if got, want := childRun.Status(), StatusQueued; got != want {
		t.Errorf("child run status = %q, want %q", got, want)
	}
	if got := len(queue.enqueued); got != 1 {
		t.Fatalf("Worker.process(success) enqueued child runs = %d, want 1", got)
	}
	if want := EventChildEnqueued; repo.events[len(repo.events)-1].EventType() != want {
		t.Errorf("last event = %q, want %q", repo.events[len(repo.events)-1].EventType(), want)
	}
}

func TestWorker_processNonTerminalDoesNotCreateChild(t *testing.T) {
	parent := newQueuedWorkerRun(t)
	repo := &workerRepo{run: parent}
	queue := &workerQueue{}
	worker := NewWorker("worker-test", repo, queue, &workerExecutor{err: errors.New("retry")}, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(parent, "child-2"))

	if got := len(repo.savedRuns); got != 0 {
		t.Errorf("Worker.process(retry) saved child runs = %d, want 0", got)
	}
}

func TestWorker_processTransientFailureRetriesAndAcks(t *testing.T) {
	run := newQueuedWorkerRun(t)
	repo := &workerRepo{run: run}
	queue := &workerQueue{}
	exec := &workerExecutor{err: errors.New("transient")}
	worker := NewWorker("worker-test", repo, queue, exec, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(run, "2-0"))

	if got, want := run.Status(), StatusRetry; got != want {
		t.Errorf("Worker.process(transient failure) status = %q, want %q", got, want)
	}
	if got, want := run.Attempts(), 1; got != want {
		t.Errorf("Worker.process(transient failure) attempts = %d, want %d", got, want)
	}
	if got, want := run.ErrorMessage(), "transient"; got != want {
		t.Errorf("Worker.process(transient failure) error message = %q, want %q", got, want)
	}
	if want := []EventType{EventJobRunStarted, EventJobRunRetry}; !eventTypesEqual(repo.events, want) {
		t.Errorf("Worker.process(transient failure) event types = %v, want %v", eventTypes(repo.events), want)
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
	worker := NewWorker("worker-test", repo, queue, exec, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(run, "3-0"))

	if got, want := run.Status(), StatusFailed; got != want {
		t.Errorf("Worker.process(max failure) status = %q, want %q", got, want)
	}
	if got, want := run.ErrorMessage(), "permanent"; got != want {
		t.Errorf("Worker.process(max failure) error message = %q, want %q", got, want)
	}
	if want := []EventType{EventJobRunStarted, EventJobRunFailed, EventJobRunDLQ}; !eventTypesEqual(repo.events, want) {
		t.Errorf("Worker.process(max failure) event types = %v, want %v", eventTypes(repo.events), want)
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
	worker := NewWorker("worker-test", repo, queue, exec, zap.NewNop(), nil)

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

	run, err := NewJobRun(shared.NewTenantID(), shared.NewJobID(), 1, time.Now().UTC().Add(-time.Minute))
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
			JobRunID:       run.ID().String(),
			TenantID:       run.TenantID().String(),
			IdempotencyKey: run.IdempotencyKey(),
			Attempts:       run.Attempts(),
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

func eventTypes(events []*RunEvent) []EventType {
	out := make([]EventType, 0, len(events))
	for _, event := range events {
		out = append(out, event.EventType())
	}
	return out
}

func eventTypesEqual(events []*RunEvent, want []EventType) bool {
	got := eventTypes(events)
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
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
