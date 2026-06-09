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
	job           *Job
	updatedStatus []Status
	events        []*RunEvent
	children      []*Job
	savedRuns     []*JobRun
	acquire       bool
}

func (r *workerRepo) SaveJob(context.Context, *Job) error { return nil }
func (r *workerRepo) CreateJobWithRun(context.Context, *Job, *JobRun, []*RunEvent) error {
	return nil
}
func (r *workerRepo) SaveRun(_ context.Context, run *JobRun) error {
	r.savedRuns = append(r.savedRuns, run)
	return nil
}
func (r *workerRepo) UpdateRunStatus(_ context.Context, run *JobRun) error {
	r.updatedStatus = append(r.updatedStatus, run.Status())
	return nil
}
func (r *workerRepo) PersistRunTransition(_ context.Context, run *JobRun, event *RunEvent) error {
	r.updatedStatus = append(r.updatedStatus, run.Status())
	r.events = append(r.events, event)
	return nil
}
func (r *workerRepo) TryMarkRunRunning(_ context.Context, run *JobRun, event *RunEvent, _ int) (bool, error) {
	if !r.acquire {
		return false, nil
	}
	r.updatedStatus = append(r.updatedStatus, run.Status())
	r.events = append(r.events, event)
	return true, nil
}
func (r *workerRepo) CancelPendingRunsByJob(context.Context, shared.TenantID, shared.JobID) ([]*JobRun, error) {
	return nil, nil
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
func (r *workerRepo) ListRunsByJob(_ context.Context, _ shared.TenantID, jobID shared.JobID, _ shared.Pagination) ([]*JobRun, int64, error) {
	var count int64
	for _, run := range r.savedRuns {
		if run.JobID() == jobID {
			count++
		}
	}
	return nil, count, nil
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
	if r.job != nil {
		return r.job, nil
	}
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

type workerQuotaRepo struct {
	quota Quota
}

func (r *workerQuotaRepo) Get(context.Context, shared.TenantID) (Quota, error) {
	return r.quota, nil
}
func (r *workerQuotaRepo) CountJobsSince(context.Context, shared.TenantID, time.Time) (int64, error) {
	return 0, nil
}
func (r *workerQuotaRepo) CountActiveRecurring(context.Context, shared.TenantID) (int64, error) {
	return 0, nil
}
func (r *workerQuotaRepo) CountActiveRuns(context.Context, shared.TenantID) (int64, error) {
	return 0, nil
}
func (r *workerQuotaRepo) ReserveDailyCost(context.Context, shared.TenantID, int, int) (bool, error) {
	return true, nil
}
func (r *workerQuotaRepo) AdjustDailyCost(context.Context, shared.TenantID, int) error {
	return nil
}

func (e *workerExecutor) Execute(context.Context, *JobRun) error {
	e.calls++
	return e.err
}

func TestWorker_processSuccessMarksSuccessAndAcks(t *testing.T) {
	run := newQueuedWorkerRun(t)
	repo := &workerRepo{run: run, acquire: true}
	queue := &workerQueue{}
	exec := &workerExecutor{}
	worker := NewWorker("worker-test", repo, queue, nil, exec, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(run, "1-0"), false)

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
	repo := &workerRepo{run: parent, children: []*Job{child}, acquire: true}
	queue := &workerQueue{}
	worker := NewWorker("worker-test", repo, queue, nil, &workerExecutor{}, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(parent, "child-1"), false)

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
	repo := &workerRepo{run: parent, acquire: true}
	queue := &workerQueue{}
	worker := NewWorker("worker-test", repo, queue, nil, &workerExecutor{err: errors.New("retry")}, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(parent, "child-2"), false)

	if got := len(repo.savedRuns); got != 0 {
		t.Errorf("Worker.process(retry) saved child runs = %d, want 0", got)
	}
}

func TestWorker_processTransientFailureRetriesAndAcks(t *testing.T) {
	run := newQueuedWorkerRun(t)
	repo := &workerRepo{run: run, acquire: true}
	queue := &workerQueue{}
	exec := &workerExecutor{err: errors.New("transient")}
	worker := NewWorker("worker-test", repo, queue, nil, exec, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(run, "2-0"), false)

	if got, want := run.Status(), StatusRetry; got != want {
		t.Errorf("Worker.process(transient failure) status = %q, want %q", got, want)
	}
	if got, want := run.Attempts(), 1; got != want {
		t.Errorf("Worker.process(transient failure) attempts = %d, want %d", got, want)
	}
	if got, want := run.ErrorMessage(), "transient"; got != want {
		t.Errorf("Worker.process(transient failure) error message = %q, want %q", got, want)
	}
	if want := []EventType{EventJobRunStarted, EventJobRunRetry, EventJobRunEnqueued}; !eventTypesEqual(repo.events, want) {
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
	repo := &workerRepo{run: run, acquire: true}
	queue := &workerQueue{}
	exec := &workerExecutor{err: errors.New("permanent")}
	worker := NewWorker("worker-test", repo, queue, nil, exec, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(run, "3-0"), false)

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

func TestWorker_processInvalidLLMOutputRetriesThenFailsWithoutSuccess(t *testing.T) {
	run := newQueuedWorkerRun(t)
	job, err := NewJob(run.TenantID(), shared.NewUserID(), "return JSON", ScheduleSpec{
		Type:           KindOneOff,
		ScheduledAtUTC: time.Now().UTC(),
		TimezoneID:     "UTC",
		JobType:        JobTypeGenericLLM,
	})
	if err != nil {
		t.Fatalf("NewJob() error = %v", err)
	}
	run.jobID = job.ID()
	run.idempotencyKey = job.ID().String() + ":1"
	repo := &workerRepo{run: run, job: job, acquire: true}
	queue := &workerQueue{}
	client := &FakeLLMClient{Response: LLMResponse{Content: "invalid"}}
	exec := NewLLMExecutor(repo, &workerQuotaRepo{quota: Quota{MaxDailyLLMCostCents: 100}}, client, LLMPolicy{
		TimeoutSeconds:  1,
		MaxInputTokens:  100,
		MaxOutputTokens: 100,
		MaxCostCents:    100,
		OutputSchema:    "{}",
	}, nil)
	worker := NewWorker("worker-test", repo, queue, nil, exec, zap.NewNop(), nil)

	for i := 0; i < maxAttempts; i++ {
		worker.process(context.Background(), queuedMessageFor(run, "llm-invalid"), false)
	}

	if got, want := run.Status(), StatusFailed; got != want {
		t.Errorf("Worker.process(invalid LLM output) status = %q, want %q", got, want)
	}
	validationFailures := 0
	for _, event := range repo.events {
		if event.EventType() == EventJobRunSucceeded {
			t.Errorf("Worker.process(invalid LLM output) emitted unexpected %q event", EventJobRunSucceeded)
		}
		if event.EventType() == EventLLMValidationFailed {
			validationFailures++
		}
	}
	if got, want := validationFailures, maxAttempts; got != want {
		t.Errorf("Worker.process(invalid LLM output) validation events = %d, want %d", got, want)
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
	repo := &workerRepo{run: run, acquire: true}
	queue := &workerQueue{}
	exec := &workerExecutor{}
	worker := NewWorker("worker-test", repo, queue, nil, exec, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(run, "4-0"), false)

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

func TestWorker_processReclaimedStalledRunRetries(t *testing.T) {
	run := newRunningWorkerRun(t)
	repo := &workerRepo{run: run, acquire: true}
	queue := &workerQueue{}
	exec := &workerExecutor{}
	worker := NewWorker("worker-test", repo, queue, nil, exec, zap.NewNop(), nil)

	// Reclaimed delivery of a run still marked running ⇒ the prior owner stalled.
	worker.process(context.Background(), queuedMessageFor(run, "stall-1"), true)

	if got, want := run.Status(), StatusRetry; got != want {
		t.Errorf("reclaimed stalled run status = %q, want %q", got, want)
	}
	if got := exec.calls; got != 0 {
		t.Errorf("reclaimed stalled run Execute calls = %d, want 0 (recovered, not executed)", got)
	}
	if got := len(queue.enqueued); got != 1 {
		t.Fatalf("reclaimed stalled run enqueued = %d, want 1", got)
	}
	if got, want := run.ErrorMessage(), ErrWorkerStalled.Error(); got != want {
		t.Errorf("reclaimed stalled run error = %q, want %q", got, want)
	}
	if got, want := queue.acked, []string{"stall-1"}; !stringsEqual(got, want) {
		t.Errorf("reclaimed stalled run acked = %v, want %v", got, want)
	}
}

func TestWorker_processReclaimedStalledRunAtMaxDeadLetters(t *testing.T) {
	run := newQueuedWorkerRun(t)
	run.attempts = maxAttempts - 1
	if err := run.MarkRunning(); err != nil {
		t.Fatalf("MarkRunning() error = %v", err)
	}
	repo := &workerRepo{run: run, acquire: true}
	queue := &workerQueue{}
	worker := NewWorker("worker-test", repo, queue, nil, &workerExecutor{}, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(run, "stall-max"), true)

	if got, want := run.Status(), StatusFailed; got != want {
		t.Errorf("reclaimed stalled run at max status = %q, want %q", got, want)
	}
	if got := len(queue.deadLetters); got != 1 {
		t.Fatalf("reclaimed stalled run at max dead letters = %d, want 1", got)
	}
}

func TestWorker_processFreshRunningRunIsSkipped(t *testing.T) {
	run := newRunningWorkerRun(t)
	repo := &workerRepo{run: run, acquire: true}
	queue := &workerQueue{}
	exec := &workerExecutor{}
	worker := NewWorker("worker-test", repo, queue, nil, exec, zap.NewNop(), nil)

	// Fresh (non-reclaimed) delivery of a running run ⇒ another worker owns it.
	worker.process(context.Background(), queuedMessageFor(run, "dup-1"), false)

	if got, want := run.Status(), StatusRunning; got != want {
		t.Errorf("fresh running run status = %q, want %q (left untouched)", got, want)
	}
	if got := exec.calls; got != 0 {
		t.Errorf("fresh running run Execute calls = %d, want 0", got)
	}
	if got := len(repo.updatedStatus); got != 0 {
		t.Errorf("fresh running run status updates = %d, want 0", got)
	}
	if got, want := queue.acked, []string{"dup-1"}; !stringsEqual(got, want) {
		t.Errorf("fresh running run acked = %v, want %v", got, want)
	}
}

func TestWorker_processOverConcurrentQuotaDefersWithoutExecuting(t *testing.T) {
	run := newQueuedWorkerRun(t)
	repo := &workerRepo{run: run, acquire: false}
	queue := &workerQueue{}
	exec := &workerExecutor{}
	quota := &workerQuotaRepo{quota: Quota{MaxConcurrentRuns: 1}}
	worker := NewWorker("worker-test", repo, queue, quota, exec, zap.NewNop(), nil)

	worker.process(context.Background(), queuedMessageFor(run, "quota-1"), false)

	if got := exec.calls; got != 0 {
		t.Errorf("Worker.process(over quota) Execute calls = %d, want 0", got)
	}
	// Deferral leaves the message unacked and un-enqueued so the consumer-group
	// reclaim redelivers it after the idle window — a natural backoff instead of
	// a tight defer loop. No lifecycle event is written for a transient defer.
	if got := len(queue.enqueued); got != 0 {
		t.Errorf("Worker.process(over quota) enqueued = %d, want 0 (left for reclaim)", got)
	}
	if got := len(queue.acked); got != 0 {
		t.Errorf("Worker.process(over quota) acked = %d, want 0 (left unacked for reclaim)", got)
	}
	if got := len(repo.updatedStatus); got != 0 {
		t.Errorf("Worker.process(over quota) DB status updates = %d, want 0 (row untouched)", got)
	}
	for _, e := range repo.events {
		if e.EventType() == EventQuotaDeferred {
			t.Errorf("Worker.process(over quota) wrote a %q event; deferrals must be metric-only", EventQuotaDeferred)
		}
	}
}

func TestWorkerReclaimNeverOverlapsLiveExecution(t *testing.T) {
	// A message is only reclaimed (and a 'running' run treated as a crashed
	// owner) after it has been idle longer than workerReclaimMinIdle. If that
	// floor is not strictly above workerMessageTimeout, a run that is still
	// legitimately executing can be reclaimed and failed mid-flight by another
	// worker — a correctness + double-cost regression. Guard the invariant.
	if workerReclaimMinIdle <= workerMessageTimeout {
		t.Fatalf("workerReclaimMinIdle (%s) must exceed workerMessageTimeout (%s) so reclaim cannot fire while a run is still executing",
			workerReclaimMinIdle, workerMessageTimeout)
	}
}

func TestFairOrderRoundRobinsTenants(t *testing.T) {
	msgs := []QueuedMessage{
		{Msg: JobRunMsg{TenantID: "a", JobRunID: "a1"}},
		{Msg: JobRunMsg{TenantID: "a", JobRunID: "a2"}},
		{Msg: JobRunMsg{TenantID: "b", JobRunID: "b1"}},
		{Msg: JobRunMsg{TenantID: "a", JobRunID: "a3"}},
		{Msg: JobRunMsg{TenantID: "b", JobRunID: "b2"}},
	}

	got := fairOrder(msgs)
	want := []string{"a1", "b1", "a2", "b2", "a3"}
	for i, jobRunID := range want {
		if got[i].Msg.JobRunID != jobRunID {
			t.Errorf("fairOrder()[%d].job_run_id = %q, want %q", i, got[i].Msg.JobRunID, jobRunID)
		}
	}
}

func TestWorker_enqueueChildrenGivesDistinctSequencesPerFiring(t *testing.T) {
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
	if err := parent.MarkRunning(); err != nil {
		t.Fatalf("MarkRunning() error = %v", err)
	}
	if err := parent.MarkSuccess(); err != nil {
		t.Fatalf("MarkSuccess() error = %v", err)
	}
	repo := &workerRepo{run: parent, children: []*Job{child}, acquire: true}
	worker := NewWorker("worker-test", repo, &workerQueue{}, nil, &workerExecutor{}, zap.NewNop(), nil)

	// A recurring parent firing twice must not collide on a single child key.
	worker.enqueueChildren(context.Background(), parent)
	worker.enqueueChildren(context.Background(), parent)

	if got := len(repo.savedRuns); got != 2 {
		t.Fatalf("child runs saved = %d, want 2", got)
	}
	if a, b := repo.savedRuns[0].IdempotencyKey(), repo.savedRuns[1].IdempotencyKey(); a == b {
		t.Errorf("child run idempotency keys collide: both %q", a)
	}
}

func newRunningWorkerRun(t *testing.T) *JobRun {
	t.Helper()
	run := newQueuedWorkerRun(t)
	if err := run.MarkRunning(); err != nil {
		t.Fatalf("MarkRunning() error = %v, want nil", err)
	}
	return run
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
