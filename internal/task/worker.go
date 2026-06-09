package task

import (
	"context"
	"fmt"
	"time"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
	"go.uber.org/zap"
)

const (
	maxAttempts     = 3
	workerReadCount = 10
	workerReadBlock = 5 * time.Second
	// A run may legitimately execute for up to workerMessageTimeout (LLM
	// timeout × retries). Reclaim must only redeliver a message after that
	// window has certainly elapsed — otherwise a healthy, still-running run
	// would be reclaimed by another worker and failed mid-execution (two
	// writers on one row, double execution, double cost reservation). Keep
	// workerReclaimMinIdle strictly greater than workerMessageTimeout.
	workerMessageTimeout = 3 * time.Minute
	workerReclaimMinIdle = workerMessageTimeout + 30*time.Second
)

type Worker struct {
	id    string
	repo  Repo
	queue Queue
	quota QuotaRepo
	exec  Executor
	log   *zap.Logger
	m     *Metrics
}

func NewWorker(id string, repo Repo, queue Queue, quota QuotaRepo, exec Executor, log *zap.Logger, m *Metrics) *Worker {
	return &Worker{
		id:    id,
		repo:  repo,
		queue: queue,
		quota: quota,
		exec:  exec,
		log:   log,
		m:     m,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.queue.EnsureGroup(ctx); err != nil {
		return fmt.Errorf("ensure group: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := w.processReclaimed(ctx); err != nil {
			if contextDone(ctx) {
				return nil
			}
			w.log.Error("worker reclaim", zap.Error(err))
		}

		msgs, err := w.queue.Read(ctx, w.id, workerReadCount, workerReadBlock)
		if err != nil {
			if contextDone(ctx) {
				return nil
			}
			w.log.Error("worker read", zap.Error(err))
			continue
		}

		for _, msg := range fairOrder(msgs) {
			w.process(ctx, msg, false)
		}
	}
}

func (w *Worker) processReclaimed(ctx context.Context) error {
	msgs, err := w.queue.Reclaim(ctx, w.id, workerReclaimMinIdle, workerReadCount)
	if err != nil {
		return err
	}
	for _, msg := range fairOrder(msgs) {
		w.process(ctx, msg, true)
	}
	return nil
}

func (w *Worker) process(ctx context.Context, qm QueuedMessage, reclaimed bool) {
	ctx, cancel := context.WithTimeout(ctx, workerMessageTimeout)
	defer cancel()

	runID, err := shared.ParseJobRunID(qm.Msg.JobRunID)
	if err != nil {
		w.log.Error("worker parse job run id", zap.String("job_run_id", qm.Msg.JobRunID), zap.Error(err))
		w.ack(ctx, qm.StreamID)
		return
	}

	run, err := w.repo.FindRunByID(ctx, runID)
	if err != nil {
		w.log.Error("worker find run", zap.String("job_run_id", runID.String()), zap.Error(err))
		return
	}

	if run.IsTerminal() {
		w.ack(ctx, qm.StreamID)
		return
	}
	if run.Status() == StatusRunning {
		// A reclaimed message whose run is still 'running' means the previous
		// owner stalled or crashed mid-execution — recover it through the
		// failure path (retry within budget, else fail + DLQ); the idempotency
		// layer protects side-effecting handlers from a double side effect. A
		// fresh delivery for a running run means another worker owns it, so the
		// duplicate is dropped.
		if reclaimed {
			w.handleFailure(ctx, qm, run, ErrWorkerStalled)
		} else {
			w.ack(ctx, qm.StreamID)
		}
		return
	}
	if run.Status() != StatusQueued && run.Status() != StatusRetry {
		w.ack(ctx, qm.StreamID)
		return
	}

	if err := run.MarkRunning(); err != nil {
		w.log.Error("worker mark running", zap.String("job_run_id", runID.String()), zap.Error(err))
		w.ack(ctx, qm.StreamID)
		return
	}
	startedEvent := NewRunEvent(run.TenantID(), run.JobID(), run.ID(), run.Status(), EventJobRunStarted, nil)
	acquired, err := w.acquireRunSlot(ctx, run, startedEvent)
	if err != nil {
		w.log.Error("worker acquire tenant run slot", zap.String("job_run_id", run.ID().String()), zap.Error(err))
		return
	}
	if !acquired {
		// Over the tenant's concurrency limit. Leave the message unacked so the
		// consumer-group reclaim redelivers it after workerReclaimMinIdle — a
		// natural backoff that avoids a tight defer/re-enqueue loop and the
		// unbounded run_events growth that a per-poll deferral event would
		// cause. The DB row is untouched (still queued/retry); deferrals are
		// surfaced via a metric, not a lifecycle event.
		w.m.recordRunDeferred(run.TenantID(), "max_concurrent_runs")
		return
	}
	if w.quota == nil {
		if err := w.persistTransition(ctx, run, startedEvent); err != nil {
			return
		}
	}

	if err := w.exec.Execute(ctx, run); err != nil {
		w.handleFailure(ctx, qm, run, err)
		return
	}

	if err := run.MarkSuccess(); err != nil {
		w.log.Error("worker mark success", zap.String("job_run_id", runID.String()), zap.Error(err))
		return
	}
	if err := w.persistTransition(ctx, run, NewRunEvent(
		run.TenantID(), run.JobID(), run.ID(), run.Status(), EventJobRunSucceeded, nil,
	)); err != nil {
		return
	}
	w.enqueueChildren(ctx, run)
	w.m.recordRun(run)
	w.ack(ctx, qm.StreamID)
}

func (w *Worker) handleFailure(ctx context.Context, qm QueuedMessage, run *JobRun, execErr error) {
	if run.Attempts()+1 >= maxAttempts {
		run.setError("execution_error", execErr.Error())
		if err := run.MarkFailed(); err != nil {
			w.log.Error("worker mark failed", zap.String("job_run_id", run.ID().String()), zap.Error(err))
			return
		}
		payload := map[string]any{"error_code": run.ErrorCode(), "error_message": run.ErrorMessage()}
		if err := w.persistTransition(ctx, run, NewRunEvent(
			run.TenantID(), run.JobID(), run.ID(), run.Status(), EventJobRunFailed, payload,
		)); err != nil {
			return
		}
		if err := w.appendEvent(ctx, run, EventJobRunDLQ, payload); err != nil {
			return
		}
		w.enqueueChildren(ctx, run)
		if err := w.queue.DeadLetter(ctx, qm.Msg); err != nil {
			w.log.Error("worker dead letter", zap.String("job_run_id", run.ID().String()), zap.Error(err))
			return
		}
		w.m.recordRun(run)
		w.m.recordDLQ()
		w.ack(ctx, qm.StreamID)
		return
	}

	run.setError("execution_error", execErr.Error())
	if err := run.MarkRetry(); err != nil {
		w.log.Error("worker mark retry", zap.String("job_run_id", run.ID().String()), zap.Error(err))
		return
	}
	if err := w.persistTransition(ctx, run, NewRunEvent(
		run.TenantID(), run.JobID(), run.ID(), run.Status(), EventJobRunRetry,
		map[string]any{"attempt": run.Attempts(), "error": execErr.Error()},
	)); err != nil {
		return
	}
	w.m.recordRun(run)
	if err := w.queue.Enqueue(ctx, JobRunMsg{
		JobRunID:       run.ID().String(),
		TenantID:       run.TenantID().String(),
		IdempotencyKey: run.IdempotencyKey(),
		Attempts:       run.Attempts(),
	}); err != nil {
		w.log.Error("worker enqueue retry", zap.String("job_run_id", run.ID().String()), zap.Error(err))
		return
	}
	if err := w.appendEvent(ctx, run, EventJobRunEnqueued, nil); err != nil {
		return
	}
	w.log.Info("job run retry scheduled", zap.String("job_run_id", run.ID().String()), zap.Error(execErr))
	w.ack(ctx, qm.StreamID)
}

func (w *Worker) enqueueChildren(ctx context.Context, parent *JobRun) {
	children, err := w.repo.FindChildren(ctx, parent.JobID(), parent.Status())
	if err != nil {
		w.log.Error("worker find child jobs", zap.String("job_id", parent.JobID().String()), zap.Error(err))
		return
	}
	for _, child := range children {
		// Derive the sequence from existing child runs so a recurring parent
		// firing repeatedly produces distinct idempotency keys (jobID:N) rather
		// than colliding on jobID:1 and being suppressed as duplicates.
		_, total, err := w.repo.ListRunsByJob(ctx, child.TenantID(), child.ID(), shared.NewPagination(1, 0))
		if err != nil {
			w.log.Error("worker count child runs", zap.String("job_id", child.ID().String()), zap.Error(err))
			continue
		}
		run, err := NewJobRun(child.TenantID(), child.ID(), int(total)+1, time.Now().UTC())
		if err != nil {
			w.log.Error("worker build child run", zap.String("job_id", child.ID().String()), zap.Error(err))
			continue
		}
		if err := run.MarkQueued(); err != nil {
			w.log.Error("worker mark child queued", zap.String("job_id", child.ID().String()), zap.Error(err))
			continue
		}
		if err := w.repo.SaveRun(ctx, run); err != nil {
			w.log.Error("worker save child run", zap.String("job_id", child.ID().String()), zap.Error(err))
			continue
		}
		if err := w.queue.Enqueue(ctx, JobRunMsg{
			JobRunID:       run.ID().String(),
			TenantID:       run.TenantID().String(),
			IdempotencyKey: run.IdempotencyKey(),
		}); err != nil {
			w.log.Error("worker enqueue child run", zap.String("job_run_id", run.ID().String()), zap.Error(err))
			continue
		}
		if err := w.appendEvent(ctx, run, EventChildEnqueued, map[string]any{"parent_job_id": parent.JobID().String()}); err != nil {
			continue
		}
	}
}

func (w *Worker) persistTransition(ctx context.Context, run *JobRun, event *RunEvent) error {
	if err := w.repo.PersistRunTransition(ctx, run, event); err != nil {
		w.log.Error("worker persist run transition",
			zap.String("job_run_id", run.ID().String()),
			zap.String("event_type", string(event.EventType())),
			zap.Error(err),
		)
		return err
	}
	return nil
}

func (w *Worker) appendEvent(ctx context.Context, run *JobRun, eventType EventType, payload map[string]any) error {
	event := NewRunEvent(run.TenantID(), run.JobID(), run.ID(), run.Status(), eventType, payload)
	if err := w.repo.AppendEvent(ctx, event); err != nil {
		w.log.Error("worker append event", zap.String("job_run_id", run.ID().String()), zap.String("event_type", string(eventType)), zap.Error(err))
		return err
	}
	return nil
}

func (w *Worker) acquireRunSlot(ctx context.Context, run *JobRun, event *RunEvent) (bool, error) {
	if w.quota == nil {
		return true, nil
	}
	quota, err := w.quota.Get(ctx, run.TenantID())
	if err != nil {
		return false, fmt.Errorf("get tenant quota: %w", err)
	}
	// Authoritative concurrency enforcement: atomically transition to running
	// only if the tenant's count of status='running' runs is under the limit.
	// This is the true "concurrent executions" gate; the creation-time check in
	// Service.checkQuota is a coarser admission guard over in-flight backlog.
	return w.repo.TryMarkRunRunning(ctx, run, event, quota.MaxConcurrentRuns)
}

func fairOrder(msgs []QueuedMessage) []QueuedMessage {
	byTenant := make(map[string][]QueuedMessage)
	order := make([]string, 0)
	for _, msg := range msgs {
		if _, ok := byTenant[msg.Msg.TenantID]; !ok {
			order = append(order, msg.Msg.TenantID)
		}
		byTenant[msg.Msg.TenantID] = append(byTenant[msg.Msg.TenantID], msg)
	}
	out := make([]QueuedMessage, 0, len(msgs))
	for len(out) < len(msgs) {
		for _, tenantID := range order {
			queue := byTenant[tenantID]
			if len(queue) == 0 {
				continue
			}
			out = append(out, queue[0])
			byTenant[tenantID] = queue[1:]
		}
	}
	return out
}

func (w *Worker) ack(ctx context.Context, streamID string) {
	if err := w.queue.Ack(ctx, streamID); err != nil {
		w.log.Error("worker ack", zap.String("stream_id", streamID), zap.Error(err))
	}
}

func contextDone(ctx context.Context) bool {
	return ctx.Err() != nil
}
