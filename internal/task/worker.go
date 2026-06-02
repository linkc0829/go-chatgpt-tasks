package task

import (
	"context"
	"fmt"
	"time"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
	"go.uber.org/zap"
)

const (
	maxAttempts          = 3
	workerReadCount      = 10
	workerReadBlock      = 5 * time.Second
	workerReclaimMinIdle = 30 * time.Second
	workerMessageTimeout = 10 * time.Second
)

type Worker struct {
	id    string
	repo  Repo
	queue Queue
	exec  Executor
	log   *zap.Logger
}

func NewWorker(id string, repo Repo, queue Queue, exec Executor, log *zap.Logger) *Worker {
	return &Worker{
		id:    id,
		repo:  repo,
		queue: queue,
		exec:  exec,
		log:   log,
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

		for _, msg := range msgs {
			w.process(ctx, msg)
		}
	}
}

func (w *Worker) processReclaimed(ctx context.Context) error {
	msgs, err := w.queue.Reclaim(ctx, w.id, workerReclaimMinIdle, workerReadCount)
	if err != nil {
		return err
	}
	for _, msg := range msgs {
		w.process(ctx, msg)
	}
	return nil
}

func (w *Worker) process(ctx context.Context, qm QueuedMessage) {
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

	if run.IsTerminal() || (run.Status() != StatusQueued && run.Status() != StatusRetry) {
		w.ack(ctx, qm.StreamID)
		return
	}

	if err := run.MarkRunning(); err != nil {
		w.log.Error("worker mark running", zap.String("job_run_id", runID.String()), zap.Error(err))
		w.ack(ctx, qm.StreamID)
		return
	}
	if err := w.persistStatus(ctx, run); err != nil {
		return
	}
	w.appendEvent(ctx, run.ID(), StatusRunning)

	if err := w.exec.Execute(ctx, run); err != nil {
		w.handleFailure(ctx, qm, run, err)
		return
	}

	if err := run.MarkSuccess(); err != nil {
		w.log.Error("worker mark success", zap.String("job_run_id", runID.String()), zap.Error(err))
		return
	}
	if err := w.persistStatus(ctx, run); err != nil {
		return
	}
	w.appendEvent(ctx, run.ID(), StatusSuccess)
	w.ack(ctx, qm.StreamID)
}

func (w *Worker) handleFailure(ctx context.Context, qm QueuedMessage, run *JobRun, execErr error) {
	if run.Attempts()+1 >= maxAttempts {
		if err := run.MarkFailed(); err != nil {
			w.log.Error("worker mark failed", zap.String("job_run_id", run.ID().String()), zap.Error(err))
			return
		}
		if err := w.persistStatus(ctx, run); err != nil {
			return
		}
		w.appendEvent(ctx, run.ID(), StatusFailed)
		if err := w.queue.DeadLetter(ctx, qm.Msg); err != nil {
			w.log.Error("worker dead letter", zap.String("job_run_id", run.ID().String()), zap.Error(err))
			return
		}
		w.ack(ctx, qm.StreamID)
		return
	}

	if err := run.MarkRetry(); err != nil {
		w.log.Error("worker mark retry", zap.String("job_run_id", run.ID().String()), zap.Error(err))
		return
	}
	if err := w.persistStatus(ctx, run); err != nil {
		return
	}
	w.appendEvent(ctx, run.ID(), StatusRetry)
	if err := w.queue.Enqueue(ctx, JobRunMsg{
		JobRunID: run.ID().String(),
		Attempts: run.Attempts(),
	}); err != nil {
		w.log.Error("worker enqueue retry", zap.String("job_run_id", run.ID().String()), zap.Error(err))
		return
	}
	w.log.Info("job run retry scheduled", zap.String("job_run_id", run.ID().String()), zap.Error(execErr))
	w.ack(ctx, qm.StreamID)
}

func (w *Worker) persistStatus(ctx context.Context, run *JobRun) error {
	if err := w.repo.UpdateRunStatus(ctx, run); err != nil {
		w.log.Error("worker update run status", zap.String("job_run_id", run.ID().String()), zap.Error(err))
		return err
	}
	return nil
}

func (w *Worker) appendEvent(ctx context.Context, runID shared.JobRunID, status Status) {
	if err := w.repo.AppendEvent(ctx, NewRunEvent(runID, status)); err != nil {
		w.log.Error("worker append event", zap.String("job_run_id", runID.String()), zap.String("status", string(status)), zap.Error(err))
	}
}

func (w *Worker) ack(ctx context.Context, streamID string) {
	if err := w.queue.Ack(ctx, streamID); err != nil {
		w.log.Error("worker ack", zap.String("stream_id", streamID), zap.Error(err))
	}
}

func contextDone(ctx context.Context) bool {
	return ctx.Err() != nil
}
