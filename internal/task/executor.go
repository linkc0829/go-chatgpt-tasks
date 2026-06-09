package task

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
)

var ErrExecutionInProgress = errors.New("execution already in progress")

type IdempotentExecutor struct {
	repo  Repo
	store IdempotencyStore
	next  Executor
	log   *zap.Logger
}

func NewIdempotentExecutor(repo Repo, store IdempotencyStore, next Executor, log *zap.Logger) *IdempotentExecutor {
	return &IdempotentExecutor{repo: repo, store: store, next: next, log: log}
}

func (e *IdempotentExecutor) Execute(ctx context.Context, run *JobRun) error {
	job, err := e.repo.FindJob(ctx, run.JobID())
	if err != nil {
		return fmt.Errorf("find job for execution: %w", err)
	}
	if !job.SideEffecting() {
		return e.next.Execute(ctx, run)
	}

	const handler = "task.default"
	acquired, err := e.store.Begin(ctx, run.IdempotencyKey(), handler, run.ID())
	if err != nil {
		return err
	}
	if !acquired {
		record, found, err := e.store.Lookup(ctx, run.IdempotencyKey())
		if err != nil {
			return err
		}
		if found && record.Status == "completed" {
			e.log.Info("duplicate job run detected",
				zap.String("job_run_id", run.ID().String()),
				zap.String("idempotency_key", run.IdempotencyKey()),
			)
			_ = e.repo.AppendEvent(ctx, NewRunEvent(
				run.TenantID(), run.JobID(), run.ID(), run.Status(),
				EventDuplicateDetected, map[string]any{"idempotency_key": run.IdempotencyKey()},
			))
			return nil
		}
		return ErrExecutionInProgress
	}

	if err := e.next.Execute(ctx, run); err != nil {
		return err
	}
	if err := e.store.Complete(ctx, run.IdempotencyKey(), ""); err != nil {
		return err
	}
	return nil
}

type StubExecutor struct {
	log      *zap.Logger
	failFunc func(*JobRun) error
}

func NewStubExecutor(log *zap.Logger) *StubExecutor {
	return &StubExecutor{log: log}
}

func (e *StubExecutor) SetFailFunc(fn func(*JobRun) error) {
	e.failFunc = fn
}

func (e *StubExecutor) Execute(ctx context.Context, r *JobRun) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.failFunc != nil {
		return e.failFunc(r)
	}
	e.log.Info("executed job run", zap.String("job_run_id", r.ID().String()))
	return nil
}
