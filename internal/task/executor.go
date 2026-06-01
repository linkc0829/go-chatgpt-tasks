package task

import (
	"context"

	"go.uber.org/zap"
)

type StubExecutor struct {
	log      *zap.Logger
	failFunc func(*JobRun) error
}

func NewStubExecutor(log *zap.Logger) *StubExecutor {
	return &StubExecutor{log: log}
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
