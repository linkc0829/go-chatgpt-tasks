package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

var ErrExecutionInProgress = errors.New("execution already in progress")

type dailyCostCounter struct {
	mu     sync.Mutex
	totals map[string]int
}

func (c *dailyCostCounter) reserve(tenantID string, cost, limit int) (func(int), bool) {
	key := tenantID + ":" + time.Now().UTC().Format(time.DateOnly)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.totals == nil {
		c.totals = make(map[string]int)
	}
	if limit >= 0 && c.totals[key]+cost > limit {
		return nil, false
	}
	c.totals[key] += cost
	return func(actual int) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.totals[key] += actual - cost
	}, true
}

type LLMExecutor struct {
	repo    Repo
	quota   QuotaRepo
	client  LLMClient
	policy  LLMPolicy
	metrics *Metrics
	costs   dailyCostCounter
}

func NewLLMExecutor(repo Repo, quota QuotaRepo, client LLMClient, policy LLMPolicy, metrics *Metrics) *LLMExecutor {
	return &LLMExecutor{repo: repo, quota: quota, client: client, policy: policy, metrics: metrics}
}

func (e *LLMExecutor) Execute(ctx context.Context, run *JobRun) error {
	job, err := e.repo.FindJob(ctx, run.JobID())
	if err != nil {
		return fmt.Errorf("find job for LLM execution: %w", err)
	}
	if job.JobType() != JobTypeGenericLLM {
		return ErrInvalidJobType
	}

	const model = "fake-default"
	estimated := EstimateCostCents(model, e.policy.MaxInputTokens, e.policy.MaxOutputTokens)
	quota, err := e.quota.Get(ctx, run.TenantID())
	if err != nil {
		return fmt.Errorf("get LLM quota: %w", err)
	}
	limit := quota.MaxDailyLLMCostCents
	if e.policy.MaxCostCents >= 0 && e.policy.MaxCostCents < limit {
		limit = e.policy.MaxCostCents
	}
	finalize, ok := e.costs.reserve(run.TenantID().String(), estimated, limit)
	if !ok {
		e.appendEvent(ctx, run, EventQuotaDeferred, map[string]any{"estimated_cost_cents": estimated})
		return ErrLLMCostExceeded
	}
	actualCost := 0
	defer func() { finalize(actualCost) }()

	timeout := time.Duration(e.policy.TimeoutSeconds) * time.Second
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	started := time.Now()
	response, err := e.client.Complete(callCtx, LLMRequest{
		Model:           model,
		Prompt:          job.Description(),
		MaxInputTokens:  e.policy.MaxInputTokens,
		MaxOutputTokens: e.policy.MaxOutputTokens,
	})
	e.metrics.recordLLMLatency(time.Since(started))
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(callCtx.Err(), context.DeadlineExceeded) {
		e.metrics.recordLLMTimeout()
		e.appendEvent(ctx, run, EventLLMTimeout, nil)
		return ErrLLMTimeout
	}
	if err != nil {
		return fmt.Errorf("complete LLM request: %w", err)
	}
	if err := ValidateOutput(e.policy.OutputSchema, response.Content); err != nil {
		e.metrics.recordLLMValidationFailure()
		e.appendEvent(ctx, run, EventLLMValidationFailed, map[string]any{"error": err.Error()})
		return err
	}
	actualCost = EstimateCostCents(model, response.InputTokens, response.OutputTokens)
	e.metrics.recordLLMCost(actualCost)
	return nil
}

func (e *LLMExecutor) appendEvent(ctx context.Context, run *JobRun, eventType EventType, payload map[string]any) {
	_ = e.repo.AppendEvent(ctx, NewRunEvent(run.TenantID(), run.JobID(), run.ID(), run.Status(), eventType, payload))
}

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
