package task

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
)

var ErrExecutionInProgress = errors.New("execution already in progress")

type LLMExecutor struct {
	repo    Repo
	quota   QuotaRepo
	client  LLMClient
	policy  LLMPolicy
	metrics *Metrics
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
	attempts := e.policy.MaxRetries + 1
	if attempts < 1 {
		attempts = 1
	}
	// Reserve the worst-case budget (every attempt at max tokens) up front so
	// retries can never exceed the per-job or daily cap. This is intentionally
	// conservative: a job whose worst-case retry cost exceeds the limit is
	// deferred even if a single attempt would fit, and the full reservation is
	// held until the deferred reconcile releases the unused remainder.
	estimated := EstimateCostCents(model, e.policy.MaxInputTokens, e.policy.MaxOutputTokens) * attempts
	if e.policy.MaxCostCents >= 0 && estimated > e.policy.MaxCostCents {
		if eventErr := e.appendEvent(ctx, run, EventQuotaDeferred, map[string]any{"estimated_cost_cents": estimated}); eventErr != nil {
			return eventErr
		}
		return ErrLLMCostExceeded
	}
	quota, err := e.quota.Get(ctx, run.TenantID())
	if err != nil {
		return fmt.Errorf("get LLM quota: %w", err)
	}
	limit := quota.MaxDailyLLMCostCents
	committed, err := e.quota.ReserveDailyCost(ctx, run.TenantID(), estimated, limit)
	if err != nil {
		return fmt.Errorf("reserve LLM cost: %w", err)
	}
	if !committed {
		if eventErr := e.appendEvent(ctx, run, EventQuotaDeferred, map[string]any{"estimated_cost_cents": estimated}); eventErr != nil {
			return eventErr
		}
		return ErrLLMCostExceeded
	}
	// actualCost accumulates the real spend across attempts; reconcile the
	// single up-front reservation against it on a detached context so a
	// per-attempt timeout cannot skip the adjustment.
	actualCost := 0
	defer func() {
		if delta := actualCost - estimated; delta != 0 {
			_ = e.quota.AdjustDailyCost(context.WithoutCancel(ctx), run.TenantID(), delta)
		}
	}()

	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		response, err := e.callModel(ctx, model, job.Description())
		if errors.Is(err, context.DeadlineExceeded) {
			e.metrics.recordLLMTimeout()
			lastErr = ErrLLMTimeout
			continue
		}
		if err != nil {
			lastErr = fmt.Errorf("complete LLM request: %w", err)
			continue
		}
		actualCost += EstimateCostCents(model, response.InputTokens, response.OutputTokens)
		if verr := ValidateOutput(e.policy.OutputSchema, response.Content); verr != nil {
			e.metrics.recordLLMValidationFailure()
			lastErr = verr
			continue
		}
		e.metrics.recordLLMCost(actualCost)
		return nil
	}

	// Retry budget exhausted: emit the terminal event for the final failure mode.
	if errors.Is(lastErr, ErrLLMTimeout) {
		if err := e.appendEvent(ctx, run, EventLLMTimeout, nil); err != nil {
			return err
		}
		return ErrLLMTimeout
	}
	if errors.Is(lastErr, ErrInvalidLLMOutput) {
		if err := e.appendEvent(ctx, run, EventLLMValidationFailed, map[string]any{"error": lastErr.Error()}); err != nil {
			return err
		}
	}
	return lastErr
}

func (e *LLMExecutor) callModel(ctx context.Context, model, prompt string) (LLMResponse, error) {
	timeout := time.Duration(e.policy.TimeoutSeconds) * time.Second
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	started := time.Now()
	response, err := e.client.Complete(callCtx, LLMRequest{
		Model:           model,
		Prompt:          prompt,
		MaxInputTokens:  e.policy.MaxInputTokens,
		MaxOutputTokens: e.policy.MaxOutputTokens,
	})
	e.metrics.recordLLMLatency(time.Since(started))
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(callCtx.Err(), context.DeadlineExceeded) {
		return LLMResponse{}, context.DeadlineExceeded
	}
	return response, err
}

func (e *LLMExecutor) appendEvent(ctx context.Context, run *JobRun, eventType EventType, payload map[string]any) error {
	if err := e.repo.AppendEvent(ctx, NewRunEvent(run.TenantID(), run.JobID(), run.ID(), run.Status(), eventType, payload)); err != nil {
		return fmt.Errorf("append %s event: %w", eventType, err)
	}
	return nil
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
	if err := e.store.Complete(ctx, run.IdempotencyKey(), responseHash(run.IdempotencyKey())); err != nil {
		return err
	}
	return nil
}

// responseHash derives a deterministic, non-empty audit marker for a completed
// idempotency record. Handlers do not yet produce persisted output, so the key
// itself is the stable identifier hashed here.
//
// WARNING: this is NOT a fingerprint of the handler's actual output. A future
// response-caching slice must replace this with a hash of the real result
// before relying on idempotency_records.response_hash to return cached responses.
func responseHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
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
