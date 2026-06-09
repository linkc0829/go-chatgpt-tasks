package task

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

type Metrics struct {
	runs                  *prometheus.CounterVec
	dur                   prometheus.Histogram
	dlq                   prometheus.Counter
	quotaRejections       prometheus.Counter
	llmLatency            prometheus.Histogram
	llmTimeouts           prometheus.Counter
	llmValidationFailures prometheus.Counter
	llmCostCents          prometheus.Counter
	logger                *zap.Logger
}

func NewMetrics(reg *prometheus.Registry, logger *zap.Logger) *Metrics {
	m := &Metrics{
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_runs_total",
			Help: "Total task runs by terminal or retry status.",
		}, []string{"status"}),
		dur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "task_run_duration_seconds",
			Help:    "Task run execution duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		dlq: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_dlq_total",
			Help: "Total task runs sent to the dead-letter queue.",
		}),
		quotaRejections: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_quota_rejections_total",
			Help: "Total task creation requests rejected by tenant quotas.",
		}),
		llmLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "task_llm_latency_seconds",
			Help: "LLM request latency in seconds.",
		}),
		llmTimeouts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_llm_timeouts_total",
			Help: "Total timed out LLM requests.",
		}),
		llmValidationFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_llm_validation_failures_total",
			Help: "Total LLM responses rejected by output validation.",
		}),
		llmCostCents: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_llm_cost_cents",
			Help: "Total estimated LLM cost in cents.",
		}),
		logger: logger,
	}
	reg.MustRegister(m.runs, m.dur, m.dlq, m.quotaRejections, m.llmLatency, m.llmTimeouts, m.llmValidationFailures, m.llmCostCents)
	return m
}

func (m *Metrics) recordLLMLatency(d time.Duration) {
	if m != nil {
		m.llmLatency.Observe(d.Seconds())
	}
}

func (m *Metrics) recordLLMTimeout() {
	if m != nil {
		m.llmTimeouts.Inc()
	}
}

func (m *Metrics) recordLLMValidationFailure() {
	if m != nil {
		m.llmValidationFailures.Inc()
	}
}

func (m *Metrics) recordLLMCost(cost int) {
	if m != nil {
		m.llmCostCents.Add(float64(cost))
	}
}

func (m *Metrics) recordRun(run *JobRun) {
	if m == nil {
		return
	}
	m.runs.WithLabelValues(string(run.Status())).Inc()
	if !run.StartedAt().IsZero() && !run.CompletedAt().IsZero() {
		m.dur.Observe(run.CompletedAt().Sub(run.StartedAt()).Seconds())
	}
	if !run.StartedAt().IsZero() && !run.FailedAt().IsZero() && run.Status() == StatusFailed {
		m.dur.Observe(run.FailedAt().Sub(run.StartedAt()).Seconds())
	}
}

func (m *Metrics) recordDLQ() {
	if m == nil {
		return
	}
	m.dlq.Inc()
}

func (m *Metrics) RecordQuotaRejection(tenantID shared.TenantID, reason string) {
	if m == nil {
		return
	}
	m.quotaRejections.Inc()
	if m.logger != nil {
		m.logger.Warn("task creation rejected by tenant quota",
			zap.String("tenant_id", tenantID.String()),
			zap.String("reason", reason),
		)
	}
}
