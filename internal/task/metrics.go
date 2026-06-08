package task

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	runs *prometheus.CounterVec
	dur  prometheus.Histogram
	dlq  prometheus.Counter
}

func NewMetrics(reg *prometheus.Registry) *Metrics {
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
	}
	reg.MustRegister(m.runs, m.dur, m.dlq)
	return m
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
