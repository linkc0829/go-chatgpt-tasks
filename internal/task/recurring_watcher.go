package task

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type RecurringWatcher struct {
	repo     Repo
	interval time.Duration
	lookback time.Duration
	limit    int32
	log      *zap.Logger
}

func NewRecurringWatcher(repo Repo, interval time.Duration, log *zap.Logger) *RecurringWatcher {
	return &RecurringWatcher{
		repo:     repo,
		interval: interval,
		lookback: time.Hour,
		limit:    100,
		log:      log,
	}
}

func (rw *RecurringWatcher) Run(ctx context.Context) error {
	t := time.NewTicker(rw.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			rw.scanOnce(ctx)
		}
	}
}

func (rw *RecurringWatcher) scanOnce(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	specs, err := rw.repo.FindTerminalRecurringRuns(cctx, time.Now().UTC().Add(-rw.lookback), rw.limit)
	if err != nil {
		rw.log.Error("recurring watcher find terminal runs", zap.Error(err))
		return
	}

	for _, spec := range specs {
		rule, err := ParseRule(spec.RecurrenceRule)
		if err != nil {
			rw.log.Error("recurring watcher parse rule", zap.Error(err))
			continue
		}
		tz, err := time.LoadLocation(spec.TimezoneID)
		if err != nil {
			rw.log.Error("recurring watcher load timezone", zap.Error(err))
			continue
		}
		scheduledAt, note, err := NextOccurrence(rule, spec.LocalTime, tz, spec.ScheduledAt)
		if err != nil {
			rw.log.Error("recurring watcher compute next occurrence", zap.Error(err))
			continue
		}
		next, err := NewJobRun(spec.TenantID, spec.JobID, spec.Sequence+1, scheduledAt)
		if err != nil {
			rw.log.Error("recurring watcher build next run", zap.Error(err))
			continue
		}
		created, err := rw.repo.InsertRunIfAbsent(cctx, next)
		if err != nil {
			rw.log.Error("recurring watcher insert next run", zap.Error(err))
			continue
		}
		if created {
			if note != "" {
				_ = rw.repo.AppendEvent(cctx, NewRunEvent(
					next.TenantID(),
					next.JobID(),
					next.ID(),
					next.Status(),
					EventJobRunCreated,
					map[string]any{"dst_note": string(note)},
				))
			}
			rw.log.Info(
				"recurring watcher created next run",
				zap.String("job_id", spec.JobID.String()),
				zap.Int("sequence", next.Sequence()),
			)
		}
	}
}
