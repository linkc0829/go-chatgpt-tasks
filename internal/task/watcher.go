package task

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type Runner interface {
	Run(ctx context.Context) error
}

type Watcher struct {
	repo     Repo
	queue    Queue
	interval time.Duration
	horizon  time.Duration
	limit    int32
	log      *zap.Logger
}

func NewWatcher(repo Repo, queue Queue, interval time.Duration, log *zap.Logger) *Watcher {
	return &Watcher{
		repo:     repo,
		queue:    queue,
		interval: interval,
		horizon:  5 * time.Minute,
		limit:    100,
		log:      log,
	}
}

func (w *Watcher) Run(ctx context.Context) error {
	t := time.NewTicker(w.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			w.scanOnce(ctx)
		}
	}
}

func (w *Watcher) scanOnce(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	now := time.Now().UTC()
	before := now.Add(w.horizon)
	for _, bucket := range bucketsInRange(now, before) {
		runs, err := w.repo.FindDueRuns(cctx, bucket, before, w.limit)
		if err != nil {
			w.log.Error("watcher find due", zap.Error(err))
			continue
		}

		for _, run := range runs {
			if err := w.queue.Enqueue(cctx, JobRunMsg{
				JobRunID: run.ID().String(),
				Attempts: run.Attempts(),
			}); err != nil {
				w.log.Error("watcher enqueue", zap.Error(err))
				continue
			}

			if err := run.MarkQueued(); err != nil {
				continue
			}
			if err := w.repo.UpdateRunStatus(cctx, run); err != nil {
				w.log.Error("watcher mark queued", zap.Error(err))
			}
		}
	}
}

func bucketsInRange(from, to time.Time) []int64 {
	from = from.UTC().Truncate(time.Hour)
	to = to.UTC().Truncate(time.Hour)
	if to.Before(from) {
		return []int64{from.Unix()}
	}

	buckets := make([]int64, 0, int(to.Sub(from)/time.Hour)+1)
	for t := from; !t.After(to); t = t.Add(time.Hour) {
		buckets = append(buckets, t.Unix())
	}
	return buckets
}
