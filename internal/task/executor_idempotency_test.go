package task_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
	"github.com/linkc0829/go-chatgpt-tasks/internal/task"
	"github.com/linkc0829/go-chatgpt-tasks/internal/task/mocks"
)

func TestIdempotentExecutor_DuplicateSideEffectRunsOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockRepo(ctrl)
	store := mocks.NewMockIdempotencyStore(ctrl)
	handler := mocks.NewMockExecutor(ctrl)
	exec := task.NewIdempotentExecutor(repo, store, handler, zap.NewNop())

	job, err := task.NewJob(
		shared.NewTenantID(),
		shared.NewUserID(),
		"side effect",
		task.ScheduleSpec{
			Type:             task.KindOneOff,
			ScheduledAtUTC:   time.Now().UTC(),
			TimezoneID:       "UTC",
			SideEffecting:    true,
			IdempotencyScope: "job_run",
		},
	)
	require.NoError(t, err)
	run, err := task.NewJobRun(job.TenantID(), job.ID(), 1, time.Now().UTC())
	require.NoError(t, err)

	repo.EXPECT().FindJob(gomock.Any(), job.ID()).Return(job, nil).Times(2)
	store.EXPECT().Begin(gomock.Any(), run.IdempotencyKey(), "task.default", run.ID()).Return(true, nil)
	handler.EXPECT().Execute(gomock.Any(), run).Return(nil).Times(1)
	store.EXPECT().Complete(gomock.Any(), run.IdempotencyKey(), gomock.Any()).Return(nil)

	store.EXPECT().Begin(gomock.Any(), run.IdempotencyKey(), "task.default", run.ID()).Return(false, nil)
	store.EXPECT().Lookup(gomock.Any(), run.IdempotencyKey()).Return(task.IdempotencyRecord{
		Key:    run.IdempotencyKey(),
		Status: "completed",
	}, true, nil)
	repo.EXPECT().AppendEvent(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, event *task.RunEvent) error {
			assert.Equal(t, task.EventDuplicateDetected, event.EventType())
			assert.Equal(t, run.IdempotencyKey(), event.Payload()["idempotency_key"])
			return nil
		},
	)

	require.NoError(t, exec.Execute(context.Background(), run))
	require.NoError(t, exec.Execute(context.Background(), run))
}
