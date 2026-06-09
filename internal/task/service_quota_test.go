package task_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
	"github.com/linkc0829/go-chatgpt-tasks/internal/task"
	"github.com/linkc0829/go-chatgpt-tasks/internal/task/mocks"
)

func TestService_CreateEnforcesTenantJobsPerHourQuota(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockRepo(ctrl)
	quotaRepo := mocks.NewMockQuotaRepo(ctrl)
	reg := prometheus.NewRegistry()
	metrics := task.NewMetrics(reg, zap.NewNop())
	svc := task.NewService(repo, quotaRepo, metrics)

	tenantA := task.Identity{TenantID: shared.NewTenantID(), UserID: shared.NewUserID()}
	tenantB := task.Identity{TenantID: shared.NewTenantID(), UserID: shared.NewUserID()}
	quota := task.Quota{
		MaxJobsPerHour:       1,
		MaxActiveRecurring:   1,
		MaxConcurrentRuns:    1,
		MaxDailyLLMCostCents: 1,
	}
	input := task.CreateInput{
		Description: "quota isolation",
		ScheduledAt: time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC),
	}

	quotaRepo.EXPECT().Get(gomock.Any(), tenantA.TenantID).Return(quota, nil)
	quotaRepo.EXPECT().CountJobsSince(gomock.Any(), tenantA.TenantID, gomock.Any()).Return(int64(1), nil)

	_, err := svc.Create(context.Background(), tenantA, input)

	require.Error(t, err)
	assert.ErrorIs(t, err, task.ErrQuotaExceeded)
	assert.Equal(t, float64(1), metricValue(t, reg, "task_quota_rejections_total"))

	quotaRepo.EXPECT().Get(gomock.Any(), tenantB.TenantID).Return(quota, nil)
	quotaRepo.EXPECT().CountJobsSince(gomock.Any(), tenantB.TenantID, gomock.Any()).Return(int64(0), nil)
	repo.EXPECT().SaveJob(gomock.Any(), gomock.Any()).Return(nil)
	repo.EXPECT().SaveRun(gomock.Any(), gomock.Any()).Return(nil)

	run, err := svc.Create(context.Background(), tenantB, input)

	require.NoError(t, err)
	assert.Equal(t, tenantB.TenantID, run.TenantID())
	assert.Equal(t, float64(1), metricValue(t, reg, "task_quota_rejections_total"))
}

func metricValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		require.Len(t, family.Metric, 1)
		if family.GetType() == dto.MetricType_COUNTER {
			return family.Metric[0].Counter.GetValue()
		}
	}
	require.Fail(t, "metric not found", name)
	return 0
}
