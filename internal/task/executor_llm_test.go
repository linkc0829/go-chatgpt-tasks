package task_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
	"github.com/linkc0829/go-chatgpt-tasks/internal/task"
	"github.com/linkc0829/go-chatgpt-tasks/internal/task/mocks"
)

func TestLLMExecutor_InvalidOutputEmitsValidationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockRepo(ctrl)
	quota := mocks.NewMockQuotaRepo(ctrl)
	client := mocks.NewMockLLMClient(ctrl)
	job, run := newLLMExecution(t)
	exec := task.NewLLMExecutor(repo, quota, client, testLLMPolicy(), nil)

	repo.EXPECT().FindJob(gomock.Any(), job.ID()).Return(job, nil)
	quota.EXPECT().Get(gomock.Any(), run.TenantID()).Return(testLLMQuota(), nil)
	client.EXPECT().Complete(gomock.Any(), gomock.Any()).Return(task.LLMResponse{Content: "not-json"}, nil)
	expectEvent(t, repo, task.EventLLMValidationFailed)

	err := exec.Execute(context.Background(), run)

	if !errors.Is(err, task.ErrInvalidLLMOutput) {
		t.Errorf("Execute(invalid output) error = %v, want ErrInvalidLLMOutput", err)
	}
	if got := run.Status(); got == task.StatusSuccess {
		t.Errorf("Execute(invalid output) status = %q, must not be success", got)
	}
}

func TestLLMExecutor_TimeoutEmitsTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockRepo(ctrl)
	quota := mocks.NewMockQuotaRepo(ctrl)
	client := mocks.NewMockLLMClient(ctrl)
	job, run := newLLMExecution(t)
	exec := task.NewLLMExecutor(repo, quota, client, testLLMPolicy(), nil)

	repo.EXPECT().FindJob(gomock.Any(), job.ID()).Return(job, nil)
	quota.EXPECT().Get(gomock.Any(), run.TenantID()).Return(testLLMQuota(), nil)
	client.EXPECT().Complete(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ task.LLMRequest) (task.LLMResponse, error) {
			<-ctx.Done()
			return task.LLMResponse{}, ctx.Err()
		},
	)
	expectEvent(t, repo, task.EventLLMTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := exec.Execute(ctx, run)

	if !errors.Is(err, task.ErrLLMTimeout) {
		t.Errorf("Execute(timeout) error = %v, want ErrLLMTimeout", err)
	}
}

func TestLLMExecutor_OverDailyCostDefersBeforeModelCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockRepo(ctrl)
	quota := mocks.NewMockQuotaRepo(ctrl)
	client := mocks.NewMockLLMClient(ctrl)
	job, run := newLLMExecution(t)
	policy := testLLMPolicy()
	policy.MaxInputTokens = 1001
	policy.MaxOutputTokens = 1000
	exec := task.NewLLMExecutor(repo, quota, client, policy, nil)

	repo.EXPECT().FindJob(gomock.Any(), job.ID()).Return(job, nil)
	quota.EXPECT().Get(gomock.Any(), run.TenantID()).Return(task.Quota{MaxDailyLLMCostCents: 1}, nil)
	expectEvent(t, repo, task.EventQuotaDeferred)

	err := exec.Execute(context.Background(), run)

	if !errors.Is(err, task.ErrLLMCostExceeded) {
		t.Errorf("Execute(over cost) error = %v, want ErrLLMCostExceeded", err)
	}
}

func newLLMExecution(t *testing.T) (*task.Job, *task.JobRun) {
	t.Helper()
	job, err := task.NewJob(shared.NewTenantID(), shared.NewUserID(), "return JSON", task.ScheduleSpec{
		Type:           task.KindOneOff,
		ScheduledAtUTC: time.Now().UTC(),
		TimezoneID:     "UTC",
		JobType:        task.JobTypeGenericLLM,
	})
	if err != nil {
		t.Fatalf("NewJob() error = %v", err)
	}
	run, err := task.NewJobRun(job.TenantID(), job.ID(), 1, time.Now().UTC())
	if err != nil {
		t.Fatalf("NewJobRun() error = %v", err)
	}
	return job, run
}

func testLLMPolicy() task.LLMPolicy {
	return task.LLMPolicy{
		TimeoutSeconds:  1,
		MaxRetries:      3,
		MaxInputTokens:  100,
		MaxOutputTokens: 100,
		MaxCostCents:    100,
		OutputSchema:    "{}",
	}
}

func testLLMQuota() task.Quota {
	return task.Quota{MaxDailyLLMCostCents: 100}
}

func expectEvent(t *testing.T, repo *mocks.MockRepo, want task.EventType) {
	t.Helper()
	repo.EXPECT().AppendEvent(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, event *task.RunEvent) error {
			if got := event.EventType(); got != want {
				t.Errorf("AppendEvent() event type = %q, want %q", got, want)
			}
			return nil
		},
	)
}
