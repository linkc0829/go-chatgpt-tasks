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
	quota.EXPECT().ReserveDailyCost(gomock.Any(), run.TenantID(), gomock.Any(), gomock.Any()).Return(true, nil)
	quota.EXPECT().AdjustDailyCost(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	client.EXPECT().Complete(gomock.Any(), gomock.Any()).Return(task.LLMResponse{Content: "not-json"}, nil).AnyTimes()
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
	quota.EXPECT().ReserveDailyCost(gomock.Any(), run.TenantID(), gomock.Any(), gomock.Any()).Return(true, nil)
	quota.EXPECT().AdjustDailyCost(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	client.EXPECT().Complete(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ task.LLMRequest) (task.LLMResponse, error) {
			<-ctx.Done()
			return task.LLMResponse{}, ctx.Err()
		},
	).AnyTimes()
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
	quota.EXPECT().ReserveDailyCost(gomock.Any(), run.TenantID(), gomock.Any(), gomock.Any()).Return(false, nil)
	expectEvent(t, repo, task.EventQuotaDeferred)

	err := exec.Execute(context.Background(), run)

	if !errors.Is(err, task.ErrLLMCostExceeded) {
		t.Errorf("Execute(over cost) error = %v, want ErrLLMCostExceeded", err)
	}
}

func TestLLMExecutor_RetriesWithinBudgetThenSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockRepo(ctrl)
	quota := mocks.NewMockQuotaRepo(ctrl)
	client := mocks.NewMockLLMClient(ctrl)
	job, run := newLLMExecution(t)
	policy := testLLMPolicy()
	policy.MaxRetries = 2
	exec := task.NewLLMExecutor(repo, quota, client, policy, nil)

	repo.EXPECT().FindJob(gomock.Any(), job.ID()).Return(job, nil)
	quota.EXPECT().Get(gomock.Any(), run.TenantID()).Return(testLLMQuota(), nil)
	perAttempt := task.EstimateCostCents("fake-default", policy.MaxInputTokens, policy.MaxOutputTokens)
	quota.EXPECT().ReserveDailyCost(gomock.Any(), run.TenantID(), perAttempt*3, testLLMQuota().MaxDailyLLMCostCents).Return(true, nil)
	quota.EXPECT().AdjustDailyCost(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	call := 0
	client.EXPECT().Complete(gomock.Any(), gomock.Any()).Times(3).DoAndReturn(
		func(context.Context, task.LLMRequest) (task.LLMResponse, error) {
			call++
			if call < 3 {
				return task.LLMResponse{Content: "not-json"}, nil
			}
			return task.LLMResponse{Content: `{}`}, nil
		},
	)
	// No terminal event: the budget absorbs the two failures and the run succeeds.
	if err := exec.Execute(context.Background(), run); err != nil {
		t.Errorf("Execute(retry then succeed) error = %v, want nil", err)
	}
}

func TestLLMExecutor_RetriesTransientClientError(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockRepo(ctrl)
	quota := mocks.NewMockQuotaRepo(ctrl)
	client := mocks.NewMockLLMClient(ctrl)
	job, run := newLLMExecution(t)
	policy := testLLMPolicy()
	policy.MaxRetries = 1
	exec := task.NewLLMExecutor(repo, quota, client, policy, nil)

	repo.EXPECT().FindJob(gomock.Any(), job.ID()).Return(job, nil)
	quota.EXPECT().Get(gomock.Any(), run.TenantID()).Return(testLLMQuota(), nil)
	quota.EXPECT().ReserveDailyCost(gomock.Any(), run.TenantID(), gomock.Any(), gomock.Any()).Return(true, nil)
	quota.EXPECT().AdjustDailyCost(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	gomock.InOrder(
		client.EXPECT().Complete(gomock.Any(), gomock.Any()).Return(task.LLMResponse{}, errors.New("rate limited")),
		client.EXPECT().Complete(gomock.Any(), gomock.Any()).Return(task.LLMResponse{Content: `{}`}, nil),
	)

	if err := exec.Execute(context.Background(), run); err != nil {
		t.Errorf("Execute(transient then success) error = %v, want nil", err)
	}
}

func TestValidateOutput(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		content string
		wantErr bool
	}{
		{"empty_schema_skips", "", "anything", false},
		{"required_field_present", `{"summary":null}`, `{"summary":"hi","extra":1}`, false},
		{"required_field_missing", `{"summary":null}`, `{"other":"x"}`, true},
		{"content_not_object", `{"summary":null}`, `["a"]`, true},
		{"invalid_schema", `not-json`, `{}`, true},
		{"json_schema_nested_valid", `{"type":"object","required":["result"],"properties":{"result":{"type":"object","required":["count"],"properties":{"count":{"type":"integer"}}}}}`, `{"result":{"count":2}}`, false},
		{"json_schema_nested_wrong_type", `{"type":"object","required":["result"],"properties":{"result":{"type":"object","required":["count"],"properties":{"count":{"type":"integer"}}}}}`, `{"result":{"count":"two"}}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := task.ValidateOutput(tt.schema, tt.content)
			if tt.wantErr && !errors.Is(err, task.ErrInvalidLLMOutput) {
				t.Errorf("ValidateOutput() error = %v, want ErrInvalidLLMOutput", err)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidateOutput() error = %v, want nil", err)
			}
		})
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
