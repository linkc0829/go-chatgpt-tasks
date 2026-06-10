package config

import (
	"strings"
	"testing"
)

func TestConfigValidateRejectsInvalidWorkerCount(t *testing.T) {
	cfg := Config{
		DB:   DBConfig{DSN: "postgres://test"},
		Task: TaskConfig{WorkerCount: 0},
	}

	err := cfg.validate(false)

	if err == nil || !strings.Contains(err.Error(), "TASK_WORKER_COUNT") {
		t.Errorf("Config.validate(worker_count=0) error = %v, want TASK_WORKER_COUNT validation error", err)
	}
}
