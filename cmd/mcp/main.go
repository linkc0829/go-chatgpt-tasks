package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/linkc0829/go-chatgpt-tasks/internal/platform/config"
	"github.com/linkc0829/go-chatgpt-tasks/internal/platform/postgres"
	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
	"github.com/linkc0829/go-chatgpt-tasks/internal/task"
	taskmcp "github.com/linkc0829/go-chatgpt-tasks/internal/task/mcp"
)

func main() {
	cfg, err := config.LoadMCP()
	if err != nil {
		cfg, err = loadLocalMCPConfig(err)
		if err != nil {
			log.Fatalf("config: %v", err)
		}
	}

	ctx := context.Background()
	pool, err := postgres.New(ctx, postgres.Config{
		DSN:      cfg.DB.DSN,
		MaxConns: cfg.DB.MaxConns,
		MinConns: cfg.DB.MinConns,
	})
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()

	svc := task.NewService(task.NewPostgresRepo(pool))
	ident, err := mcpIdentity()
	if err != nil {
		log.Fatalf("mcp identity: %v", err)
	}
	reg := taskmcp.NewRegistry()
	taskmcp.Register(reg, svc, ident)

	server := sdkmcp.NewServer(
		&sdkmcp.Implementation{Name: "task-scheduler", Version: "0.1.0"},
		nil,
	)
	bindRegistry(server, reg)

	if err := server.Run(ctx, &sdkmcp.StdioTransport{}); err != nil && !isNormalStdioClose(err) {
		log.Fatalf("mcp serve: %v", err)
	}
}

func mcpIdentity() (task.Identity, error) {
	tenantRaw := os.Getenv("MCP_TENANT_ID")
	if tenantRaw == "" {
		tenantRaw = "00000000-0000-0000-0000-0000000000cc"
	}
	userRaw := os.Getenv("MCP_USER_ID")
	if userRaw == "" {
		userRaw = "00000000-0000-0000-0000-0000000000dd"
	}

	tenantID, err := shared.ParseTenantID(tenantRaw)
	if err != nil {
		return task.Identity{}, fmt.Errorf("parse MCP_TENANT_ID: %w", err)
	}
	userID, err := shared.ParseUserID(userRaw)
	if err != nil {
		return task.Identity{}, fmt.Errorf("parse MCP_USER_ID: %w", err)
	}
	return task.Identity{TenantID: tenantID, UserID: userID}, nil
}

func loadLocalMCPConfig(loadErr error) (*config.Config, error) {
	if !strings.Contains(loadErr.Error(), "POSTGRES_DSN is required") {
		return nil, loadErr
	}

	return &config.Config{
		DB: config.DBConfig{
			DSN:      "postgres://postgres:pgadmin@localhost:5432/chatpgt-tasks?sslmode=disable",
			MaxConns: 20,
			MinConns: 2,
		},
	}, nil
}

func isNormalStdioClose(err error) bool {
	return errors.Is(err, io.EOF) || strings.Contains(err.Error(), "EOF")
}

func bindRegistry(s *sdkmcp.Server, reg *taskmcp.Registry) {
	handlers := reg.Handlers()

	sdkmcp.AddTool(s, &sdkmcp.Tool{Name: "task.create", Description: "Create a scheduled task run."},
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in taskCreateInput) (*sdkmcp.CallToolResult, map[string]any, error) {
			return callRegistryTool(ctx, handlers["task.create"], in)
		})

	sdkmcp.AddTool(s, &sdkmcp.Tool{Name: "task.list", Description: "List scheduled task runs."},
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in taskListInput) (*sdkmcp.CallToolResult, map[string]any, error) {
			return callRegistryTool(ctx, handlers["task.list"], in)
		})

	sdkmcp.AddTool(s, &sdkmcp.Tool{Name: "task.status", Description: "Get a scheduled task run status."},
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in taskRunRefInput) (*sdkmcp.CallToolResult, map[string]any, error) {
			return callRegistryTool(ctx, handlers["task.status"], in)
		})

	sdkmcp.AddTool(s, &sdkmcp.Tool{Name: "task.cancel", Description: "Cancel a scheduled task run."},
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in taskRunRefInput) (*sdkmcp.CallToolResult, map[string]any, error) {
			return callRegistryTool(ctx, handlers["task.cancel"], in)
		})
}

type taskCreateInput struct {
	Description             string `json:"description" jsonschema:"Task description"`
	ScheduledAt             string `json:"scheduled_at" jsonschema:"RFC3339 scheduled time, for example 2025-01-01T00:00:00Z"`
	RecurringIntervalSecond int64  `json:"recurring_interval_seconds,omitempty" jsonschema:"Optional recurring interval in seconds"`
}

type taskListInput struct {
	Limit  int `json:"limit,omitempty" jsonschema:"Page size, default 20"`
	Offset int `json:"offset,omitempty" jsonschema:"Page offset, default 0"`
}

type taskRunRefInput struct {
	JobID string `json:"job_id" jsonschema:"Job run ID returned by task.create or task.list"`
}

func callRegistryTool(
	ctx context.Context,
	handler taskmcp.ToolHandler,
	in any,
) (*sdkmcp.CallToolResult, map[string]any, error) {
	if handler == nil {
		return nil, nil, fmt.Errorf("tool handler not registered")
	}

	raw, err := json.Marshal(in)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal args: %w", err)
	}

	out, err := handler(ctx, raw)
	if err != nil {
		return nil, nil, err
	}

	result, err := toMap(out)
	if err != nil {
		return nil, nil, err
	}
	return nil, result, nil
}

func toMap(v any) (map[string]any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("decode result: %w", err)
	}
	return out, nil
}
