package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/linkc0829/go-backend-template/internal/platform/config"
	"github.com/linkc0829/go-backend-template/internal/platform/postgres"
	"github.com/linkc0829/go-backend-template/internal/task"
	taskmcp "github.com/linkc0829/go-backend-template/internal/task/mcp"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
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
	reg := taskmcp.NewRegistry()
	taskmcp.Register(reg, svc)

	server := sdkmcp.NewServer(
		&sdkmcp.Implementation{Name: "task-scheduler", Version: "0.1.0"},
		nil,
	)
	bindRegistry(server, reg)

	if err := server.Run(ctx, &sdkmcp.StdioTransport{}); err != nil {
		log.Fatalf("mcp serve: %v", err)
	}
}

func bindRegistry(s *sdkmcp.Server, reg *taskmcp.Registry) {
	descriptions := map[string]string{
		"task.create": "Create a scheduled task run.",
		"task.list":   "List scheduled task runs.",
		"task.status": "Get a scheduled task run status.",
		"task.cancel": "Cancel a scheduled task run.",
	}

	for name, h := range reg.Handlers() {
		handler := h
		sdkmcp.AddTool(s, &sdkmcp.Tool{Name: name, Description: descriptions[name]},
			func(ctx context.Context, _ *sdkmcp.CallToolRequest, in map[string]any) (*sdkmcp.CallToolResult, map[string]any, error) {
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
			})
	}
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
