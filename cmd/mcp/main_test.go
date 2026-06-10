package main

import (
	"testing"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	taskmcp "github.com/linkc0829/go-chatgpt-tasks/internal/task/mcp"
)

func TestBindRegistryAcceptsToolSchemas(t *testing.T) {
	server := sdkmcp.NewServer(
		&sdkmcp.Implementation{Name: "test", Version: "test"},
		nil,
	)

	bindRegistry(server, taskmcp.NewRegistry())
}
