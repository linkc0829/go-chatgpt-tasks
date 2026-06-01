package mcp

import (
	"context"
	"encoding/json"
)

type ToolHandler func(ctx context.Context, raw json.RawMessage) (any, error)

type Registry struct {
	handlers map[string]ToolHandler
}

func NewRegistry() *Registry {
	return &Registry{handlers: map[string]ToolHandler{}}
}

func (r *Registry) Register(name string, h ToolHandler) {
	r.handlers[name] = h
}

func (r *Registry) Handlers() map[string]ToolHandler {
	return r.handlers
}
