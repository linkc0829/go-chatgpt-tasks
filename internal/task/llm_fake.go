package task

import "context"

type FakeLLMClient struct {
	Response LLMResponse
	Err      error
}

func NewFakeLLMClient() *FakeLLMClient {
	return &FakeLLMClient{Response: LLMResponse{Content: "{}"}}
}

func (c *FakeLLMClient) Complete(ctx context.Context, _ LLMRequest) (LLMResponse, error) {
	if err := ctx.Err(); err != nil {
		return LLMResponse{}, err
	}
	return c.Response, c.Err
}
