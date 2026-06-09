package task

import (
	"encoding/json"
	"fmt"
)

type LLMRequest struct {
	Model           string
	Prompt          string
	MaxInputTokens  int
	MaxOutputTokens int
}

type LLMResponse struct {
	Content      string
	InputTokens  int
	OutputTokens int
}

type LLMPolicy struct {
	TimeoutSeconds  int
	MaxRetries      int
	MaxInputTokens  int
	MaxOutputTokens int
	MaxCostCents    int
	OutputSchema    string
}

func ValidateOutput(schema, content string) error {
	if schema == "" {
		return nil
	}
	if !json.Valid([]byte(schema)) {
		return fmt.Errorf("%w: invalid configured schema", ErrInvalidLLMOutput)
	}
	if !json.Valid([]byte(content)) {
		return fmt.Errorf("%w: response is not valid JSON", ErrInvalidLLMOutput)
	}
	return nil
}

func EstimateCostCents(_ string, inputTokens, outputTokens int) int {
	total := inputTokens + outputTokens
	if total <= 0 {
		return 0
	}
	return (total + 999) / 1000
}
