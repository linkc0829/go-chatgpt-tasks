package task

import "errors"

var (
	ErrJobNotFound             = errors.New("job not found")
	ErrJobRunNotFound          = errors.New("job run not found")
	ErrInvalidDescription      = errors.New("invalid description")
	ErrInvalidSchedule         = errors.New("invalid schedule")
	ErrInvalidOwner            = errors.New("invalid owner")
	ErrInvalidStatusTransition = errors.New("invalid status transition")
	ErrQuotaExceeded           = errors.New("tenant quota exceeded")
	ErrInvalidTimezone         = errors.New("invalid timezone")
	ErrInvalidRecurrence       = errors.New("invalid recurrence")
	ErrInvalidJobType          = errors.New("invalid job type")
	ErrInvalidLLMOutput        = errors.New("invalid LLM output")
	ErrLLMTimeout              = errors.New("LLM request timed out")
	ErrLLMCostExceeded         = errors.New("LLM cost budget exceeded")
)
