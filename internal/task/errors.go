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
)
