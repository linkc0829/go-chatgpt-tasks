package task

import "errors"

var (
	ErrJobNotFound             = errors.New("job not found")
	ErrJobRunNotFound          = errors.New("job run not found")
	ErrInvalidDescription      = errors.New("invalid description")
	ErrInvalidSchedule         = errors.New("invalid schedule")
	ErrInvalidStatusTransition = errors.New("invalid status transition")
)
