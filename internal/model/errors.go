package model

import (
	"errors"
)

type FieldError struct {
	Field string
	Err   error
}

func (e *FieldError) Error() string {
	return e.Err.Error()
}

func (e *FieldError) Unwrap() error {
	return e.Err
}

func invalidField(field string, err error) error {
	return &FieldError{Field: field, Err: err}
}

var (
	// Check validation errors.
	ErrInvalidType        = errors.New("invalid check type: must be http, tcp, or dns")
	ErrIntervalTooShort   = errors.New("interval too short: minimum 10 seconds")
	ErrTimeoutOutOfRange  = errors.New("timeout out of range: must be between 1 and 30 seconds")
	ErrTimeoutGreaterOrEqualInterval = errors.New("timeout cannot be greater than or equal to interval")
	ErrInvalidStatusRange = errors.New("invalid status range: status_min must be >= 100, status_max <= 599")

	// Store errors.
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("conflict")
	ErrAlreadyExists = errors.New("already exists")
)
