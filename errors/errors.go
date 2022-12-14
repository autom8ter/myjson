package errors

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Code is a code associated with an error
type Code int

const (
	Internal     Code = http.StatusInternalServerError
	NotFound     Code = http.StatusNotFound
	Forbidden    Code = http.StatusForbidden
	Validation   Code = http.StatusBadRequest
	Unauthorized Code = http.StatusUnauthorized
)

// Error is a custom error
type Error struct {
	Code     Code     `json:"code"`
	Messages []string `json:"messages"`
	Err      error    `json:"err,omitempty"`
}

// Error returns the Error as a json string
func (e *Error) Error() string {
	bits, _ := json.Marshal(e)
	return string(bits)
}

// RemoveError removes the error from the Error and leaves it's messages and code
func (e *Error) RemoveError() *Error {
	return &Error{
		Code:     e.Code,
		Messages: e.Messages,
		Err:      nil,
	}
}

// Extract extracts the custom Error from the given error
func Extract(err error) *Error {
	e, ok := err.(*Error)
	if !ok {
		return &Error{
			Code:     0,
			Messages: nil,
			Err:      err,
		}
	}
	return e
}

// New creates a new error and returns it
func New(code Code, msg string, args ...any) error {
	e := &Error{
		Code: code,
		Err:  nil,
	}
	if msg != "" {
		e.Messages = append(e.Messages, fmt.Sprintf(msg, args...))
	}
	return e
}

// Wrap Wraps the given error and returns a new one. If the error is nil, it will return nil
func Wrap(err error, code Code, msg string, args ...any) error {
	if err == nil {
		return nil
	}
	e, ok := err.(*Error)
	if ok {
		if msg != "" {
			e.Messages = append(e.Messages, fmt.Sprintf(msg, args...))
		}
		if e.Err == nil {
			e.Err = err
		}
		if code > 0 {
			e.Code = code
		}
		return e
	} else {
		e = &Error{
			Code: code,
			Err:  err,
		}
		if msg != "" {
			e.Messages = append(e.Messages, fmt.Sprintf(msg, args...))
		}
		return e
	}
}
