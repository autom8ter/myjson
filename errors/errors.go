package errors

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Code int

const (
	Internal   Code = http.StatusInternalServerError
	NotFound   Code = http.StatusNotFound
	Forbidden  Code = http.StatusForbidden
	Validation Code = http.StatusBadRequest
)

// Error is a custom error
type Error struct {
	Code     Code     `json:"code"`
	Messages []string `json:"messages"`
	Err      error    `json:"err,omitempty"`
}

// Error returns the Error as a json string
func (e *Error) Error() string {
	if e.Code == 0 {
		e.Code = http.StatusOK
	}
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

// Wraps the given error and returns a new one
func Wrap(err error, code Code, msg string, args ...any) error {
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
