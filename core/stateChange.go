package core

import (
	"context"
	"time"
)

type Action string

const (
	Set    = "set"
	Update = "update"
	Delete = "delete"
)

type StateChange struct {
	ctx        context.Context
	Collection string                    `json:"collection,omitempty"`
	Deletes    []string                  `json:"deletes,omitempty"`
	Sets       []*Document               `json:"sets,omitempty"`
	Updates    map[string]map[string]any `json:"updates,omitempty"`
	Timestamp  time.Time                 `json:"timestamp,omitempty"`
}

// Context returns the context of the operation that triggered the state change
func (s StateChange) Context() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

// ChangeStreamHandler is a function executed on changes to documents which emit events
type ChangeStreamHandler func(ctx context.Context, change StateChange) error
