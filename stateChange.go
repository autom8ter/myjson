package wolverine

import (
	"context"
	"time"
)

// Action
type Action string

const (
	// Set sets a document's values in place
	Set = "set"
	// Update updates a set of fields on a document
	Update = "update"
	// Delete deletes a document
	Delete = "delete"
)

// StateChange is a mutation to a set of documents
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
