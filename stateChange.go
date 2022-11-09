package brutus

import (
	"context"
	"time"
)

// Action
type Action string

const (
	// Create creates a document
	Create = "create"
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
	Creates    []*Document               `json:"creates,omitempty"`
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

// DocChange is a mutation to a single document - it includes the action, the document id, and the before & after state of the document
// Note: the after value is what's persisted to the database, the before value is what was in the database prior to the change.
// After will be always null on delete
type DocChange struct {
	Action Action
	DocID  string
	Before *Document
	After  *Document
}
