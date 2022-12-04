package model

import (
	"github.com/palantir/stacktrace"
	"time"
)

// Action is an action that causes a mutation to the database
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

// Command is a command executed against the database that causes a change in state
type Command struct {
	// Target collection
	Collection string `json:"collection"`
	// Action taking place
	Action Action `json:"action"`
	// DocID is the document being changed
	DocID string `json:"docID"`
	// Before is the document before the change
	Before *Document `json:"before"`
	// After is the value after the change
	After *Document `json:"after"`
	// Timestamp is the timestamp of the change
	Timestamp time.Time `json:"timestamp"`
	// Metadata is the context metadata at the time of the commands execution
	Metadata *Metadata `json:"metadata"`
}

func (c *Command) Validate() error {
	if c.Collection == "" {
		return stacktrace.NewError("command: empty command.collection")
	}
	if c.Metadata == nil {
		return stacktrace.NewError("command: empty command.metadata")
	}
	if c.Timestamp.IsZero() {
		return stacktrace.NewError("command: empty command.timestamp")
	}
	if c.DocID == "" {
		return stacktrace.NewError("command: empty command.docID")
	}
	switch c.Action {
	case Set, Update, Create:
		if c.After == nil {
			return stacktrace.NewError("command: empty command.change")
		}
	case Delete:
		if c.Before == nil {
			return stacktrace.NewError("command: empty command.before")
		}
	default:
		return stacktrace.NewError("command: unsupported command.action: %s", c.Action)
	}

	return nil
}
