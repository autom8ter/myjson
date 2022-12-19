package model

import (
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
	Collection string    `json:"collection" validate:"required"`
	Action     Action    `json:"action" validate:"required"`
	Document   *Document `json:"document" validate:"required"`
	Timestamp  time.Time `json:"timestamp" validate:"required"`
	Metadata   *Metadata `json:"metadata" validate:"required"`
}
