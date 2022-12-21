package gokvkit

import (
	_ "embed"
	"time"
)

//go:embed cdc.yaml
var cdcSchema string

// FieldOp is the type of change made to a json field
type FieldOp string

const (
	// Replace indicates that a field value was replaced
	Replace FieldOp = "replace"
	// Add indicates that a field value was added
	Add FieldOp = "add"
	// Remove indicates that a field value was removed
	Remove FieldOp = "remove"
)

// FieldChange is a change to a json field
type FieldChange struct {
	Op          FieldOp `json:"op" validate:"required,oneof='replace' 'add' 'remove'"`
	Path        string  `json:"path" validate:"required"`
	Value       any     `json:"value,omitempty"`
	ValueBefore any     `json:"valueBefore,omitempty"`
}

// CDC is a change data capture object used for tracking changes to documents over time
type CDC struct {
	Collection string        `json:"collection" validate:"required"`
	Action     Action        `json:"action" validate:"required,oneof='create' 'update' 'delete' 'set'"`
	DocumentID string        `json:"documentID" validate:"required"`
	Document   *Document     `json:"document" validate:"required"`
	Diff       []FieldChange `json:"diff" validate:"required"`
	Timestamp  time.Time     `json:"timestamp" validate:"required"`
	Metadata   *Metadata     `json:"metadata" validate:"required"`
}

func cdcCollectionSchema() (CollectionSchema, error) {
	return newCollectionSchema([]byte(cdcSchema))
}
