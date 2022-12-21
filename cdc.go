package gokvkit

import (
	_ "embed"
	"time"
)

//go:embed cdc.yaml
var cdcSchema string

const cdcCollectionName = "cdc"

type JSONOp string

const (
	JSONOpRemove  JSONOp = "remove"
	JSONOpAdd     JSONOp = "add"
	JSONOpReplace JSONOp = "replace"
)

// JSONFieldOp
type JSONFieldOp struct {
	Path        string `json:"path"`
	Op          JSONOp `json:"op"`
	Value       any    `json:"value,omitempty"`
	BeforeValue any    `json:"beforeValue,omitempty"`
}

// CDC is a change data capture object used for tracking changes to documents over time
type CDC struct {
	ID         string        `json:"_id" validate:"required"`
	Collection string        `json:"collection" validate:"required"`
	Action     Action        `json:"action" validate:"required,oneof='create' 'update' 'delete' 'set'"`
	DocumentID string        `json:"documentID" validate:"required"`
	Document   *Document     `json:"document" validate:"required"`
	Diff       []JSONFieldOp `json:"diff,omitempty"`
	Timestamp  time.Time     `json:"timestamp" validate:"required"`
	Metadata   *Metadata     `json:"metadata" validate:"required"`
}

func cdcCollectionSchema() (CollectionSchema, error) {
	return newCollectionSchema([]byte(cdcSchema))
}
