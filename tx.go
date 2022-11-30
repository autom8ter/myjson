package gokvkit

import (
	"context"
	"github.com/autom8ter/gokvkit/kv"
)

// Tx is a database transaction interface
type Tx interface {
	Get(ctx context.Context, collection, id string) (*Document, error)
	// Set sets the specified key/value in the database
	Set(ctx context.Context, collection string, document *Document) error
	// Delete deletes the specified key from the database
	Delete(ctx context.Context, collection string, id string) error
}

type transaction struct {
	db *DB
	tx kv.Tx
}
