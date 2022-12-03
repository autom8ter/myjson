package gokvkit

import (
	"context"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/palantir/stacktrace"
	"github.com/segmentio/ksuid"
	"time"
)

// Tx is a database transaction interface
type Tx interface {
	// Create creates a new document - if the documents primary key is unset, it will be set as a sortable unique id
	Create(ctx context.Context, collection string, document *Document) (string, error)
	// Update updates a value in the database
	Update(ctx context.Context, collection, id string, document map[string]any) error
	// Set sets the specified key/value in the database
	Set(ctx context.Context, collection string, document *Document) error
	// Delete deletes the specified key from the database
	Delete(ctx context.Context, collection string, id string) error
	// Commit commits the transaction to the database
	Commit(ctx context.Context) error
	// Rollback rollsback all changes made to the datbase
	Rollback(ctx context.Context)
}

// TxFunc is a function executed against a transaction - if the function returns an error, all changes will be rolled back.
// Otherwise, the changes will be commited to the database
type TxFunc func(ctx context.Context, tx Tx) error

type transaction struct {
	db       *DB
	commands []*Command
}

func (t *transaction) Commit(ctx context.Context) error {
	if len(t.commands) >= batchThreshold {
		batch := t.db.kv.Batch()
		if err := t.db.persistStateChange(ctx, batch, t.commands); err != nil {
			return stacktrace.Propagate(err, "")
		}
		return stacktrace.Propagate(batch.Flush(), "")
	}
	if err := t.db.kv.Tx(true, func(tx kv.Tx) error {
		if err := t.db.persistStateChange(ctx, tx, t.commands); err != nil {
			return stacktrace.Propagate(err, "")
		}
		return nil
	}); err != nil {
		return stacktrace.Propagate(err, "")
	}
	return nil
}

func (t *transaction) Rollback(ctx context.Context) {
	t.commands = []*Command{}
}

func (t *transaction) Update(ctx context.Context, collection string, id string, update map[string]any) error {
	if !t.db.hasCollection(collection) {
		return stacktrace.NewError("unsupported collection: %s", collection)
	}
	doc := NewDocument()
	if err := doc.SetAll(update); err != nil {
		return stacktrace.Propagate(err, "")
	}
	md, _ := GetMetadata(ctx)
	t.commands = append(t.commands, &Command{
		Collection: collection,
		Action:     UpdateDocument,
		DocID:      id,
		Change:     doc,
		Timestamp:  time.Now(),
		Metadata:   md,
	})
	return nil
}

func (t *transaction) Create(ctx context.Context, collection string, document *Document) (string, error) {
	if !t.db.hasCollection(collection) {
		return "", stacktrace.NewError("unsupported collection: %s", collection)
	}
	if t.db.getPrimaryKey(collection, document) == "" {
		id := ksuid.New().String()
		err := t.db.setPrimaryKey(collection, document, id)
		if err != nil {
			return "", stacktrace.Propagate(err, "")
		}
	}

	md, _ := GetMetadata(ctx)
	t.commands = append(t.commands, &Command{
		Collection: collection,
		Action:     CreateDocument,
		DocID:      t.db.getPrimaryKey(collection, document),
		Change:     document,
		Timestamp:  time.Now(),
		Metadata:   md,
	})
	return t.db.getPrimaryKey(collection, document), nil
}

func (t *transaction) Set(ctx context.Context, collection string, document *Document) error {
	if !t.db.hasCollection(collection) {
		return stacktrace.NewError("unsupported collection: %s", collection)
	}
	md, _ := GetMetadata(ctx)
	t.commands = append(t.commands, &Command{
		Collection: collection,
		Action:     SetDocument,
		DocID:      t.db.getPrimaryKey(collection, document),
		Change:     document,
		Timestamp:  time.Now(),
		Metadata:   md,
	})
	return nil
}

func (t *transaction) Delete(ctx context.Context, collection string, id string) error {
	if !t.db.hasCollection(collection) {
		return stacktrace.NewError("unsupported collection: %s", collection)
	}
	md, _ := GetMetadata(ctx)
	t.commands = append(t.commands, &Command{
		Collection: collection,
		Action:     DeleteDocument,
		DocID:      id,
		Timestamp:  time.Now(),
		Metadata:   md,
	})
	return nil
}