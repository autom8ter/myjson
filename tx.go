package gokvkit

import (
	"context"
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
	db      *DB
	changes map[string]*StateChange
}

func (t *transaction) Commit(ctx context.Context) error {
	return t.db.persistStateChange(ctx, t.changes)
}

func (t *transaction) Rollback(ctx context.Context) {
	t.changes = map[string]*StateChange{}
}

func (t *transaction) Update(ctx context.Context, collection string, id string, update map[string]any) error {
	t.checkCollection(ctx, collection)
	t.changes[collection].Updates[id] = update
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
	t.checkCollection(ctx, collection)
	t.changes[collection].Creates = append(t.changes[collection].Creates, document)
	return t.db.getPrimaryKey(collection, document), nil
}

func (t *transaction) Set(ctx context.Context, collection string, document *Document) error {
	t.checkCollection(ctx, collection)
	t.changes[collection].Sets = append(t.changes[collection].Sets, document)
	return nil
}

func (t *transaction) Delete(ctx context.Context, collection string, id string) error {
	t.checkCollection(ctx, collection)
	t.changes[collection].Deletes = append(t.changes[collection].Deletes, id)
	return nil
}

func (t *transaction) checkCollection(ctx context.Context, collection string) {
	if t.changes[collection] == nil {
		md, _ := GetMetadata(ctx)
		t.changes[collection] = &StateChange{
			Metadata:   md,
			Collection: collection,
			Deletes:    nil,
			Creates:    nil,
			Sets:       nil,
			Updates:    map[string]map[string]any{},
			Timestamp:  time.Now(),
		}
	}
}
