package gokvkit

import (
	"context"
	"time"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/model"
	"github.com/segmentio/ksuid"
)

// Tx is a database transaction interface
type Tx interface {
	// Query executes a query against the database
	Query(ctx context.Context, collection string, query model.Query) (model.Page, error)
	// Create creates a new document - if the documents primary key is unset, it will be set as a sortable unique id
	Create(ctx context.Context, collection string, document *model.Document) (string, error)
	// Update updates a value in the database
	Update(ctx context.Context, collection, id string, document map[string]any) error
	// Set sets the specified key/value in the database
	Set(ctx context.Context, collection string, document *model.Document) error
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
	commands []*model.Command
}

func (t *transaction) Commit(ctx context.Context) error {
	md, _ := model.GetMetadata(ctx)
	if len(t.commands) >= batchThreshold {
		batch := t.db.kv.Batch()
		if !md.Exists(string(isIndexingKey)) {
			for _, c := range t.commands {
				if err := t.db.applyPersistHooks(ctx, t, c, true); err != nil {
					return err
				}
			}
		}
		if err := t.db.persistStateChange(ctx, batch, t.commands); err != nil {
			return err
		}
		if !md.Exists(string(isIndexingKey)) {
			for _, c := range t.commands {
				if err := t.db.applyPersistHooks(ctx, t, c, false); err != nil {
					return err
				}
			}
		}
		return batch.Flush()
	}
	if err := t.db.kv.Tx(true, func(tx kv.Tx) error {
		if !md.Exists(string(isIndexingKey)) {
			for _, c := range t.commands {
				if err := t.db.applyPersistHooks(ctx, t, c, true); err != nil {
					return err
				}
			}
		}
		if err := t.db.persistStateChange(ctx, tx, t.commands); err != nil {
			return err
		}
		if !md.Exists(string(isIndexingKey)) {
			for _, c := range t.commands {
				if err := t.db.applyPersistHooks(ctx, t, c, false); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (t *transaction) Rollback(ctx context.Context) {
	t.commands = []*model.Command{}
}

func (t *transaction) Update(ctx context.Context, collection string, id string, update map[string]any) error {
	if !t.db.HasCollection(collection) {
		return errors.Wrap(nil, 0, "unsupported collection: %s", collection)
	}
	doc := model.NewDocument()
	if err := doc.SetAll(update); err != nil {
		return err
	}
	md, _ := model.GetMetadata(ctx)
	t.commands = append(t.commands, &model.Command{
		Collection: collection,
		Action:     model.Update,
		DocID:      id,
		After:      doc,
		Timestamp:  time.Now(),
		Metadata:   md,
	})
	return nil
}

func (t *transaction) Create(ctx context.Context, collection string, document *model.Document) (string, error) {
	if !t.db.HasCollection(collection) {
		return "", errors.Wrap(nil, 0, "unsupported collection: %s", collection)
	}
	if t.db.GetPrimaryKey(collection, document) == "" {
		id := ksuid.New().String()
		err := t.db.SetPrimaryKey(collection, document, id)
		if err != nil {
			return "", err
		}
	}

	md, _ := model.GetMetadata(ctx)
	t.commands = append(t.commands, &model.Command{
		Collection: collection,
		Action:     model.Create,
		DocID:      t.db.GetPrimaryKey(collection, document),
		After:      document,
		Timestamp:  time.Now(),
		Metadata:   md,
	})
	return t.db.GetPrimaryKey(collection, document), nil
}

func (t *transaction) Set(ctx context.Context, collection string, document *model.Document) error {
	if !t.db.HasCollection(collection) {
		return errors.Wrap(nil, 0, "unsupported collection: %s", collection)
	}
	md, _ := model.GetMetadata(ctx)
	t.commands = append(t.commands, &model.Command{
		Collection: collection,
		Action:     model.Set,
		DocID:      t.db.GetPrimaryKey(collection, document),
		After:      document,
		Timestamp:  time.Now(),
		Metadata:   md,
	})
	return nil
}

func (t *transaction) Delete(ctx context.Context, collection string, id string) error {
	if !t.db.HasCollection(collection) {
		return errors.Wrap(nil, 0, "unsupported collection: %s", collection)
	}
	md, _ := model.GetMetadata(ctx)
	t.commands = append(t.commands, &model.Command{
		Collection: collection,
		Action:     model.Delete,
		DocID:      id,
		Timestamp:  time.Now(),
		Metadata:   md,
	})
	return nil
}

func (t *transaction) Query(ctx context.Context, collection string, query model.Query) (model.Page, error) {
	return t.db.Query(ctx, collection, query)
}
