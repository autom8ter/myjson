package gokvkit

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/nqd/flat"
	"github.com/segmentio/ksuid"
)

func (t *transaction) updateDocument(ctx context.Context, c CollectionSchema, docID string, before *Document, command *Command) error {
	if before == nil {
		return errors.New(errors.Internal, "tx: updateDocument - empty before value")
	}
	primaryIndex := c.PrimaryIndex()

	after := before.Clone()
	flattened, err := flat.Flatten(command.Document.Value(), nil)
	if err != nil {
		return err
	}
	err = after.SetAll(flattened)
	if err != nil {
		return err
	}
	if err := c.ValidateDocument(ctx, after); err != nil {
		return err
	}
	if err := t.tx.Set(seekPrefix(c.Collection(), primaryIndex, map[string]any{
		c.PrimaryKey(): docID,
	}).SetDocumentID(docID).Path(), after.Bytes()); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to batch set documents to primary index")
	}
	return nil
}

func (t *transaction) deleteDocument(ctx context.Context, c CollectionSchema, docID string) error {
	if docID == "" {
		return errors.New(errors.Validation, "tx: delete command - empty document id")
	}
	primaryIndex := c.PrimaryIndex()
	if err := t.tx.Delete(seekPrefix(c.Collection(), primaryIndex, map[string]any{
		c.PrimaryKey(): docID,
	}).SetDocumentID(docID).Path()); err != nil {
		return errors.Wrap(err, 0, "failed to delete documents")
	}
	return nil
}

func (t *transaction) createDocument(ctx context.Context, c CollectionSchema, command *Command) error {
	primaryIndex := c.PrimaryIndex()
	docID := c.GetPrimaryKey(command.Document)
	if err := c.SetPrimaryKey(command.Document, docID); err != nil {
		return err
	}
	if err := c.ValidateDocument(ctx, command.Document); err != nil {
		return err
	}
	if err := t.tx.Set(seekPrefix(c.Collection(), primaryIndex, map[string]any{
		c.PrimaryKey(): docID,
	}).SetDocumentID(docID).Path(), command.Document.Bytes()); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to batch set documents to primary index")
	}
	return nil
}

func (t *transaction) setDocument(ctx context.Context, c CollectionSchema, docID string, command *Command) error {
	if docID == "" {
		return errors.New(errors.Validation, "tx: set command - empty document id")
	}
	if err := c.ValidateDocument(ctx, command.Document); err != nil {
		return err
	}
	primaryIndex := c.PrimaryIndex()
	if err := t.tx.Set(seekPrefix(c.Collection(), primaryIndex, map[string]any{
		c.PrimaryKey(): docID,
	}).SetDocumentID(docID).Path(), command.Document.Bytes()); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to set documents to primary index")
	}
	return nil
}

func (t *transaction) persistCommand(ctx context.Context, md *Metadata, command *Command) error {

	c := t.db.GetSchema(ctx, command.Collection)
	if c == nil {
		return fmt.Errorf("tx: collection: %s does not exist", command.Collection)
	}
	docID := c.GetPrimaryKey(command.Document)
	if command.Timestamp.IsZero() {
		command.Timestamp = time.Now()
	}
	if command.Metadata == nil {
		md, _ := GetMetadata(ctx)
		command.Metadata = md
	}
	if err := command.Validate(); err != nil {
		return err
	}
	before, _ := t.Get(ctx, command.Collection, docID)
	if md.Exists(string(isIndexingKey)) {
		for _, i := range t.db.GetSchema(ctx, command.Collection).Indexing() {
			if i.Primary {
				continue
			}
			if err := t.updateSecondaryIndex(ctx, i, docID, before, command); err != nil {
				return errors.Wrap(err, errors.Internal, "")
			}
		}
		return nil
	}
	if t.db.collectionIsLocked(command.Collection) {
		return errors.New(errors.Forbidden, "collection %s is locked", command.Collection)
	}
	if err := t.applyPersistHooks(ctx, t, command, true); err != nil {
		return err
	}

	switch command.Action {
	case Update:
		if err := t.updateDocument(ctx, c, docID, before, command); err != nil {
			return err
		}
	case Create:
		if err := t.createDocument(ctx, c, command); err != nil {
			return err
		}
	case Delete:
		if err := t.deleteDocument(ctx, c, docID); err != nil {
			return err
		}
	case Set:
		if err := t.setDocument(ctx, c, docID, command); err != nil {
			return err
		}
	}
	for _, i := range t.db.GetSchema(ctx, command.Collection).Indexing() {
		if err := t.updateSecondaryIndex(ctx, i, docID, before, command); err != nil {
			return errors.Wrap(err, errors.Internal, "")
		}
	}
	if err := t.applyPersistHooks(ctx, t, command, false); err != nil {
		return err
	}
	if command.Collection != cdcCollectionName {
		cdc := CDC{
			ID:         ksuid.New().String(),
			Collection: command.Collection,
			Action:     command.Action,
			DocumentID: c.GetPrimaryKey(command.Document),
			Timestamp:  command.Timestamp,
			Metadata:   command.Metadata,
			Diff:       command.Document.Diff(before),
		}
		cdcDoc, err := NewDocumentFrom(&cdc)
		if err != nil {
			return errors.Wrap(err, errors.Internal, "failed to persist cdc")
		}
		if err := t.persistCommand(ctx, cdc.Metadata, &Command{
			Collection: "cdc",
			Action:     Create,
			Document:   cdcDoc,
			Timestamp:  cdc.Timestamp,
			Metadata:   cdc.Metadata,
		}); err != nil {
			return errors.Wrap(err, errors.Internal, "failed to persist cdc")
		}
		t.cdc = append(t.cdc, cdc)
	}
	return nil
}

func (t *transaction) updateSecondaryIndex(ctx context.Context, idx Index, docID string, before *Document, command *Command) error {
	if idx.Primary {
		return nil
	}
	switch command.Action {
	case Delete:
		if err := t.tx.Delete(seekPrefix(command.Collection, idx, before.Value()).SetDocumentID(docID).Path()); err != nil {
			return errors.Wrap(
				err,
				errors.Internal,
				"failed to delete document %s/%s index references",
				command.Collection,
				docID,
			)
		}
	case Set, Update, Create:
		if before != nil {
			if err := t.tx.Delete(seekPrefix(command.Collection, idx, before.Value()).SetDocumentID(docID).Path()); err != nil {
				return errors.Wrap(
					err,
					errors.Internal,
					"failed to delete document %s/%s index references",
					command.Collection,
					docID,
				)
			}
		}
		if idx.Unique && !idx.Primary && command.Document != nil {
			if err := t.db.kv.Tx(false, func(tx kv.Tx) error {
				it := tx.NewIterator(kv.IterOpts{
					Prefix: seekPrefix(command.Collection, idx, command.Document.Value()).Path(),
				})
				defer it.Close()
				for it.Valid() {
					item := it.Item()
					split := bytes.Split(item.Key(), []byte("\x00"))
					id := split[len(split)-1]
					if string(id) != docID {
						return errors.New(errors.Internal, "duplicate value( %s ) found for unique index: %s", docID, idx.Name)
					}
					it.Next()
				}
				return nil
			}); err != nil {
				return errors.Wrap(
					err,
					errors.Internal,
					"failed to set document %s/%s index references",
					command.Collection,
					docID,
				)
			}
		}
		// only persist ids in secondary index - lookup full document in primary index
		if err := t.tx.Set(seekPrefix(command.Collection, idx, command.Document.Value()).SetDocumentID(docID).Path(), []byte(docID)); err != nil {
			return errors.Wrap(
				err,
				errors.Internal,
				"failed to set document %s/%s index references",
				command.Collection,
				docID,
			)
		}
	}
	return nil
}

func (t *transaction) applyPersistHooks(ctx context.Context, tx Tx, command *Command, before bool) error {
	for _, sideEffect := range t.db.persistHooks {
		if sideEffect.Before == before {
			if err := sideEffect.Func(ctx, tx, command); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *transaction) queryScan(ctx context.Context, collection string, where []Where, fn ForEachFunc) (Optimization, error) {
	if fn == nil {
		return Optimization{}, errors.New(errors.Validation, "empty scan handler")
	}
	if !t.db.HasCollection(ctx, collection) {
		return Optimization{}, errors.New(errors.Validation, "unsupported collection: %s", collection)
	}
	var err error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if t.db.collectionIsLocked(collection) {
		return Optimization{}, errors.New(errors.Forbidden, "collection %s is locked", collection)
	}
	optimization, err := t.db.optimizer.Optimize(t.db.GetSchema(ctx, collection), where)
	if err != nil {
		return Optimization{}, err
	}

	pfx := seekPrefix(collection, optimization.Index, optimization.MatchedValues)
	opts := kv.IterOpts{
		Prefix:  pfx.Path(),
		Seek:    pfx.Path(),
		Reverse: false,
	}
	it := t.tx.NewIterator(opts)
	defer it.Close()
	for it.Valid() {
		item := it.Item()

		var document *Document
		if optimization.Index.Primary {
			bits, err := item.Value()
			if err != nil {
				return optimization, err
			}
			document, err = NewDocumentFromBytes(bits)
			if err != nil {
				return optimization, err
			}
		} else {
			split := bytes.Split(item.Key(), []byte("\x00"))
			id := split[len(split)-1]
			document, err = t.Get(ctx, collection, string(id))
			if err != nil {
				return optimization, err
			}
		}

		pass, err := document.Where(where)
		if err != nil {
			return optimization, err
		}
		if pass {
			shouldContinue, err := fn(document)
			if err != nil {
				return optimization, err
			}
			if !shouldContinue {
				return optimization, err
			}
		}
		it.Next()
	}
	return optimization, nil
}
