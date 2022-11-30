package gokvkit

import (
	"bytes"
	"context"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/palantir/stacktrace"
)

func (d *DB) persistStateChange(ctx context.Context, changes map[string]*StateChange) error {
	txn := d.kv.Batch()
	for collection, change := range changes {
		if change.Updates != nil {
			for id, edit := range change.Updates {
				before, _ := d.Get(ctx, collection, id)
				if !before.Valid() {
					before = NewDocument()
				}
				after := before.Clone()
				err := after.SetAll(edit)
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				if err := d.indexDocument(ctx, txn, &DocChange{
					Collection: collection,
					Action:     Update,
					DocID:      id,
					Before:     before,
					After:      after,
				}); err != nil {
					return stacktrace.Propagate(err, "")
				}
			}
		}
		for _, id := range change.Deletes {
			before, _ := d.Get(ctx, collection, id)
			if err := d.indexDocument(ctx, txn, &DocChange{
				Collection: collection,
				Action:     Delete,
				DocID:      id,
				Before:     before,
				After:      nil,
			}); err != nil {
				return stacktrace.Propagate(err, "")
			}
		}
		for _, after := range change.Creates {
			if !after.Valid() {
				return stacktrace.NewErrorWithCode(ErrTODO, "invalid json document")
			}
			docID := d.getPrimaryKey(collection, after)
			if docID == "" {
				return stacktrace.NewErrorWithCode(ErrTODO, "document missing primary key %s", d.primaryKey(collection))
			}
			before, _ := d.Get(ctx, collection, docID)
			if before != nil {
				return stacktrace.NewErrorWithCode(ErrTODO, "document already exists %s", docID)
			}
			if err := d.indexDocument(ctx, txn, &DocChange{
				Collection: collection,
				Action:     Create,
				DocID:      docID,
				Before:     nil,
				After:      after,
			}); err != nil {
				return stacktrace.Propagate(err, "")
			}
		}
		for _, after := range change.Sets {
			if !after.Valid() {
				return stacktrace.NewErrorWithCode(ErrTODO, "invalid json document")
			}
			docID := d.getPrimaryKey(collection, after)
			if docID == "" {
				return stacktrace.NewErrorWithCode(ErrTODO, "document missing primary key %s", d.primaryKey(collection))
			}
			before, _ := d.Get(ctx, collection, docID)
			if err := d.indexDocument(ctx, txn, &DocChange{
				Collection: collection,
				Action:     Set,
				DocID:      docID,
				Before:     before,
				After:      after,
			}); err != nil {
				return stacktrace.Propagate(err, "")
			}
		}
		if err := txn.Flush(); err != nil {
			return stacktrace.Propagate(err, "failed to batch collection documents")
		}
	}
	return nil
}

func (d *DB) indexDocument(ctx context.Context, txn kv.Batch, change *DocChange) error {
	if change.DocID == "" {
		return stacktrace.NewErrorWithCode(ErrTODO, "empty document id")
	}
	var err error
	primaryIndex := d.primaryIndex(change.Collection)

	if change.After != nil {
		if d.getPrimaryKey(change.Collection, change.After) != change.DocID {
			return stacktrace.NewErrorWithCode(ErrTODO, "document id is immutable: %v -> %v", d.getPrimaryKey(change.Collection, change.After), change.DocID)
		}
		if err := d.applyValidationHooks(ctx, change); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	change, err = d.applySideEffectHooks(ctx, change)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	switch change.Action {
	case Delete:
		if err := txn.Delete(primaryIndex.Seek(map[string]any{
			d.primaryKey(change.Collection): change.DocID,
		}).SetDocumentID(change.DocID).Path()); err != nil {
			return stacktrace.Propagate(err, "failed to batch delete documents")
		}
	case Set, Update, Create:
		if err := txn.Set(primaryIndex.Seek(map[string]any{
			d.primaryKey(change.Collection): change.DocID,
		}).SetDocumentID(change.DocID).Path(), change.After.Bytes()); err != nil {
			return stacktrace.PropagateWithCode(err, ErrTODO, "failed to batch set documents to primary index")
		}
	}

	for _, i := range d.collections.Get(change.Collection).Indexes {
		if i.Primary {
			continue
		}
		if err := d.updateSecondaryIndex(ctx, txn, i, change); err != nil {
			return stacktrace.PropagateWithCode(err, ErrTODO, "")
		}
	}
	return nil
}

func (d *DB) updateSecondaryIndex(ctx context.Context, txn kv.Batch, idx Index, change *DocChange) error {
	if idx.Primary {
		return nil
	}

	switch change.Action {
	case Delete:
		if err := txn.Delete(idx.Seek(change.Before.Value()).SetDocumentID(change.DocID).Path()); err != nil {
			return stacktrace.PropagateWithCode(
				err,
				ErrTODO,
				"failed to delete document %s/%s index references",
				change.Collection,
				change.DocID,
			)
		}
	case Set, Update, Create:
		if change.Before != nil && change.Before.Valid() {
			if err := txn.Delete(idx.Seek(change.Before.Value()).SetDocumentID(change.DocID).Path()); err != nil {
				return stacktrace.PropagateWithCode(
					err,
					ErrTODO,
					"failed to delete document %s/%s index references",
					change.Collection,
					change.DocID,
				)
			}
		}
		if idx.Unique && !idx.Primary && change.After != nil {
			if err := d.kv.Tx(false, func(tx kv.Tx) error {
				it := tx.NewIterator(kv.IterOpts{
					Prefix: idx.Seek(change.After.Value()).Path(),
				})
				defer it.Close()
				for it.Valid() {
					item := it.Item()
					split := bytes.Split(item.Key(), []byte("\x00"))
					id := split[len(split)-1]
					if string(id) != change.DocID {
						return stacktrace.NewErrorWithCode(ErrTODO, "duplicate value( %s ) found for unique index: %s", change.DocID, idx.Name)
					}
					it.Next()
				}
				return nil
			}); err != nil {
				return stacktrace.PropagateWithCode(
					err,
					ErrTODO,
					"failed to set document %s/%s index references",
					change.Collection,
					change.DocID,
				)
			}
		}
		// only persist ids in secondary index - lookup full document in primary index
		if err := txn.Set(idx.Seek(change.After.Value()).SetDocumentID(change.DocID).Path(), []byte(change.DocID)); err != nil {
			return stacktrace.PropagateWithCode(
				err,
				ErrTODO,
				"failed to set document %s/%s index references",
				change.Collection,
				change.DocID,
			)
		}
	}
	return nil
}

func (d *DB) getReadyIndexes(ctx context.Context, collection string) map[string]Index {
	var indexes = map[string]Index{}
	for _, i := range d.collections.Get(collection).Indexes {
		if i.IsBuilding {
			continue
		}
		indexes[i.Name] = i
	}
	return indexes
}

func (d *DB) queryScan(ctx context.Context, scan Scan, handlerFunc ScanFunc) (OptimizerResult, error) {
	if handlerFunc == nil {
		return OptimizerResult{}, stacktrace.NewError("empty scan handler")
	}
	var err error
	scan.Where, err = d.applyWhereHooks(ctx, scan.From, scan.Where)
	if err != nil {
		return OptimizerResult{}, stacktrace.Propagate(err, "")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	index, err := d.optimizer.Optimize(d.getReadyIndexes(ctx, scan.From), scan.Where)
	if err != nil {
		return OptimizerResult{}, stacktrace.Propagate(err, "")
	}
	pfx := index.Ref.Seek(index.Values)
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		opts := kv.IterOpts{
			Prefix:  pfx.Path(),
			Seek:    nil,
			Reverse: false,
		}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Valid() {
			item := it.Item()

			var document *Document
			if index.IsPrimaryIndex {
				bits, err := item.Value()
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				document, err = NewDocumentFromBytes(bits)
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
			} else {
				split := bytes.Split(item.Key(), []byte("\x00"))
				id := split[len(split)-1]
				document, err = d.Get(ctx, scan.From, string(id))
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
			}

			pass, err := document.Where(scan.Where)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if pass {
				document, err = d.applyReadHooks(ctx, scan.From, document)
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				shouldContinue, err := handlerFunc(document)
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				if !shouldContinue {
					return nil
				}
			}
			it.Next()
		}
		return nil
	}); err != nil {
		return index, stacktrace.Propagate(err, "")
	}
	return index, nil
}

func (d *DB) applyWhereHooks(ctx context.Context, collection string, where []Where) ([]Where, error) {
	var err error
	for _, whereHook := range d.whereHooks.Get(collection) {
		where, err = whereHook.Func(ctx, d, where)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
	}
	return where, nil
}

func (d *DB) applyReadHooks(ctx context.Context, collection string, doc *Document) (*Document, error) {
	var err error
	for _, readHook := range d.readHooks.Get(collection) {
		doc, err = readHook.Func(ctx, d, doc)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
	}
	return doc, nil
}
func (d *DB) applySideEffectHooks(ctx context.Context, change *DocChange) (*DocChange, error) {
	var err error
	for _, sideEffect := range d.sideEffects.Get(change.Collection) {
		change, err = sideEffect.Func(ctx, d, change)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
	}
	return change, nil
}

func (d *DB) applyValidationHooks(ctx context.Context, doc *DocChange) error {
	for _, validator := range d.validators.Get(doc.Collection) {
		if err := validator.Func(ctx, d, doc); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	return nil
}
