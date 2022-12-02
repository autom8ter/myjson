package gokvkit

import (
	"bytes"
	"context"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/nqd/flat"
	"github.com/palantir/stacktrace"
	"github.com/segmentio/ksuid"
	"time"
)

const batchThreshold = 10

func (d *DB) updateDocument(ctx context.Context, mutator kv.Mutator, command *Command) error {
	if err := command.validate(); err != nil {
		return stacktrace.Propagate(err, "")
	}
	primaryIndex := d.primaryIndex(command.Collection)
	after := command.Before.Clone()
	flattened, err := flat.Flatten(command.Change.Value(), nil)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	err = after.SetAll(flattened)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	command.Change = after
	if err := d.applyValidationHooks(ctx, command); err != nil {
		return stacktrace.Propagate(err, "")
	}
	command, err = d.applySideEffectHooks(ctx, command)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := d.applyValidationHooks(ctx, command); err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := mutator.Set(primaryIndex.seekPrefix(map[string]any{
		d.primaryKey(command.Collection): command.DocID,
	}).SetDocumentID(command.DocID).Path(), command.Change.Bytes()); err != nil {
		return stacktrace.PropagateWithCode(err, ErrTODO, "failed to batch set documents to primary index")
	}
	return nil
}

func (d *DB) deleteDocument(ctx context.Context, mutator kv.Mutator, command *Command) error {
	if err := command.validate(); err != nil {
		return stacktrace.Propagate(err, "")
	}
	primaryIndex := d.primaryIndex(command.Collection)
	if err := d.applyValidationHooks(ctx, command); err != nil {
		return stacktrace.Propagate(err, "")
	}
	command, err := d.applySideEffectHooks(ctx, command)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := d.applyValidationHooks(ctx, command); err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := mutator.Delete(primaryIndex.seekPrefix(map[string]any{
		d.primaryKey(command.Collection): command.DocID,
	}).SetDocumentID(command.DocID).Path()); err != nil {
		return stacktrace.Propagate(err, "failed to batch delete documents")
	}
	return nil
}

func (d *DB) createDocument(ctx context.Context, mutator kv.Mutator, command *Command) error {
	primaryIndex := d.primaryIndex(command.Collection)
	if command.DocID == "" {
		command.DocID = ksuid.New().String()
		if err := d.setPrimaryKey(command.Collection, command.Change, command.DocID); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	if err := command.validate(); err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := d.applyValidationHooks(ctx, command); err != nil {
		return stacktrace.Propagate(err, "")
	}
	command, err := d.applySideEffectHooks(ctx, command)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := d.applyValidationHooks(ctx, command); err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := mutator.Set(primaryIndex.seekPrefix(map[string]any{
		d.primaryKey(command.Collection): command.DocID,
	}).SetDocumentID(command.DocID).Path(), command.Change.Bytes()); err != nil {
		return stacktrace.PropagateWithCode(err, ErrTODO, "failed to batch set documents to primary index")
	}
	return nil
}

func (d *DB) setDocument(ctx context.Context, mutator kv.Mutator, command *Command) error {
	if err := command.validate(); err != nil {
		return stacktrace.Propagate(err, "")
	}
	primaryIndex := d.primaryIndex(command.Collection)
	if err := d.applyValidationHooks(ctx, command); err != nil {
		return stacktrace.Propagate(err, "")
	}
	command, err := d.applySideEffectHooks(ctx, command)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := d.applyValidationHooks(ctx, command); err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := mutator.Set(primaryIndex.seekPrefix(map[string]any{
		d.primaryKey(command.Collection): command.DocID,
	}).SetDocumentID(command.DocID).Path(), command.Change.Bytes()); err != nil {
		return stacktrace.PropagateWithCode(err, ErrTODO, "failed to batch set documents to primary index")
	}
	return nil
}

func (d *DB) persistStateChange(ctx context.Context, mutator kv.Mutator, commands []*Command) error {
	for _, command := range commands {
		if command.Timestamp.IsZero() {
			command.Timestamp = time.Now()
		}
		before, _ := d.Get(ctx, command.Collection, command.DocID)
		if before == nil || !before.Valid() {
			before = NewDocument()
		}
		command.Before = before
		switch command.Action {
		case UpdateDocument:
			if err := d.updateDocument(ctx, mutator, command); err != nil {
				return stacktrace.Propagate(err, "")
			}
		case CreateDocument:
			if err := d.createDocument(ctx, mutator, command); err != nil {
				return stacktrace.Propagate(err, "")
			}
		case DeleteDocument:
			if err := d.deleteDocument(ctx, mutator, command); err != nil {
				return stacktrace.Propagate(err, "")
			}
		case SetDocument:
			if err := d.setDocument(ctx, mutator, command); err != nil {
				return stacktrace.Propagate(err, "")
			}
		}
		for _, i := range d.collections.Get(command.Collection).Indexes {
			if i.Primary {
				continue
			}
			if err := d.updateSecondaryIndex(ctx, mutator, i, command); err != nil {
				return stacktrace.PropagateWithCode(err, ErrTODO, "")
			}
		}
	}
	return nil
}

func (d *DB) updateSecondaryIndex(ctx context.Context, mutator kv.Mutator, idx Index, command *Command) error {
	if idx.Primary {
		return nil
	}

	switch command.Action {
	case DeleteDocument:
		if err := mutator.Delete(idx.seekPrefix(command.Before.Value()).SetDocumentID(command.DocID).Path()); err != nil {
			return stacktrace.PropagateWithCode(
				err,
				ErrTODO,
				"failed to delete document %s/%s index references",
				command.Collection,
				command.DocID,
			)
		}
	case SetDocument, UpdateDocument, CreateDocument:
		if command.Before != nil && command.Before.Valid() {
			if err := mutator.Delete(idx.seekPrefix(command.Before.Value()).SetDocumentID(command.DocID).Path()); err != nil {
				return stacktrace.PropagateWithCode(
					err,
					ErrTODO,
					"failed to delete document %s/%s index references",
					command.Collection,
					command.DocID,
				)
			}
		}
		if idx.Unique && !idx.Primary && command.Change != nil {
			if err := d.kv.Tx(false, func(tx kv.Tx) error {
				it := tx.NewIterator(kv.IterOpts{
					Prefix: idx.seekPrefix(command.Change.Value()).Path(),
				})
				defer it.Close()
				for it.Valid() {
					item := it.Item()
					split := bytes.Split(item.Key(), []byte("\x00"))
					id := split[len(split)-1]
					if string(id) != command.DocID {
						return stacktrace.NewErrorWithCode(ErrTODO, "duplicate value( %s ) found for unique index: %s", command.DocID, idx.Name)
					}
					it.Next()
				}
				return nil
			}); err != nil {
				return stacktrace.PropagateWithCode(
					err,
					ErrTODO,
					"failed to set document %s/%s index references",
					command.Collection,
					command.DocID,
				)
			}
		}
		// only persist ids in secondary index - lookup full document in primary index
		if err := mutator.Set(idx.seekPrefix(command.Change.Value()).SetDocumentID(command.DocID).Path(), []byte(command.DocID)); err != nil {
			return stacktrace.PropagateWithCode(
				err,
				ErrTODO,
				"failed to set document %s/%s index references",
				command.Collection,
				command.DocID,
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
	pfx := index.Ref.seekPrefix(index.Values)
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
func (d *DB) applySideEffectHooks(ctx context.Context, command *Command) (*Command, error) {
	var err error
	for _, sideEffect := range d.sideEffects.Get(command.Collection) {
		command, err = sideEffect.Func(ctx, d, command)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
	}
	return command, nil
}

func (d *DB) applyValidationHooks(ctx context.Context, command *Command) error {
	for _, validator := range d.validators.Get(command.Collection) {
		if err := validator.Func(ctx, d, command); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	return nil
}
