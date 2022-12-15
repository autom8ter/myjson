package gokvkit

import (
	"bytes"
	"context"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/model"
)

func (d *DB) getReadyIndexes(ctx context.Context, collection string) map[string]model.Index {
	var indexes = map[string]model.Index{}
	for _, i := range d.collections.Get(collection).indexing {
		if i.IsBuilding {
			continue
		}
		indexes[i.Name] = i
	}
	return indexes
}

func (d *DB) queryScan(ctx context.Context, scan model.Scan, handlerFunc model.ScanFunc) (model.OptimizerResult, error) {
	if handlerFunc == nil {
		return model.OptimizerResult{}, errors.New(errors.Validation, "empty scan handler")
	}
	var err error
	scan.Where, err = d.applyWhereHooks(ctx, scan.From, scan.Where)
	if err != nil {
		return model.OptimizerResult{}, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	index, err := d.optimizer.Optimize(d.getReadyIndexes(ctx, scan.From), scan.Where)
	if err != nil {
		return model.OptimizerResult{}, err
	}
	pfx := index.Ref.SeekPrefix(index.Values)
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

			var document *model.Document
			if index.IsPrimaryIndex {
				bits, err := item.Value()
				if err != nil {
					return err
				}
				document, err = model.NewDocumentFromBytes(bits)
				if err != nil {
					return err
				}
			} else {
				split := bytes.Split(item.Key(), []byte("\x00"))
				id := split[len(split)-1]
				document, err = d.Get(ctx, scan.From, string(id))
				if err != nil {
					return err
				}
			}

			pass, err := document.Where(scan.Where)
			if err != nil {
				return err
			}
			if pass {
				document, err = d.applyReadHooks(ctx, scan.From, document)
				if err != nil {
					return err
				}
				shouldContinue, err := handlerFunc(document)
				if err != nil {
					return err
				}
				if !shouldContinue {
					return nil
				}
			}
			it.Next()
		}
		return nil
	}); err != nil {
		return index, err
	}
	return index, nil
}

func (d *DB) applyWhereHooks(ctx context.Context, collection string, where []model.Where) ([]model.Where, error) {
	var err error
	for _, whereHook := range d.whereHooks.Get(collection) {
		where, err = whereHook.Func(ctx, d, where)
		if err != nil {
			return nil, err
		}
	}
	return where, nil
}

func (d *DB) applyReadHooks(ctx context.Context, collection string, doc *model.Document) (*model.Document, error) {
	var err error
	for _, readHook := range d.readHooks.Get(collection) {
		doc, err = readHook.Func(ctx, d, doc)
		if err != nil {
			return nil, err
		}
	}
	return doc, nil
}
func (d *DB) applyPersistHooks(ctx context.Context, tx Tx, command *model.Command, before bool) error {
	for _, sideEffect := range d.persistHooks.Get(command.Collection) {
		if sideEffect.Before == before {
			if err := sideEffect.Func(ctx, tx, command); err != nil {
				return err
			}
		}
	}
	return nil
}
