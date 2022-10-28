package core

import (
	"context"
	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/internal/prefix"
	"github.com/autom8ter/wolverine/kv"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"strings"
	"time"
)

type coreImplementation struct {
	kv      kv.DB
	machine machine.Machine
}

// NewCore creates a new CoreAPI instance with the given storeage path and collections. Under the hood it is powered by the kv.DB interface
func NewCore(kv kv.DB, collections []*Collection) (CoreAPI, error) {
	d := coreImplementation{
		kv:      kv,
		machine: machine.New(),
	}
	for _, collection := range collections {
		if collection == nil {
			panic("null collection")
		}
	}
	return d, nil
}

func (d coreImplementation) ChangeStream(ctx context.Context, collection *Collection, fn ChangeStreamHandler) error {
	return d.machine.Subscribe(ctx, collection.Collection(), func(ctx context.Context, msg machine.Message) (bool, error) {
		switch change := msg.Body.(type) {
		case *StateChange:
			if err := fn(ctx, *change); err != nil {
				return false, stacktrace.Propagate(err, "")
			}
		case StateChange:
			if err := fn(ctx, change); err != nil {
				return false, stacktrace.Propagate(err, "")
			}
		}
		return true, nil
	})
}

func (d coreImplementation) Aggregate(ctx context.Context, collection *Collection, query AggregateQuery) (Page, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()
	index, err := collection.OptimizeIndex(query.Where, query.OrderBy)
	if err != nil {
		return Page{}, stacktrace.Propagate(err, "")
	}
	var results []*Document
	if err := d.kv.Tx(false, func(tx kv.Tx) error {
		opts := kv.IterOpts{
			Prefix:  index.Ref.GetPrefix(IndexableFields(query.Where, query.OrderBy), ""),
			Seek:    nil,
			Reverse: false,
		}
		if query.OrderBy.Direction == DESC {
			opts.Reverse = true
			opts.Seek = prefix.PrefixNextKey(opts.Prefix)
		} else {
			opts.Seek = opts.Prefix
		}
		it := tx.NewIterator(opts)
		it.Seek(opts.Prefix)
		defer it.Close()
		for it.Valid() {
			if ctx.Err() != nil {
				return nil
			}
			item := it.Item()
			bits, err := item.Value()
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			document, err := NewDocumentFromBytes(bits)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			pass, err := document.Where(query.Where)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if pass {
				results = append(results, document)
			}
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			it.Next()
		}
		return nil
	}); err != nil {
		return Page{}, stacktrace.Propagate(err, "")
	}
	grouped := lo.GroupBy[*Document](results, func(d *Document) string {
		var values []string
		for _, g := range query.GroupBy {
			values = append(values, cast.ToString(d.Get(g)))
		}
		return strings.Join(values, ".")
	})
	var reduced []*Document
	for _, values := range grouped {
		value, err := ApplyReducers(ctx, query, values)
		if err != nil {
			return Page{}, stacktrace.Propagate(err, "")
		}
		reduced = append(reduced, value)
	}
	reduced = SortOrder(query.OrderBy, reduced)
	if query.Limit > 0 && query.Page > 0 {
		reduced = lo.Slice(reduced, query.Limit*query.Page, (query.Limit*query.Page)+query.Limit)
	}
	if query.Limit > 0 && len(reduced) > query.Limit {
		reduced = reduced[:query.Limit]
	}
	for _, r := range reduced {
		toSelect := query.GroupBy
		for _, a := range query.Aggregates {
			toSelect = append(toSelect, a.Alias)
		}
		err := r.Select(toSelect)
		if err != nil {
			return Page{}, stacktrace.Propagate(err, "")
		}
	}
	return Page{
		Documents: reduced,
		NextPage:  query.Page + 1,
		Count:     len(reduced),
		Stats: PageStats{
			ExecutionTime: time.Since(now),
			IndexMatch:    index,
		},
	}, nil
}

func (d coreImplementation) getAllCollection(ctx context.Context, collection *Collection, ids []string) (Documents, error) {
	var documents []*Document
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		for _, id := range ids {
			pkey, err := collection.GetPrimaryKeyRef(id)
			if err != nil {
				return stacktrace.PropagateWithCode(err, errors.ErrTODO, "failed to get document %s/%s primary key ref", collection.Collection(), id)
			}
			value, err := txn.Get(pkey)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}

			document, err := NewDocumentFromBytes(value)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			documents = append(documents, document)
		}
		return nil
	}); err != nil {
		return documents, err
	}
	return documents, nil
}

func (d coreImplementation) getCollection(ctx context.Context, collection *Collection, id string) (*Document, error) {
	var (
		document *Document
	)
	pkey, err := collection.GetPrimaryKeyRef(id)
	if err != nil {
		return nil, stacktrace.PropagateWithCode(err, errors.ErrTODO, "failed to get document %s/%s primary key ref", collection.Collection(), id)
	}
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		val, err := txn.Get(pkey)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		document, err = NewDocumentFromBytes(val)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		return nil
	}); err != nil {
		return document, err
	}
	return document, nil
}

func (d coreImplementation) Persist(ctx context.Context, collection *Collection, change StateChange) error {
	txn := d.kv.Batch()
	if collection == nil {
		return stacktrace.NewErrorWithCode(errors.ErrTODO, "null collection schema")
	}
	if change.Updates != nil {
		for id, edit := range change.Updates {
			before, _ := d.getCollection(ctx, collection, id)
			if !before.Valid() {
				before = NewDocument()
			}
			after := before.Clone()
			err := after.SetAll(edit)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if err := d.indexDocument(ctx, txn, collection, &singleChange{
				action: Update,
				docId:  id,
				before: before,
				after:  after,
			}); err != nil {
				return stacktrace.Propagate(err, "")
			}
		}
	}
	for _, id := range change.Deletes {
		before, _ := d.getCollection(ctx, collection, id)
		if err := d.indexDocument(ctx, txn, collection, &singleChange{
			action: Delete,
			docId:  id,
			before: before,
			after:  nil,
		}); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	for _, after := range change.Sets {
		if !after.Valid() {
			return stacktrace.NewErrorWithCode(errors.ErrTODO, "invalid json document")
		}
		docId := collection.GetPrimaryKey(after)
		if docId == "" {
			return stacktrace.NewErrorWithCode(errors.ErrTODO, "document missing primary key %s", collection.PrimaryKey())
		}
		before, _ := d.getCollection(ctx, collection, docId)
		if err := d.indexDocument(ctx, txn, collection, &singleChange{
			action: Set,
			docId:  docId,
			before: before,
			after:  after,
		}); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	if err := txn.Flush(); err != nil {
		return stacktrace.Propagate(err, "failed to batch collection documents")
	}
	d.machine.Publish(ctx, machine.Message{
		Channel: change.Collection,
		Body:    change,
	})
	return nil
}

type singleChange struct {
	action Action
	docId  string
	before *Document
	after  *Document
}

func (d coreImplementation) indexDocument(ctx context.Context, txn kv.Batch, collection *Collection, change *singleChange) error {
	if change.docId == "" {
		return stacktrace.NewErrorWithCode(errors.ErrTODO, "empty document id")
	}
	pkey, err := collection.GetPrimaryKeyRef(change.docId)
	if err != nil {
		return stacktrace.PropagateWithCode(err, errors.ErrTODO, "failed to get document %s/%s primary key ref", collection.Collection(), change.docId)
	}
	for _, i := range collection.Indexing().Indexes {
		if err := d.updateSecondaryIndex(ctx, txn, collection, i, change); err != nil {
			return stacktrace.PropagateWithCode(err, errors.ErrTODO, "")
		}
	}
	switch change.action {
	case Delete:
		if !change.before.Valid() {
			return stacktrace.NewError("invalid document")
		}
		if err := txn.Delete(pkey); err != nil {
			return stacktrace.Propagate(err, "failed to batch delete documents")
		}
	case Set, Update:
		if collection.GetPrimaryKey(change.after) != change.docId {
			return stacktrace.NewErrorWithCode(errors.ErrTODO, "document id is immutable: %v -> %v", collection.GetPrimaryKey(change.after), change.docId)
		}
		err := collection.Validate(ctx, change.after.Bytes())
		if err != nil {
			return stacktrace.Propagate(err, "")
		}

		if err := txn.Set(pkey, change.after.Bytes()); err != nil {
			return stacktrace.PropagateWithCode(err, errors.ErrTODO, "failed to batch set documents to primary index")
		}
	}
	return nil
}

func (d coreImplementation) updateSecondaryIndex(ctx context.Context, txn kv.Batch, collection *Collection, idx *Index, change *singleChange) error {
	switch change.action {
	case Delete:
		pindex := prefix.NewPrefixedIndex(collection.Collection(), idx.Fields)
		if err := txn.Delete(pindex.GetPrefix(change.before.Value(), change.docId)); err != nil {
			return stacktrace.Propagate(err, "failed to delete index references")
		}
	case Set, Update:
		pindex := prefix.NewPrefixedIndex(collection.Collection(), idx.Fields)
		if change.before != nil && change.before.Valid() {
			if err := txn.Delete(pindex.GetPrefix(change.before.Value(), change.docId)); err != nil {
				return stacktrace.PropagateWithCode(
					err,
					errors.ErrTODO,
					"failed to delete document %s/%s index references",
					collection.Collection(),
					change.docId,
				)
			}
		}
		i := pindex.GetPrefix(change.after.Value(), change.docId)
		if err := txn.Set(i, change.after.Bytes()); err != nil {
			return stacktrace.PropagateWithCode(
				err,
				errors.ErrTODO,
				"failed to set document %s/%s index references",
				collection.Collection(),
				change.docId,
			)
		}
	}
	return nil
}

func (d coreImplementation) Query(ctx context.Context, collection *Collection, query Query) (Page, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()
	index, err := collection.OptimizeIndex(query.Where, query.OrderBy)
	if err != nil {
		return Page{}, stacktrace.Propagate(err, "")
	}
	var results []*Document
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		opts := kv.IterOpts{
			Prefix:  index.Ref.GetPrefix(IndexableFields(query.Where, query.OrderBy), ""),
			Seek:    nil,
			Reverse: false,
		}
		if query.OrderBy.Direction == DESC {
			opts.Reverse = true
			opts.Seek = prefix.PrefixNextKey(opts.Prefix)
		} else {
			opts.Seek = opts.Prefix
		}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Valid() {
			item := it.Item()
			bits, err := item.Value()
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			document, err := NewDocumentFromBytes(bits)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			pass, err := document.Where(query.Where)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if !pass {
				return nil
			}
			results = append(results, document)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			it.Next()
		}
		return nil
	}); err != nil {
		return Page{}, stacktrace.Propagate(err, "")
	}
	results = SortOrder(query.OrderBy, results)

	if query.Limit > 0 && query.Page > 0 {
		results = lo.Slice(results, query.Limit*query.Page, (query.Limit*query.Page)+query.Limit)
	}
	if query.Limit > 0 && len(results) > query.Limit {
		results = results[:query.Limit]
	}

	if len(query.Select) > 0 && query.Select[0] != "*" {
		for _, result := range results {
			err := result.Select(query.Select)
			if err != nil {
				return Page{}, stacktrace.Propagate(err, "")
			}
		}
	}

	return Page{
		Documents: results,
		NextPage:  query.Page + 1,
		Count:     len(results),
		Stats: PageStats{
			ExecutionTime: time.Since(now),
			IndexMatch:    index,
		},
	}, nil
}

func (d coreImplementation) Scan(ctx context.Context, collection *Collection, scan Scan, handlerFunc ScanFunc) error {
	if handlerFunc == nil {
		return stacktrace.NewError("empty scan handler")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	index, err := collection.OptimizeIndex(scan.Filter, OrderBy{})
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		opts := kv.IterOpts{
			Prefix:  index.Ref.GetPrefix(IndexableFields(scan.Filter, OrderBy{}), ""),
			Seek:    nil,
			Reverse: scan.Reverse,
		}
		if scan.Reverse {
			opts.Seek = prefix.PrefixNextKey(opts.Prefix)
		} else {
			opts.Seek = opts.Prefix
		}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Valid() {
			item := it.Item()
			bits, err := item.Value()
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			document, err := NewDocumentFromBytes(bits)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			pass, err := document.Where(scan.Filter)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if pass {
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
		return stacktrace.Propagate(err, "")
	}
	return nil
}

func (d coreImplementation) Close(ctx context.Context) error {
	return stacktrace.Propagate(d.kv.Close(), "")
}
