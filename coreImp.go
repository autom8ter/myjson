package brutus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/autom8ter/brutus/kv"
	"github.com/autom8ter/machine/v4"
	"github.com/palantir/stacktrace"
	"sort"
	"sync"
)

type coreImplementation struct {
	kv          kv.DB
	machine     machine.Machine
	collections sync.Map
	isBuilding  sync.Map
	optimizer   Optimizer
}

// NewCore creates a new CoreAPI instance with the given key value database
func NewCore(kv kv.DB) (CoreAPI, error) {
	d := &coreImplementation{
		kv:          kv,
		machine:     machine.New(),
		collections: sync.Map{},
		optimizer:   defaultOptimizer{},
		isBuilding:  sync.Map{},
	}
	return d, nil
}

func (d *coreImplementation) coll(collection string) (*Collection, bool) {
	c, ok := d.collections.Load(collection)
	if !ok {
		return nil, false
	}
	return c.(*Collection), true
}

func (d *coreImplementation) getPrimaryIndex(collection string) (Index, bool) {
	c, ok := d.coll(collection)
	if !ok {
		return Index{}, false
	}
	for _, i := range c.indexes {
		if i.Primary {
			return i, true
		}
	}
	return Index{}, false
}

//
func (d *coreImplementation) GetCollection(ctx context.Context, name string) (*Collection, bool) {
	c, ok := d.collections.Load(name)
	if !ok {
		return nil, false
	}
	return c.(*Collection), true
}

func (d *coreImplementation) GetCollections(ctx context.Context) ([]*Collection, error) {
	var collections []*Collection
	d.collections.Range(func(key, value any) bool {
		collections = append(collections, value.(*Collection))
		return true
	})
	sort.Slice(collections, func(i, j int) bool {
		return collections[i].name < collections[j].name
	})
	return collections, nil
}

func (d *coreImplementation) SetCollections(ctx context.Context, collections []*Collection) error {
	meta, _ := GetContext(ctx)
	meta.Set("_internal", true)
	ctx = meta.ToContext(ctx)
	for _, c := range collections {
		d.collections.Store(c.name, c)
		indexes, err := d.getIndexes(c.name)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		diff, err := getIndexDiff(indexes, c.indexes)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		for _, update := range diff.toUpdate {
			if err := d.removeIndex(ctx, c.name, update); err != nil {
				return stacktrace.Propagate(err, "")
			}
		}
		for _, toUpdate := range diff.toUpdate {
			if err := d.addIndex(ctx, c.name, toUpdate); err != nil {
				return stacktrace.Propagate(err, "")
			}
		}
		for _, toDelete := range diff.toRemove {
			if err := d.removeIndex(ctx, c.name, toDelete); err != nil {
				return stacktrace.Propagate(err, "")
			}
		}
		for _, toAdd := range diff.toAdd {
			if err := d.addIndex(ctx, c.name, toAdd); err != nil {
				return stacktrace.Propagate(err, "")
			}
		}
	}
	return nil
}

func (d *coreImplementation) addIndex(ctx context.Context, collection string, index Index) error {

	c, ok := d.coll(collection)
	if !ok {
		return stacktrace.NewErrorWithCode(ErrTODO, "collection is not registered: %s", collection)
	}
	d.isBuilding.Store(fmt.Sprintf("%s/%s", collection, index.Name), true)
	c.indexes[index.Name] = index
	d.collections.Store(collection, c)
	batch := d.kv.Batch()
	meta, _ := GetContext(ctx)
	meta.Set("_internal", true)
	_, err := d.Scan(meta.ToContext(ctx), collection, Scan{
		Filter:  nil,
		Reverse: false,
	}, func(doc *Document) (bool, error) {
		if err := d.indexDocument(ctx, batch, collection, &DocChange{
			Action: Set,
			DocID:  doc.GetString(c.PrimaryKey()),
			After:  doc,
		}); err != nil {
			return false, stacktrace.Propagate(err, "")
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	d.isBuilding.Store(fmt.Sprintf("%s/%s", collection, index.Name), false)
	if err := d.setIndexes(collection, c.indexes); err != nil {
		return stacktrace.Propagate(err, "")
	}
	return nil
}

func (d *coreImplementation) removeIndex(ctx context.Context, collection string, index Index) error {
	c, ok := d.coll(collection)
	if !ok {
		return nil
	}
	delete(c.indexes, index.Name)
	d.collections.Store(collection, c)
	d.machine.Go(ctx, func(ctx context.Context) error {
		batch := d.kv.Batch()
		_, err := d.Scan(ctx, collection, Scan{
			Filter:  nil,
			Reverse: false,
		}, func(doc *Document) (bool, error) {
			if err := d.updateSecondaryIndex(ctx, batch, collection, index, &DocChange{
				Action: Delete,
				DocID:  doc.GetString(c.PrimaryKey()),
				Before: doc,
			}); err != nil {
				return false, stacktrace.Propagate(err, "")
			}
			return true, nil
		})
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		return nil
	})
	if err := d.setIndexes(collection, c.indexes); err != nil {
		return stacktrace.Propagate(err, "")
	}
	return nil
}

func (d *coreImplementation) Indexes(ctx context.Context, collection string) []string {
	c, ok := d.coll(collection)
	if !ok {
		return nil
	}
	var names []string
	for k, _ := range c.indexes {
		names = append(names, k)
	}
	return names
}

func (d *coreImplementation) setIndexes(collection string, indexes map[string]Index) error {
	bits, err := json.Marshal(&indexes)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := d.kv.Tx(true, func(tx kv.Tx) error {
		err := tx.Set([]byte(fmt.Sprintf("internal.indexing.%s", collection)), bits)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (d *coreImplementation) getIndexes(collection string) (map[string]Index, error) {
	var (
		indexing = map[string]Index{}
	)
	if err := d.kv.Tx(false, func(tx kv.Tx) error {
		indexes, err := tx.Get([]byte(fmt.Sprintf("internal.indexing.%s", collection)))
		if err != nil {
			return nil
		}
		if err := json.Unmarshal(indexes, &indexing); err != nil {
			return stacktrace.Propagate(err, "failed to decode indexing")
		}
		return nil
	}); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return indexing, nil
}

func (d *coreImplementation) getAllCollection(ctx context.Context, collection string, ids []string) (Documents, error) {
	c, ok := d.coll(collection)
	if !ok {
		return Documents{}, stacktrace.NewErrorWithCode(ErrTODO, "collection is not registered: %s", collection)
	}
	var documents []*Document
	primaryIndex, ok := d.getPrimaryIndex(collection)
	if !ok {
		return Documents{}, stacktrace.NewErrorWithCode(ErrTODO, "collection is missing primary index: %s", collection)
	}
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		for _, id := range ids {
			value, err := txn.Get(primaryIndex.Seek(map[string]any{
				c.primaryKey: id,
			}).SetDocumentID(id).Path())
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

func (d *coreImplementation) getCollection(ctx context.Context, collection string, id string) (*Document, error) {
	var (
		document *Document
	)
	c, ok := d.coll(collection)
	if !ok {
		return nil, stacktrace.NewErrorWithCode(ErrTODO, "collection is not registered: %s", collection)
	}
	primaryIndex, ok := d.getPrimaryIndex(collection)
	if !ok {
		return nil, stacktrace.NewErrorWithCode(ErrTODO, "collection is missing primary index: %s", collection)
	}
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		val, err := txn.Get(primaryIndex.Seek(map[string]any{
			c.primaryKey: id,
		}).SetDocumentID(id).Path())
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		document, err = NewDocumentFromBytes(val)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		return nil
	}); err != nil {
		return document, stacktrace.Propagate(err, "")
	}
	return document, nil
}

func (d *coreImplementation) Persist(ctx context.Context, collection string, change StateChange) error {
	c, ok := d.coll(collection)
	if !ok {
		return stacktrace.NewErrorWithCode(ErrTODO, "collection is not registered: %s", collection)
	}
	txn := d.kv.Batch()
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
			if err := d.indexDocument(ctx, txn, collection, &DocChange{
				Action: Update,
				DocID:  id,
				Before: before,
				After:  after,
			}); err != nil {
				return stacktrace.Propagate(err, "")
			}
		}
	}
	for _, id := range change.Deletes {
		before, _ := d.getCollection(ctx, collection, id)
		if err := d.indexDocument(ctx, txn, collection, &DocChange{
			Action: Delete,
			DocID:  id,
			Before: before,
			After:  nil,
		}); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	for _, after := range change.Creates {
		if !after.Valid() {
			return stacktrace.NewErrorWithCode(ErrTODO, "invalid json document")
		}
		docID := c.GetPrimaryKey(after)
		if docID == "" {
			return stacktrace.NewErrorWithCode(ErrTODO, "document missing primary key %s", c.PrimaryKey())
		}
		before, _ := d.getCollection(ctx, collection, docID)
		if before != nil {
			return stacktrace.NewErrorWithCode(ErrTODO, "document already exists %s", docID)
		}
		if err := d.indexDocument(ctx, txn, collection, &DocChange{
			Action: Create,
			DocID:  docID,
			Before: nil,
			After:  after,
		}); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	for _, after := range change.Sets {
		if !after.Valid() {
			return stacktrace.NewErrorWithCode(ErrTODO, "invalid json document")
		}
		docID := c.GetPrimaryKey(after)
		if docID == "" {
			return stacktrace.NewErrorWithCode(ErrTODO, "document missing primary key %s", c.PrimaryKey())
		}
		before, _ := d.getCollection(ctx, collection, docID)
		if err := d.indexDocument(ctx, txn, collection, &DocChange{
			Action: Set,
			DocID:  docID,
			Before: before,
			After:  after,
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

func (d *coreImplementation) indexDocument(ctx context.Context, txn kv.Batch, collection string, change *DocChange) error {
	c, ok := d.coll(collection)
	if !ok {
		return stacktrace.NewErrorWithCode(ErrTODO, "collection is not registered: %s", collection)
	}
	if change.DocID == "" {
		return stacktrace.NewErrorWithCode(ErrTODO, "empty document id")
	}
	var err error
	primaryIndex, ok := d.getPrimaryIndex(collection)
	if !ok {
		return stacktrace.NewErrorWithCode(ErrTODO, "collection is missing primary index: %s", collection)
	}
	if change.After != nil {
		if c.GetPrimaryKey(change.After) != change.DocID {
			return stacktrace.NewErrorWithCode(ErrTODO, "document id is immutable: %v -> %v", c.GetPrimaryKey(change.After), change.DocID)
		}
		if err := c.Validate(ctx, d, change); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	change, err = c.SideAffects(ctx, d, change)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	switch change.Action {
	case Delete:
		if err := txn.Delete(primaryIndex.Seek(map[string]any{
			c.primaryKey: change.DocID,
		}).SetDocumentID(change.DocID).Path()); err != nil {
			return stacktrace.Propagate(err, "failed to batch delete documents")
		}
	case Set, Update, Create:
		if err := txn.Set(primaryIndex.Seek(map[string]any{
			c.primaryKey: change.DocID,
		}).SetDocumentID(change.DocID).Path(), change.After.Bytes()); err != nil {
			return stacktrace.PropagateWithCode(err, ErrTODO, "failed to batch set documents to primary index")
		}
	}
	for _, i := range c.indexes {
		if i.Primary {
			continue
		}
		if err := d.updateSecondaryIndex(ctx, txn, collection, i, change); err != nil {
			return stacktrace.PropagateWithCode(err, ErrTODO, "")
		}
	}
	return nil
}

func (d *coreImplementation) updateSecondaryIndex(ctx context.Context, txn kv.Batch, collection string, idx Index, change *DocChange) error {
	if idx.Primary {
		return nil
	}
	c, ok := d.coll(collection)
	if !ok {
		return stacktrace.NewErrorWithCode(ErrTODO, "collection is not registered: %s", collection)
	}

	switch change.Action {
	case Delete:
		if err := txn.Delete(idx.Seek(change.Before.Value()).SetDocumentID(change.DocID).Path()); err != nil {
			return stacktrace.PropagateWithCode(
				err,
				ErrTODO,
				"failed to delete document %s/%s index references",
				c.Name(),
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
					c.Name(),
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
					c.Name(),
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
				c.Name(),
				change.DocID,
			)
		}
	}
	return nil
}

func (d *coreImplementation) hasCollection(ctx context.Context, collection string) bool {
	_, ok := d.coll(collection)
	return ok
}

func (c *coreImplementation) getReadyIndexes(ctx context.Context, collection string) map[string]Index {
	var indexes = map[string]Index{}
	coll, ok := c.coll(collection)
	if !ok {
		return map[string]Index{}
	}
	for _, i := range coll.indexes {
		if val, ok := c.isBuilding.Load(fmt.Sprintf("%s/%s", collection, i.Name)); ok && val == true {
			continue
		}
		indexes[i.Name] = i
	}
	return indexes
}

func (d *coreImplementation) Scan(ctx context.Context, collection string, scan Scan, handlerFunc ScanFunc) (IndexMatch, error) {
	coll, ok := d.coll(collection)
	if !ok {
		return IndexMatch{}, stacktrace.NewErrorWithCode(ErrTODO, "collection is not registered: %s", collection)
	}
	if handlerFunc == nil {
		return IndexMatch{}, stacktrace.NewError("empty scan handler")
	}
	if coll.whereHooks != nil {
		for _, w := range coll.whereHooks {
			filters, err := w(ctx, d, scan.Filter)
			if err != nil {
				return IndexMatch{}, stacktrace.Propagate(err, "")
			}
			scan.Filter = filters
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var index IndexMatch
	index, err := d.optimizer.BestIndex(d.getReadyIndexes(ctx, collection), scan.Filter, OrderBy{})
	if err != nil {
		return IndexMatch{}, stacktrace.Propagate(err, "")
	}
	pfx := index.Ref.Seek(index.Values)
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		opts := kv.IterOpts{
			Prefix:  pfx.Path(),
			Seek:    nil,
			Reverse: scan.Reverse,
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
				document, err = d.getCollection(ctx, collection, string(id))
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
			}

			pass, err := document.Where(scan.Filter)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if pass {
				for _, rhook := range coll.readHooks {
					document, err = rhook(ctx, d, document)
					if err != nil {
						return stacktrace.Propagate(err, "")
					}
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

func (d *coreImplementation) Close(ctx context.Context) error {
	return stacktrace.Propagate(d.kv.Close(), "")
}
