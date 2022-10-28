package coreimp

import (
	"context"
	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/wolverine/core"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/internal/prefix"
	"github.com/autom8ter/wolverine/kv"
	"github.com/autom8ter/wolverine/kv/badger"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"strings"
	"time"
)

type defaultStore struct {
	kv       kv.DB
	fullText map[string]bleve.Index
	machine  machine.Machine
}

// Default creates a new Core instance with the given storeage path and collections. Under the hood it is powered by BadgerDB and Blevesearch
func Default(storagePath string, collections []*core.Collection, middlewares ...core.Middleware) (core.Core, error) {
	db, err := badger.New(storagePath)
	if err != nil {
		return core.Core{}, stacktrace.Propagate(err, "")
	}
	d := defaultStore{
		kv:       db,
		fullText: map[string]bleve.Index{},
		machine:  machine.New(),
	}
	for _, collection := range collections {
		if collection == nil {
			panic("null collection")
		}
		if collection.Indexing().SearchEnabled {
			idx, err := openFullTextIndex(storagePath, collection, false)
			if err != nil {
				return core.Core{}, stacktrace.Propagate(err, "")
			}
			d.fullText[collection.Collection()] = idx
		}
	}
	c := core.Core{}.
		WithPersist(d.persistCollection).
		WithAggregate(d.aggregateCollection).
		WithSearch(d.searchCollection).
		WithQuery(d.queryCollection).
		WithGet(d.getCollection).
		WithGetAll(d.getAllCollection).
		WithChangeStream(d.changeStreamCollection).
		WithClose(d.closeAll)
	for _, mw := range middlewares {
		c = c.Apply(mw)
	}
	return c, nil
}

func (d defaultStore) changeStreamCollection(ctx context.Context, collection *core.Collection, fn core.ChangeStreamHandler) error {
	return d.machine.Subscribe(ctx, collection.Collection(), func(ctx context.Context, msg machine.Message) (bool, error) {
		switch change := msg.Body.(type) {
		case *core.StateChange:
			if err := fn(ctx, *change); err != nil {
				return false, stacktrace.Propagate(err, "")
			}
		case core.StateChange:
			if err := fn(ctx, change); err != nil {
				return false, stacktrace.Propagate(err, "")
			}
		}
		return true, nil
	})
}

func (d defaultStore) aggregateCollection(ctx context.Context, collection *core.Collection, query core.AggregateQuery) (core.Page, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()
	index, err := collection.OptimizeIndex(query.Where, query.OrderBy)
	if err != nil {
		return core.Page{}, stacktrace.Propagate(err, "")
	}
	var results []*core.Document
	if err := d.kv.Tx(false, func(tx kv.Tx) error {
		opts := kv.IterOpts{
			Prefix:  index.Ref.GetPrefix(core.IndexableFields(query.Where, query.OrderBy), ""),
			Seek:    nil,
			Reverse: false,
		}
		if query.OrderBy.Direction == core.DESC {
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
			document, err := core.NewDocumentFromBytes(bits)
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
		return core.Page{}, stacktrace.Propagate(err, "")
	}
	grouped := lo.GroupBy[*core.Document](results, func(d *core.Document) string {
		var values []string
		for _, g := range query.GroupBy {
			values = append(values, cast.ToString(d.Get(g)))
		}
		return strings.Join(values, ".")
	})
	var reduced []*core.Document
	for _, values := range grouped {
		value, err := core.ApplyReducers(ctx, query, values)
		if err != nil {
			return core.Page{}, stacktrace.Propagate(err, "")
		}
		reduced = append(reduced, value)
	}
	reduced = core.SortOrder(query.OrderBy, reduced)
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
			return core.Page{}, stacktrace.Propagate(err, "")
		}
	}
	return core.Page{
		Documents: reduced,
		NextPage:  query.Page + 1,
		Count:     len(reduced),
		Stats: core.PageStats{
			ExecutionTime: time.Since(now),
			IndexMatch:    index,
		},
	}, nil
}

func (d defaultStore) getAllCollection(ctx context.Context, collection *core.Collection, ids []string) (core.Documents, error) {
	var documents []*core.Document
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

			document, err := core.NewDocumentFromBytes(value)
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

func (d defaultStore) getCollection(ctx context.Context, collection *core.Collection, id string) (*core.Document, error) {
	var (
		document *core.Document
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
		document, err = core.NewDocumentFromBytes(val)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		return nil
	}); err != nil {
		return document, err
	}
	return document, nil
}

func (d defaultStore) persistCollection(ctx context.Context, collection *core.Collection, change core.StateChange) error {
	txn := d.kv.Batch()
	var batch *bleve.Batch
	if collection == nil {
		return stacktrace.NewErrorWithCode(errors.ErrTODO, "null collection schema")
	}
	if collection.Indexing().SearchEnabled && d.fullText[collection.Collection()] != nil {
		batch = d.fullText[collection.Collection()].NewBatch()
	}
	if change.Updates != nil {
		for id, edit := range change.Updates {
			before, _ := d.getCollection(ctx, collection, id)
			if !before.Valid() {
				before = core.NewDocument()
			}
			after := before.Clone()
			err := after.SetAll(edit)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if err := d.indexDocument(ctx, txn, batch, collection, &singleChange{
				action: core.Update,
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
		if err := d.indexDocument(ctx, txn, batch, collection, &singleChange{
			action: core.Delete,
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
		if err := d.indexDocument(ctx, txn, batch, collection, &singleChange{
			action: core.Set,
			docId:  docId,
			before: before,
			after:  after,
		}); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}

	if batch != nil {
		if err := d.fullText[collection.Collection()].Batch(batch); err != nil {
			return stacktrace.Propagate(err, "failed to batch collection documents")
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
	action core.Action
	docId  string
	before *core.Document
	after  *core.Document
}

func (d defaultStore) indexDocument(ctx context.Context, txn kv.Batch, batch *bleve.Batch, collection *core.Collection, change *singleChange) error {
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
	case core.Delete:
		if !change.before.Valid() {
			return stacktrace.NewError("invalid document")
		}
		if err := txn.Delete(pkey); err != nil {
			return stacktrace.Propagate(err, "failed to batch delete documents")
		}
		if batch != nil {
			batch.Delete(change.docId)
		}
	case core.Set, core.Update:
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
		if batch != nil {
			if err := batch.Index(change.docId, change.after.Value()); err != nil {
				return stacktrace.PropagateWithCode(err, errors.ErrTODO, "failed to batch set documents to search index")
			}
		}
	}
	return nil
}

func (d defaultStore) updateSecondaryIndex(ctx context.Context, txn kv.Batch, collection *core.Collection, idx *core.Index, change *singleChange) error {
	switch change.action {
	case core.Delete:
		pindex := prefix.NewPrefixedIndex(collection.Collection(), idx.Fields)
		if err := txn.Delete(pindex.GetPrefix(change.before.Value(), change.docId)); err != nil {
			return stacktrace.Propagate(err, "failed to delete index references")
		}
	case core.Set, core.Update:
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

func (d defaultStore) queryCollection(ctx context.Context, collection *core.Collection, query core.Query) (core.Page, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()
	index, err := collection.OptimizeIndex(query.Where, query.OrderBy)
	if err != nil {
		return core.Page{}, stacktrace.Propagate(err, "")
	}
	var results []*core.Document
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		opts := kv.IterOpts{
			Prefix:  index.Ref.GetPrefix(core.IndexableFields(query.Where, query.OrderBy), ""),
			Seek:    nil,
			Reverse: false,
		}
		if query.OrderBy.Direction == core.DESC {
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
			document, err := core.NewDocumentFromBytes(bits)
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
		return core.Page{}, stacktrace.Propagate(err, "")
	}
	results = core.SortOrder(query.OrderBy, results)

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
				return core.Page{}, stacktrace.Propagate(err, "")
			}
		}
	}

	return core.Page{
		Documents: results,
		NextPage:  query.Page + 1,
		Count:     len(results),
		Stats: core.PageStats{
			ExecutionTime: time.Since(now),
			IndexMatch:    index,
		},
	}, nil
}

func (d defaultStore) searchCollection(ctx context.Context, collection *core.Collection, q core.SearchQuery) (core.Page, error) {
	if !collection.Indexing().SearchEnabled {
		return core.Page{}, stacktrace.NewErrorWithCode(
			errors.ErrTODO,
			"%s does not have a search index",
			collection.Collection(),
		)
	}

	now := time.Now()
	var (
		fields []string
		limit  = q.Limit
	)
	for _, w := range q.Where {
		fields = append(fields, w.Field)
	}
	if limit == 0 {
		limit = 1000
	}
	var queries []query.Query
	for _, where := range q.Where {
		if where.Value == nil {
			return core.Page{}, stacktrace.NewError("empty where clause value")
		}
		switch where.Op {
		case core.Basic:
			switch where.Value.(type) {
			case bool:
				qry := bleve.NewBoolFieldQuery(cast.ToBool(where.Value))
				if where.Boost > 0 {
					qry.SetBoost(where.Boost)
				}
				qry.SetField(where.Field)
				queries = append(queries, qry)
			case float64, int, int32, int64, float32, uint64, uint, uint8, uint16, uint32:
				qry := bleve.NewNumericRangeQuery(lo.ToPtr(cast.ToFloat64(where.Value)), nil)
				if where.Boost > 0 {
					qry.SetBoost(where.Boost)
				}
				qry.SetField(where.Field)
				queries = append(queries, qry)
			default:
				qry := bleve.NewMatchQuery(cast.ToString(where.Value))
				if where.Boost > 0 {
					qry.SetBoost(where.Boost)
				}
				qry.SetField(where.Field)
				queries = append(queries, qry)
			}
		case core.Prefix:
			qry := bleve.NewPrefixQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case core.Fuzzy:
			qry := bleve.NewFuzzyQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case core.Regex:
			qry := bleve.NewRegexpQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case core.Wildcard:
			qry := bleve.NewWildcardQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		}
	}
	if len(queries) == 0 {
		queries = []query.Query{bleve.NewMatchAllQuery()}
	}
	var searchRequest *bleve.SearchRequest
	if len(queries) > 1 {
		searchRequest = bleve.NewSearchRequestOptions(bleve.NewConjunctionQuery(queries...), limit, q.Page*limit, false)
	} else {
		searchRequest = bleve.NewSearchRequestOptions(bleve.NewConjunctionQuery(queries[0]), limit, q.Page*limit, false)
	}
	searchRequest.Fields = []string{"*"}
	results, err := d.fullText[collection.Collection()].Search(searchRequest)
	if err != nil {
		return core.Page{}, stacktrace.Propagate(err, "failed to search index: %s", collection.Collection())
	}

	var data []*core.Document
	if len(results.Hits) == 0 {
		return core.Page{}, stacktrace.NewError("zero results")
	}
	for _, h := range results.Hits {
		if len(h.Fields) == 0 {
			continue
		}
		record, err := core.NewDocumentFrom(h.Fields)
		if err != nil {
			return core.Page{}, stacktrace.Propagate(err, "failed to search index: %s", collection.Collection())
		}
		pass, err := record.Where(q.Filter)
		if err != nil {
			return core.Page{}, stacktrace.Propagate(err, "")
		}
		if pass {
			data = append(data, record)
		}
	}

	if len(q.Select) > 0 && q.Select[0] != "*" {
		for _, r := range data {
			err := r.Select(q.Select)
			if err != nil {
				return core.Page{}, stacktrace.Propagate(err, "")
			}
		}
	}
	return core.Page{
		Documents: data,
		NextPage:  q.Page + 1,
		Count:     len(data),
		Stats: core.PageStats{
			ExecutionTime: time.Since(now),
		},
	}, nil
}

func (d defaultStore) forEach(ctx context.Context, collection *core.Collection, where []core.Where, reverse bool, fn func(d *core.Document) (bool, error)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	index, err := collection.OptimizeIndex(where, core.OrderBy{})
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		opts := kv.IterOpts{
			Prefix:  index.Ref.GetPrefix(core.IndexableFields(where, core.OrderBy{}), ""),
			Seek:    nil,
			Reverse: false,
		}
		if reverse {
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
			document, err := core.NewDocumentFromBytes(bits)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			pass, err := document.Where(where)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if pass {
				shouldContinue, err := fn(document)
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

func (d defaultStore) closeAll(ctx context.Context) error {
	for _, i := range d.fullText {
		if err := i.Close(); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	return stacktrace.Propagate(d.kv.Close(), "")
}
