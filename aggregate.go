package wolverine

import (
	"context"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/schema"
	"github.com/dgraph-io/badger/v3"
	"github.com/palantir/stacktrace"
	"github.com/reactivex/rxgo/v2"
	"time"
)

func (d *db) aggregateIndex(ctx context.Context, i *schema.AggregateIndex, query schema.AggregateQuery) (schema.Page, error) {
	now := time.Now()
	input := make(chan rxgo.Item)
	go func() {
		results := i.Aggregate(query.Aggregates...)
		results = schema.SortOrder(query.OrderBy, results)
		for _, result := range results {
			input <- rxgo.Of(result)
		}
		close(input)
	}()
	pipe, err := query.pipeIndex(ctx, input)
	if err != nil {
		return schema.Page{}, stacktrace.Propagate(err, "")
	}
	var results []*schema.Document
	for result := range pipe.Observe() {
		doc, ok := result.V.(*schema.Document)
		if !ok {
			return schema.Page{}, stacktrace.NewError("expected type: %T got: %#v", &schema.Document{}, result.V)
		}
		results = append(results, doc)
	}

	return schema.Page{
		Documents: results,
		NextPage:  query.Page + 1,
		Count:     len(results),
		Stats: schema.PageStats{
			ExecutionTime: time.Since(now),
			IndexMatch:    schema.QueryIndexMatch{},
		},
	}, nil
}

func (d *db) Aggregate(ctx context.Context, collection string, query schema.AggregateQuery) (schema.Page, error) {
	c, ok := d.getInmemCollection(collection)
	if !ok {
		return schema.Page{}, stacktrace.NewErrorWithCode(errors.ErrUnsuportedCollection, "unsupported collection: %s must be one of: %v", collection, d.collectionNames())
	}
	indexes, ok := d.aggIndexes.Load(collection)
	if ok {
		for _, i := range indexes.([]*schema.AggregateIndex) {
			if i.Matches(query) {
				return d.aggregateIndex(ctx, i, query)
			}
		}
	}

	now := time.Now()
	var (
		input = make(chan rxgo.Item)
	)
	index, err := c.OptimizeQueryIndex(query.Where, query.OrderBy)
	if err != nil {
		return schema.Page{}, stacktrace.Propagate(err, "")
	}

	go func() {
		if err := d.kv.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = true
			opts.PrefetchSize = 10
			opts.Prefix = index.Ref.GetPrefix(schema.IndexableFields(query.Where, query.OrderBy), "")
			it := txn.NewIterator(opts)
			it.Seek(opts.Prefix)
			defer it.Close()
			for it.ValidForPrefix(opts.Prefix) {
				if ctx.Err() != nil {
					return nil
				}
				item := it.Item()
				err := item.Value(func(bits []byte) error {
					document, err := schema.NewDocumentFromBytes(bits)
					if err != nil {
						return stacktrace.Propagate(err, "")
					}
					input <- rxgo.Of(document)
					return nil
				})
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				it.Next()
			}
			return nil
		}); err != nil {
			close(input)
			panic(err)
		}
		close(input)
	}()

	pipe, err := query.Observe(ctx, input, index.FullScan())
	if err != nil {
		return schema.Page{}, stacktrace.Propagate(err, "")
	}
	var results []*schema.Document
	for result := range pipe.Observe() {
		doc, ok := result.V.(*schema.Document)
		if !ok {
			return schema.Page{}, stacktrace.NewError("expected type: %T got: %#v", &schema.Document{}, result.V)
		}
		results = append(results, doc)
	}
	return schema.Page{
		Documents: results,
		NextPage:  query.Page + 1,
		Count:     len(results),
		Stats: schema.PageStats{
			ExecutionTime: time.Since(now),
			IndexMatch:    index,
		},
	}, nil
}
