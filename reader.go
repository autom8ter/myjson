package wolverine

import (
	"context"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/schema"
	"time"

	"github.com/autom8ter/machine/v4"
	"github.com/dgraph-io/badger/v3"
	"github.com/palantir/stacktrace"
	"github.com/reactivex/rxgo/v2"

	"github.com/autom8ter/wolverine/internal/prefix"
)

func (d *db) Query(ctx context.Context, collection string, query schema.Query) (schema.Page, error) {
	now := time.Now()
	qmachine := machine.New()
	c, ok := d.getInmemCollection(collection)
	if !ok {
		return schema.Page{}, stacktrace.NewErrorWithCode(errors.ErrUnsuportedCollection, "unsupported collection: %s must be one of: %v", collection, d.schema.CollectionNames())
	}
	index, err := c.OptimizeQueryIndex(query.Where, query.OrderBy)
	if err != nil {
		return schema.Page{}, stacktrace.Propagate(err, "")
	}
	var (
		input = make(chan rxgo.Item)
	)
	qmachine.Go(ctx, func(ctx context.Context) error {
		if err := d.kv.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = true
			opts.PrefetchSize = 10
			opts.Prefix = index.Ref.GetPrefix(schema.IndexableFields(query.Where, query.OrderBy), "")
			seek := opts.Prefix

			if query.OrderBy.Direction == schema.DESC {
				opts.Reverse = true
				seek = prefix.PrefixNextKey(opts.Prefix)
			}
			it := txn.NewIterator(opts)
			it.Seek(seek)
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
			return stacktrace.Propagate(err, "")
		}
		close(input)
		return nil
	})
	var results []*schema.Document
	for result := range query.Observe(ctx, input, index.FullScan()).Observe() {
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

func (d *db) Get(ctx context.Context, collection, id string) (*schema.Document, error) {
	if _, ok := d.getInmemCollection(collection); !ok {
		return nil, stacktrace.Propagate(stacktrace.NewError("unsupported collection: %s must be one of: %v", collection, d.schema.CollectionNames()), "")
	}
	var (
		document *schema.Document
	)

	if err := d.kv.View(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix.PrimaryKey(collection, id))
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		return item.Value(func(val []byte) error {
			document, err = schema.NewDocumentFromBytes(val)
			return stacktrace.Propagate(err, "")
		})
	}); err != nil {
		return document, err
	}
	return document, nil
}

func (d *db) GetAll(ctx context.Context, collection string, ids []string) ([]*schema.Document, error) {
	if _, ok := d.getInmemCollection(collection); !ok {
		return nil, stacktrace.Propagate(stacktrace.NewError("unsupported collection: %s must be one of: %v", collection, d.schema.CollectionNames()), "")
	}
	var documents []*schema.Document
	if err := d.kv.View(func(txn *badger.Txn) error {
		for _, id := range ids {
			item, err := txn.Get([]byte(prefix.PrimaryKey(collection, id)))
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if err := item.Value(func(val []byte) error {
				document, err := schema.NewDocumentFromBytes(val)
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				documents = append(documents, document)
				return nil
			}); err != nil {
				return stacktrace.Propagate(err, "")
			}
		}
		return nil
	}); err != nil {
		return documents, err
	}
	return documents, nil
}

// QueryPaginate paginates through each page of the query until the handlePage function returns false or there are no more results
func (d *db) QueryPaginate(ctx context.Context, collection string, query schema.Query, handlePage schema.PageHandler) error {
	page := query.Page
	for {
		results, err := d.Query(ctx, collection, schema.Query{
			Select:  query.Select,
			Where:   query.Where,
			Page:    page,
			Limit:   query.Limit,
			OrderBy: query.OrderBy,
		})
		if err != nil {
			return stacktrace.Propagate(err, "failed to query collection: %s", collection)
		}
		if len(results.Documents) == 0 {
			return nil
		}
		if !handlePage(results) {
			return nil
		}
		page++
	}
}
