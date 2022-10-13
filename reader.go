package wolverine

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/palantir/stacktrace"

	"github.com/autom8ter/wolverine/internal/prefix"
)

// WhereOp is an operator used to compare a value to a records field value in a where clause
type WhereOp string

const (
	// Eq matches on equality
	Eq WhereOp = "eq"
	// Neq matches on inequality
	Neq WhereOp = "neq"
	// Gt matches on greater than
	Gt WhereOp = "gt"
	// Gte matches on greater than or equal to
	Gte WhereOp = "gte"
	// Lt matches on less than
	Lt WhereOp = "lt"
	// Lte matches on greater than or equal to
	Lte WhereOp = "lte"
)

// Where is field-level filter for database queries
type Where struct {
	// Field is a field to compare against records field. For full text search, wrap the field in search(field1,field2,field3) and use a search operator
	Field string `json:"field"`
	// Op is an operator used to compare the field against the value.
	Op WhereOp `json:"op"`
	// Value is a value to compare against a records field value
	Value interface{} `json:"value"`
}

// OrderByDirection indicates whether results should be sorted in ascending or descending order
type OrderByDirection string

const (
	// ASC indicates ascending order
	ASC OrderByDirection = "ASC"
	// DESC indicates descending order
	DESC OrderByDirection = "DESC"
)

// OrderBy orders the result set by a given field in a given direction
type OrderBy struct {
	Field     string           `json:"field"`
	Direction OrderByDirection `json:"direction"`
}

// Query is a query against the NOSQL database - it does not support full text search
type Query struct {
	// Select is a list of fields to select from each record in the datbase(optional)
	Select []string `json:"select"`
	// Where is a list of where clauses used to filter records
	Where   []Where `json:"where"`
	Page    int     `json:"page"`
	Limit   int     `json:"limit"`
	OrderBy OrderBy `json:"order_by"`
}

func (d *db) Query(ctx context.Context, collection string, query Query) (Page, error) {
	now := time.Now()
	if collection != systemCollection {
		if _, ok := d.getInmemCollection(collection); !ok {
			return Page{}, stacktrace.Propagate(stacktrace.NewError("unsupported collection: %s must be one of: %v", collection, d.collectionNames()), "")
		}
	}
	pfx, _ := d.getQueryPrefix(collection, query.Where)

	var documents []*Document
	if err := d.kv.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 10
		opts.Prefix = pfx
		it := txn.NewIterator(opts)
		it.Seek(pfx)
		defer it.Close()
		for it.ValidForPrefix(pfx) {
			item := it.Item()
			err := item.Value(func(bits []byte) error {
				document, err := NewDocumentFromBytes(bits)
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				pass, err := document.Where(query.Where)
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				if pass {
					if err := document.Validate(); err != nil {
						return stacktrace.Propagate(err, "")
					}
					documents = append(documents, document)
					documents = orderBy(query.OrderBy, documents)
				}
				return nil
			})
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			it.Next()
		}
		return nil
	}); err != nil {
		return Page{}, stacktrace.Propagate(err, "")
	}
	documents = orderBy(query.OrderBy, documents)
	documents, _ = prunePage(query.Page, query.Limit, documents)
	if len(query.Select) > 0 {
		for _, r := range documents {
			r.Select(query.Select)
		}
	}
	return Page{
		Documents: documents,
		NextPage:  query.Page + 1,
		Count:     len(documents),
		Stats: Stats{
			ExecutionTime: time.Since(now),
		},
	}, nil
}

func (d *db) Get(ctx context.Context, collection, id string) (*Document, error) {
	if _, ok := d.getInmemCollection(collection); !ok {
		return nil, stacktrace.Propagate(stacktrace.NewError("unsupported collection: %s must be one of: %v", collection, d.collectionNames()), "")
	}
	var (
		document *Document
	)

	if err := d.kv.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(prefix.PrimaryKey(collection, id)))
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		return item.Value(func(val []byte) error {
			document, err = NewDocumentFromBytes(val)
			return stacktrace.Propagate(err, "")
		})
	}); err != nil {
		return document, err
	}
	return document, nil
}

func (d *db) GetAll(ctx context.Context, collection string, ids []string) ([]*Document, error) {
	if _, ok := d.getInmemCollection(collection); !ok {
		return nil, stacktrace.Propagate(stacktrace.NewError("unsupported collection: %s must be one of: %v", collection, d.collectionNames()), "")
	}
	var documents []*Document
	if err := d.kv.View(func(txn *badger.Txn) error {
		for _, id := range ids {
			item, err := txn.Get([]byte(prefix.PrimaryKey(collection, id)))
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if err := item.Value(func(val []byte) error {
				document, err := NewDocumentFromBytes(val)
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
func (d *db) QueryPaginate(ctx context.Context, collection string, query Query, handlePage PageHandler) error {
	page := query.Page
	for {
		results, err := d.Query(ctx, collection, Query{
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
		page = results.NextPage
	}
}
