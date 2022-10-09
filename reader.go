package wolverine

import (
	"context"
	"fmt"
	"sort"

	"github.com/dgraph-io/badger/v3"
	"github.com/tidwall/gjson"

	"github.com/autom8ter/wolverine/internal/prefix"
)

// WhereOp is an operator used to compare a value to a records field value in a where clause
type WhereOp string

// IsSearch returns true if the operator requires full text search
func (w WhereOp) IsSearch() bool {
	switch w {
	case Contains, Fuzzy, Prefix, Term:
		return true
	default:
		return false
	}
}

const (
	// PrefixOp is a full text search type for finding records based on prefix matching. full text search operators can only be used
	// against collections that have full text search enabled
	Prefix WhereOp = "prefix"
	// ContainsOp full text search type for finding records based on contains matching. full text search operators can only be used
	// against collections that have full text search enabled
	Contains WhereOp = "contains"
	// TermOp full text search type for finding records based on term matching. full text search operators can only be used
	// against collections that have full text search enabled
	Term WhereOp = "term"
	// FuzzyOp full text search type for finding records based on a fuzzy search. full text search operators can only be used
	// against collections that have full text search enabled
	Fuzzy WhereOp = "fuzzy"
	// EqOp matches on equality
	Eq WhereOp = "eq"
	// NeqOp matches on inequality
	Neq WhereOp = "neq"
	// GtOp matches on greater than
	Gt WhereOp = "gt"
	// GteOp matches on greater than or equal to
	Gte WhereOp = "gte"
	// LtOp matches on less than
	Lt WhereOp = "lt"
	// LteOp matches on greater than or equal to
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

// Query is a query against the NOSQL database
type Query struct {
	Select  []string `json:"select"`
	Where   []Where  `json:"where"`
	StartAt string   `json:"start_at"`
	Limit   int      `json:"limit"`
	OrderBy OrderBy  `json:"order_by"`
}

func (d *db) Query(ctx context.Context, collection string, query Query) ([]*Document, error) {
	if _, ok := d.getInmemCollection(collection); !ok {
		return nil, d.wrapErr(fmt.Errorf("unsupported collection: %s", collection), "")
	}
	if d.isSearchQuery(collection, query) {
		return d.search(ctx, collection, query)
	}
	prefix := d.getQueryPrefix(collection, query.Where)
	var documents []*Document
	if err := d.kv.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 10
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		seek := prefix
		if query.StartAt != "" {
			seek = []byte(fmt.Sprintf("%s.%s", string(prefix), query.StartAt))
		}

		it.Seek(seek)
		defer it.Close()
		for it.ValidForPrefix(prefix) {
			if query.Limit > 0 && len(documents) >= query.Limit {
				return nil
			}
			item := it.Item()
			err := item.Value(func(bits []byte) error {
				document, err := NewDocumentFromBytes(bits)
				if err != nil {
					return d.wrapErr(err, "")
				}
				pass, err := document.Where(query.Where)
				if err != nil {
					return d.wrapErr(err, "")
				}
				if pass {
					if err := document.Validate(); err != nil {
						return d.wrapErr(err, "")
					}
					documents = append(documents, document)
					documents = orderBy(query.OrderBy, query.Limit, documents)
				}
				return nil
			})
			if err != nil {
				return d.wrapErr(err, "")
			}
			it.Next()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	documents = orderBy(query.OrderBy, query.Limit, documents)
	if len(query.Select) > 0 {
		for _, r := range documents {
			r.Select(query.Select)
		}
	}
	if query.Limit > 0 && len(documents) > query.Limit {
		return documents[:query.Limit], nil
	}
	return documents, nil
}

func (d *db) Get(ctx context.Context, collection, id string) (*Document, error) {
	if _, ok := d.getInmemCollection(collection); !ok {
		return nil, d.wrapErr(fmt.Errorf("unsupported collection: %s", collection), "")
	}
	var (
		document *Document
	)

	if err := d.kv.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(prefix.PrimaryKey(collection, id)))
		if err != nil {
			return d.wrapErr(err, "")
		}
		return item.Value(func(val []byte) error {
			document, err = NewDocumentFromBytes(val)
			return d.wrapErr(err, "")
		})
	}); err != nil {
		return document, err
	}
	return document, nil
}

func (d *db) GetAll(ctx context.Context, collection string, ids []string) ([]*Document, error) {
	if _, ok := d.getInmemCollection(collection); !ok {
		return nil, d.wrapErr(fmt.Errorf("unsupported collection: %s", collection), "")
	}
	var documents []*Document
	if err := d.kv.View(func(txn *badger.Txn) error {
		for _, id := range ids {
			item, err := txn.Get([]byte(prefix.PrimaryKey(collection, id)))
			if err != nil {
				return d.wrapErr(err, "")
			}
			if err := item.Value(func(val []byte) error {
				document, err := NewDocumentFromBytes(val)
				if err != nil {
					return d.wrapErr(err, "")
				}
				documents = append(documents, document)
				return nil
			}); err != nil {
				return d.wrapErr(err, "")
			}
		}
		return nil
	}); err != nil {
		return documents, err
	}
	return documents, nil
}

func orderBy(orderBy OrderBy, limit int, documents []*Document) []*Document {
	if orderBy.Field == "" {
		return documents
	}
	if orderBy.Direction == DESC {
		sort.Slice(documents, func(i, j int) bool {
			return compareField(orderBy.Field, documents[i], documents[j])
		})
	} else {
		sort.Slice(documents, func(i, j int) bool {
			return !compareField(orderBy.Field, documents[i], documents[j])
		})
	}
	if limit > 0 && len(documents) > limit {
		return documents[:limit]
	}
	return documents
}

func compareField(field string, i, j *Document) bool {
	iFieldVal := i.result.Get(field)
	jFieldVal := j.result.Get(field)
	switch i.result.Get(field).Type {
	case gjson.Null:
		return false
	case gjson.False:
		return iFieldVal.Bool() && !jFieldVal.Bool()
	case gjson.Number:
		return iFieldVal.Float() > jFieldVal.Float()
	case gjson.String:
		return iFieldVal.String() > jFieldVal.String()
	default:
		return iFieldVal.String() > jFieldVal.String()
	}
}
