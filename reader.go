package wolverine

import (
	"context"
	"fmt"
	"sort"

	"github.com/dgraph-io/badger/v3"
	"github.com/tidwall/gjson"

	"github.com/autom8ter/wolverine/internal/prefix"
)

func (d *db) Query(ctx context.Context, collection string, query Query) ([]*Document, error) {
	_, ok := d.collections[collection]
	if !ok {
		return nil, fmt.Errorf("unsupported collection: %s", collection)
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
					if d.config.OnRead != nil {
						if err := d.config.OnRead(d, ctx, document); err != nil {
							return d.wrapErr(err, "")
						}
					}
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
	if len(documents) > query.Limit {
		return documents[:query.Limit], nil
	}
	return documents, nil
}

func (d *db) Get(ctx context.Context, collection, id string) (*Document, error) {
	if _, ok := d.collections[collection]; !ok {
		return nil, fmt.Errorf("unsupported collection: %s", collection)
	}
	var (
		record *Document
	)

	if err := d.kv.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(prefix.PrimaryKey(collection, id)))
		if err != nil {
			return d.wrapErr(err, "")
		}
		return item.Value(func(val []byte) error {
			record, err = NewDocumentFromBytes(val)
			return d.wrapErr(err, "")
		})
	}); err != nil {
		return record, err
	}
	if d.config.OnRead != nil {
		if err := d.config.OnRead(d, ctx, record); err != nil {
			return record, err
		}
	}
	return record, nil
}

func (d *db) GetAll(ctx context.Context, collection string, ids []string) ([]*Document, error) {
	if _, ok := d.collections[collection]; !ok {
		return nil, fmt.Errorf("unsupported collection: %s", collection)
	}
	var documents []*Document
	if err := d.kv.View(func(txn *badger.Txn) error {
		for _, id := range ids {

			item, err := txn.Get([]byte(fmt.Sprintf("%s.%s", collection, id)))
			if err != nil {
				return d.wrapErr(err, "")
			}
			if err := item.Value(func(val []byte) error {
				record, err := NewDocumentFromBytes(val)
				if err != nil {
					return d.wrapErr(err, "")
				}
				if d.config.OnRead != nil {
					if err := d.config.OnRead(d, ctx, record); err != nil {
						return d.wrapErr(err, "")
					}
				}
				documents = append(documents, record)
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
