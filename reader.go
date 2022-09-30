package wolverine

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/spf13/cast"
)

func (d *db) Query(ctx context.Context, collection string, query Query) ([]Record, error) {
	_, ok := d.collections[collection]
	if !ok {
		return nil, fmt.Errorf("unsupported collection: %s", collection)
	}
	if d.isSearchQuery(collection, query) {
		return d.search(ctx, collection, query)
	}
	prefix := d.getQueryPrefix(collection, query.Where)
	var records []Record
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
		for it.Valid() {
			if query.Limit > 0 && len(records) >= query.Limit*3 {
				return nil
			}
			item := it.Item()
			err := item.Value(func(bits []byte) error {
				record, err := NewRecordFromJSON(bits)
				if err != nil {
					return err
				}
				if record.Where(query.Where) {
					if d.config.OnRead != nil {
						if err := d.config.OnRead(d, ctx, record); err != nil {
							return err
						}
					}
					records = append(records, record)
					records = orderBy(query.OrderBy, query.Limit, records)
				}
				return nil
			})
			if err != nil {
				return err
			}
			it.Next()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	records = orderBy(query.OrderBy, query.Limit, records)
	if len(query.Select) > 0 {
		for i, r := range records {
			records[i] = r.Select(query.Select)
		}
	}
	if len(records) > query.Limit {
		return records[:query.Limit], nil
	}
	return records, nil
}

func (d *db) Get(ctx context.Context, collection, id string) (Record, error) {
	if _, ok := d.collections[collection]; !ok {
		return nil, fmt.Errorf("unsupported collection: %s", collection)
	}
	var (
		record Record
	)
	if err := d.kv.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("%s.%s", collection, id)))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			record, err = NewRecordFromJSON(val)
			if err != nil {
				return err
			}
			return nil
		})
	}); err != nil {
		return nil, err
	}
	if d.config.OnRead != nil {
		if err := d.config.OnRead(d, ctx, record); err != nil {
			return nil, err
		}
	}
	return record, nil
}

func (d *db) GetAll(ctx context.Context, collection string, ids []string) ([]Record, error) {
	if _, ok := d.collections[collection]; !ok {
		return nil, fmt.Errorf("unsupported collection: %s", collection)
	}
	var records []Record
	if err := d.kv.View(func(txn *badger.Txn) error {
		for _, id := range ids {
			var (
				record Record
			)
			item, err := txn.Get([]byte(fmt.Sprintf("%s.%s", collection, id)))
			if err != nil {
				return err
			}
			if err := item.Value(func(val []byte) error {
				record, err = NewRecordFromJSON(val)
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
			if d.config.OnRead != nil {
				if err := d.config.OnRead(d, ctx, record); err != nil {
					return err
				}
			}
			records = append(records, record)
		}
		return nil
	}); err != nil {
		return records, err
	}
	return records, nil
}

func orderBy(orderBy OrderBy, limit int, records []Record) []Record {
	if orderBy.Field == "" {
		return records
	}
	if orderBy.Direction == DESC {
		sort.Slice(records, func(i, j int) bool {
			return compareField(orderBy.Field, records[i], records[j])
		})
	} else {
		sort.Slice(records, func(i, j int) bool {
			return !compareField(orderBy.Field, records[i], records[j])
		})
	}
	if limit > 0 && len(records) > limit {
		return records[:limit]
	}
	return records
}

func compareField(field string, i, j Record) bool {
	switch fieldVal := i[field].(type) {
	case nil:
		return false
	case string:
		return fieldVal > cast.ToString(j[field])
	case float64:
		return fieldVal > cast.ToFloat64(j[field])
	case float32:
		return fieldVal > cast.ToFloat32(j[field])
	case int:
		return fieldVal > cast.ToInt(j[field])
	case int64:
		return fieldVal > cast.ToInt64(j[field])
	case int32:
		return fieldVal > cast.ToInt32(j[field])
	case time.Time:
		return fieldVal.UnixMilli() > cast.ToTime(j[field]).UnixMilli()
	case time.Duration:
		return fieldVal > cast.ToDuration(j[field])
	case bool:
		return fieldVal && !cast.ToBool(j[field])
	default:
		return cast.ToString(fieldVal) > cast.ToString(j[field])
	}
}
