package wolverine

import (
	"context"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

func (d *db) Aggregate(ctx context.Context, collection string, query AggregateQuery) ([]Record, error) {
	_, ok := d.collections[collection]
	if !ok {
		return nil, fmt.Errorf("unsupported collection: %s", collection)
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
		it.Seek(seek)
		defer it.Close()
		for it.Valid() {
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
	var groupRecords = map[string]Record{}
	grouped := lo.GroupBy(records, func(t Record) string {
		var values []string
		for _, field := range query.GroupBy {
			values = append(values, cast.ToString(t[field]))
		}
		return strings.Join(values, ".")
	})
	for key, group := range grouped {
		for _, agg := range query.Aggregate {
			for k, v := range getReducer(agg.Function)(agg.Field, group) {
				if groupRecords[key] == nil {
					groupRecords[key] = map[string]interface{}{}
					for _, qgroup := range query.GroupBy {
						groupRecords[key][qgroup] = group[0][qgroup]
					}
				}
				groupRecords[key][k] = v
			}
		}
	}
	var aggRecords []Record
	for _, record := range groupRecords {
		aggRecords = append(aggRecords, record)
	}
	aggRecords = orderBy(query.OrderBy, query.Limit, aggRecords)
	if query.Limit > 0 && len(aggRecords) > query.Limit {
		return aggRecords[:query.Limit], nil
	}
	return aggRecords, nil
}

type reducer func(aggField string, records []Record) Record

func getReducer(function AggregateFunction) reducer {
	switch function {
	case AggregateSum:
		return sumReducer()
	case AggregateMax:
		return maxReducer()
	case AggregateMin:
		return minReducer()
	case AggregateAvg:
		return avgReducer()
	default:
		return countReducer()
	}
}

func sumReducer() reducer {
	return func(aggField string, records []Record) Record {
		return map[string]interface{}{
			fmt.Sprintf("%s.%s", aggField, AggregateSum): lo.SumBy(records, func(t Record) float64 {
				return cast.ToFloat64(t[aggField])
			}),
		}
	}
}

func avgReducer() reducer {
	return func(aggField string, records []Record) Record {
		sum := lo.SumBy(records, func(t Record) float64 {
			return cast.ToFloat64(t[aggField])
		})
		return map[string]interface{}{
			fmt.Sprintf("%s.%s", aggField, AggregateAvg): sum / float64(len(records)),
		}
	}
}

func countReducer() reducer {
	return func(aggField string, records []Record) Record {
		return map[string]interface{}{
			fmt.Sprintf("%s.%s", aggField, AggregateCount): lo.CountBy(records, func(t Record) bool {
				return t[aggField] != nil
			}),
		}
	}
}

func maxReducer() reducer {
	return func(aggField string, records []Record) Record {
		return map[string]interface{}{
			fmt.Sprintf("%s.%s", aggField, AggregateMax): lo.MaxBy(records, func(this Record, that Record) bool {
				return compareField(aggField, this, that)
			}),
		}
	}
}

func minReducer() reducer {
	return func(aggField string, records []Record) Record {
		return map[string]interface{}{
			fmt.Sprintf("%s.%s", aggField, AggregateMin): lo.MinBy(records, func(this Record, that Record) bool {
				return !compareField(aggField, this, that)
			}),
		}
	}
}
