package wolverine

import (
	"context"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

// AggregateFunction is an aggregate function used within MapReduce
type AggregateFunction string

const (
	// AggregateSum calculates the sum
	AggregateSum AggregateFunction = "sum"
	// AggregateMin calculates the min
	AggregateMin AggregateFunction = "min"
	// AggregateMax calculates the max
	AggregateMax AggregateFunction = "max"
	// AggregateAvg calculates the avg
	AggregateAvg AggregateFunction = "avg"
	// AggregateCount calculates the count
	AggregateCount AggregateFunction = "count"
)

// Aggregate is an aggregate function applied to a field
type Aggregate struct {
	Function AggregateFunction `json:"function"`
	Field    string            `json:"field"`
}

// AggregateQuery is an aggregate query against a database collection
type AggregateQuery struct {
	GroupBy   []string    `json:"group_by"`
	Aggregate []Aggregate `json:"aggregate"`
	Where     []Where     `json:"where"`
	OrderBy   OrderBy     `json:"order_by"`
	Page      int         `json:"page"`
	Limit     int         `json:"limit"`
}

func (d *db) Aggregate(ctx context.Context, collection string, query AggregateQuery) (Results, error) {
	_, ok := d.getInmemCollection(collection)
	if !ok {
		return Results{}, stacktrace.Propagate(stacktrace.NewError("unsupported collection: %s must be one of: %v", collection, d.collectionNames()), "")
	}
	prefix, _ := d.getQueryPrefix(collection, query.Where)
	var records []*Document
	if err := d.kv.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 10
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		seek := prefix
		it.Seek(seek)
		defer it.Close()
		skip := 0
		for it.ValidForPrefix(prefix) {
			if skip < query.Page*query.Limit {
				skip++
				it.Next()
				continue
			}
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
					records = append(records, document)
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
		return Results{}, err
	}
	var groupDocuments = map[string]*Document{}
	grouped := lo.GroupBy(records, func(t *Document) string {
		var values []string
		for _, field := range query.GroupBy {
			values = append(values, cast.ToString(t.Get(field)))
		}
		return strings.Join(values, ".")
	})
	for key, group := range grouped {
		for _, agg := range query.Aggregate {
			document, err := getReducer(agg.Function)(agg.Field, group)
			if err != nil {
				return Results{}, err
			}
			for k, v := range document.result.Map() {
				if groupDocuments[key] == nil || groupDocuments[key].Empty() {
					groupDocuments[key] = NewDocument()
					for _, qgroup := range query.GroupBy {
						groupDocuments[key].Set(qgroup, group[0].Get(qgroup))
					}
				}
				groupDocuments[key].Set(k, v.Value())
			}
		}
	}
	var aggDocuments []*Document
	for _, record := range groupDocuments {
		aggDocuments = append(aggDocuments, record)
	}
	aggDocuments = orderBy(query.OrderBy, aggDocuments)
	if query.Limit > 0 && len(aggDocuments) > query.Limit {
		aggDocuments = aggDocuments[:query.Limit]
	}
	return Results{
		Documents: aggDocuments,
		NextPage:  query.Page + 1,
	}, nil
}

type reducer func(aggField string, records []*Document) (*Document, error)

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
	return func(aggField string, records []*Document) (*Document, error) {
		return NewDocumentFromMap(map[string]interface{}{
			fmt.Sprintf("%s.%s", aggField, AggregateSum): lo.SumBy(records, func(t *Document) float64 {
				return cast.ToFloat64(t.Get(aggField))
			}),
		})
	}
}

func avgReducer() reducer {
	return func(aggField string, records []*Document) (*Document, error) {
		sum := lo.SumBy(records, func(t *Document) float64 {
			return cast.ToFloat64(t.Get(aggField))
		})
		return NewDocumentFromMap(map[string]interface{}{
			fmt.Sprintf("%s.%s", aggField, AggregateAvg): sum / float64(len(records)),
		})
	}
}

func countReducer() reducer {
	return func(aggField string, records []*Document) (*Document, error) {
		return NewDocumentFromMap(map[string]interface{}{
			fmt.Sprintf("%s.%s", aggField, AggregateCount): lo.CountBy(records, func(t *Document) bool {
				return t.Get(aggField) != nil
			}),
		})
	}
}

func maxReducer() reducer {
	return func(aggField string, records []*Document) (*Document, error) {
		var max = float64(0)
		for _, record := range records {
			if value := record.GetFloat(aggField); value > max {
				max = value
			}
		}
		return NewDocumentFromMap(map[string]interface{}{
			fmt.Sprintf("%s.%s", aggField, AggregateMax): max,
		})
	}
}

func minReducer() reducer {
	return func(aggField string, records []*Document) (*Document, error) {
		var min = float64(0)
		for _, record := range records {
			if value := record.GetFloat(aggField); value < min {
				min = value
			}
		}
		return NewDocumentFromMap(map[string]interface{}{
			fmt.Sprintf("%s.%s", aggField, AggregateMin): min,
		})
	}
}
