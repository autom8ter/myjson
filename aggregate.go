package wolverine

import (
	"context"
	"fmt"
	"time"

	"github.com/autom8ter/machine/v4"
	"github.com/dgraph-io/badger/v3"
	"github.com/palantir/stacktrace"
	"github.com/reactivex/rxgo/v2"
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

func (a Aggregate) ComputeField() string {
	return fmt.Sprintf("%s.%s", a.Field, a.Function)
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

func (d *db) Aggregate(ctx context.Context, collection string, query AggregateQuery) (Page, error) {
	if query.Limit == 0 {
		query.Limit = 1000
	}
	now := time.Now()
	_, ok := d.getInmemCollection(collection)
	if !ok {
		return Page{}, stacktrace.NewErrorWithCode(ErrUnsuportedCollection, "unsupported collection: %s must be one of: %v", collection, d.collectionNames())
	}
	qmachine := machine.New()
	pfx, _, err := d.getQueryPrefix(collection, query.Where, query.OrderBy)
	if err != nil {
		return Page{}, stacktrace.Propagate(err, "")
	}
	var (
		input = make(chan rxgo.Item, 1)
	)
	qmachine.Go(ctx, func(ctx context.Context) error {
		if err := d.kv.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = true
			opts.PrefetchSize = 10
			opts.Prefix = pfx
			if query.OrderBy.Direction == DESC {
				opts.Reverse = true
			}
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
	results := queryStream(ctx, pipelineOpts{
		selectFields:  nil,
		groupByFields: query.GroupBy,
		aggregates:    query.Aggregate,
		wheres:        query.Where,
		orderBy:       query.OrderBy,
		page:          query.Page,
		limit:         query.Limit,
	}, input)
	return Page{
		Documents: results,
		NextPage:  query.Page + 1,
		Count:     len(results),
		Stats:     Stats{ExecutionTime: time.Since(now)},
	}, nil
}

type reducer func(aggField, computeField string, accumulate, next *Document) (*Document, error)

func getReducer(function AggregateFunction) reducer {
	switch function {
	case AggregateSum:
		return sumReducer()
	case AggregateMax:
		return maxReducer()
	case AggregateMin:
		return minReducer()
	//case AggregateAvg:
	//	return avgReducer()
	case AggregateCount:
		return countReducer()
	default:
		return nil
	}
}

func sumReducer() reducer {
	return func(aggField, computed string, accumulate, next *Document) (*Document, error) {
		if accumulate == nil {
			next.Set(computed, next.GetFloat(aggField))
			return next, nil
		}
		current := accumulate.GetFloat(computed)
		current += next.GetFloat(aggField)
		accumulate.Set(computed, current)
		return accumulate, nil
	}
}

func countReducer() reducer {
	return func(aggField, computed string, accumulate, next *Document) (*Document, error) {
		if accumulate == nil {
			next.Set(computed, 1)
			return next, nil
		}
		current := accumulate.GetFloat(computed)
		current += 1
		accumulate.Set(computed, current)
		return accumulate, nil
	}
}

func maxReducer() reducer {
	return func(aggField, computed string, accumulate, next *Document) (*Document, error) {
		if accumulate == nil {
			next.Set(computed, next.GetFloat(aggField))
			return next, nil
		}
		current := accumulate.GetFloat(computed)
		if value := next.GetFloat(aggField); value > current {
			current = value
			accumulate.Set(computed, current)
		}
		return accumulate, nil
	}
}

func minReducer() reducer {
	return func(aggField, computed string, accumulate, next *Document) (*Document, error) {
		if accumulate == nil {
			next.Set(computed, next.GetFloat(aggField))
			return next, nil
		}
		current := accumulate.GetFloat(computed)
		if value := next.GetFloat(aggField); value < current {
			current = value
			accumulate.Set(computed, current)
		}
		return accumulate, nil
	}
}
