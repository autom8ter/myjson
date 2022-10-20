package wolverine

import (
	"context"
	"fmt"
	"github.com/autom8ter/machine/v4"
	"github.com/dgraph-io/badger/v3"
	"github.com/palantir/stacktrace"
	"github.com/reactivex/rxgo/v2"
	"github.com/spf13/cast"
	"strings"
	"sync"
	"time"
)

// AggFunction is a function used to aggregate against a document field
type AggFunction string

const (
	SUM   AggFunction = "sum"
	MAX   AggFunction = "max"
	MIN   AggFunction = "min"
	COUNT AggFunction = "count"
)

// Aggregate represents an aggregation function applied to a document field
type Aggregate struct {
	Field    string      `json:"field"`
	Function AggFunction `json:"function"`
	Alias    string      `json:"alias"`
}

// AggregateQuery is an aggregation query against the NOSQL database
type AggregateQuery struct {
	GroupBy []string `json:"group_by"`
	// Where is a list of where clauses used to filter records
	Where []Where `json:"where"`
	//
	Aggregates []Aggregate
	// Page is page index of the result set
	Page int `json:"page"`
	// Limit is the page size
	Limit int `json:"limit"`
	// Order by is the order to return results in. OrderBy requires an index on the field that the query is sorting on.
	OrderBy OrderBy `json:"order_by"`
}

func (a AggregateQuery) reducer() func(ctx context.Context, i interface{}, i2 interface{}) (interface{}, error) {
	return func(ctx context.Context, i interface{}, i2 interface{}) (interface{}, error) {
		if i == nil {
			i = i2
		}
		aggregated := i.(*Document)
		next := i2.(*Document)
		for _, agg := range a.Aggregates {
			if agg.Alias == "" {
				return nil, stacktrace.NewError("empty aggregate alias: %s/%s", agg.Field, agg.Function)
			}
			current := aggregated.GetFloat(agg.Alias)
			switch agg.Function {
			case COUNT:
				current++
			case MAX:
				if value := next.GetFloat(agg.Field); value > current {
					current = value
				}
			case MIN:
				if value := next.GetFloat(agg.Field); value < current {
					current = value
				}
			case SUM:
				current += next.GetFloat(agg.Field)
			default:
				return nil, stacktrace.NewError("unsupported aggregate function: %s/%s", agg.Field, agg.Function)
			}
			aggregated.Set(agg.Alias, current)
		}
		return aggregated, nil
	}
}

func (query AggregateQuery) pipe(ctx context.Context, input chan rxgo.Item, fullScan bool) (rxgo.Observable, error) {
	limit := 1000000
	if query.Limit > 0 {
		limit = query.Limit
	}
	wg := sync.WaitGroup{}
	grouped := make(chan rxgo.Item)
	if fullScan {
		return query.pipe(ctx, pipeFullScan(ctx, input, query.Where, query.OrderBy), false)
	}
	rxgo.FromChannel(input, rxgo.WithContext(ctx), rxgo.WithCPUPool()).
		Filter(func(i interface{}) bool {
			pass, err := i.(*Document).Where(query.Where)
			if err != nil {
				return false
			}
			return pass
		}).GroupByDynamic(func(item rxgo.Item) string {
		var values []string
		for _, g := range query.GroupBy {
			values = append(values, cast.ToString(item.V.(*Document).Get(g)))
		}
		return strings.Join(values, ".")
	}).ForEach(func(i interface{}) {
		o := i.(rxgo.GroupedObservable)
		wg.Add(1)
		go func() {
			defer wg.Done()
			reduced := <-o.Reduce(query.reducer()).Observe()
			grouped <- reduced
		}()
	}, func(err error) {
		fmt.Println(err)
	}, func() {
		wg.Wait()
		close(grouped)
	})
	return rxgo.FromChannel(grouped, rxgo.WithContext(ctx), rxgo.WithCPUPool()).
		Skip(uint(query.Page * limit)).
		Take(uint(limit)).
		Map(func(ctx context.Context, i interface{}) (interface{}, error) {
			toSelect := query.GroupBy
			for _, a := range query.Aggregates {
				toSelect = append(toSelect, a.Alias)
			}
			i.(*Document).Select(toSelect)
			return i, nil
		}), nil
}

func (d *db) Aggregate(ctx context.Context, collection string, query AggregateQuery) (Page, error) {
	now := time.Now()
	qmachine := machine.New()
	if collection != systemCollection {
		if _, ok := d.getInmemCollection(collection); !ok {
			return Page{}, stacktrace.NewErrorWithCode(ErrUnsuportedCollection, "unsupported collection: %s must be one of: %v", collection, d.collectionNames())
		}
	}
	pfx, indexedFields, ordered, err := d.getQueryPrefix(collection, query.Where, query.OrderBy)
	if err != nil {
		return Page{}, stacktrace.Propagate(err, "")
	}
	fullScan := query.OrderBy.Field != "" && !ordered
	var (
		input = make(chan rxgo.Item)
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
				if ctx.Err() != nil {
					return nil
				}
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
	var results []*Document
	pipe, err := query.pipe(ctx, input, fullScan)
	if err != nil {
		return Page{}, stacktrace.Propagate(err, "")
	}
	for result := range pipe.Observe() {
		doc, ok := result.V.(*Document)
		if !ok {
			return Page{}, stacktrace.NewError("expected type: %T got: %#v", &Document{}, result.V)
		}
		results = append(results, doc)
	}
	//if err := qmachine.Wait(); err != nil {
	//	return Page{}, stacktrace.Propagate(err, "")
	//}
	return Page{
		Documents: results,
		NextPage:  query.Page + 1,
		Count:     len(results),
		Stats: Stats{
			ExecutionTime: time.Since(now),
			IndexedFields: indexedFields,
			OrderedIndex:  ordered,
		},
	}, nil
}
