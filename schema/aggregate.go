package schema

import (
	"context"
	"encoding/json"
	"github.com/palantir/stacktrace"
	"github.com/reactivex/rxgo/v2"
	"github.com/spf13/cast"
	"strings"
	"sync"
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

func (a AggregateQuery) String() string {
	bits, _ := json.Marshal(&a)
	return string(bits)
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

func (query AggregateQuery) pipeIndex(ctx context.Context, input chan rxgo.Item) (rxgo.Observable, error) {
	limit := 1000000
	if query.Limit > 0 {
		limit = query.Limit
	}
	return rxgo.FromChannel(input, rxgo.WithContext(ctx), rxgo.WithCPUPool(), rxgo.WithObservationStrategy(rxgo.Eager)).
		Skip(uint(query.Page * limit)).
		Take(uint(limit)), nil
}

func (query AggregateQuery) Observe(ctx context.Context, input chan rxgo.Item, fullScan bool) (rxgo.Observable, error) {
	limit := 1000000
	if query.Limit > 0 {
		limit = query.Limit
	}
	wg := sync.WaitGroup{}
	mu := sync.RWMutex{}
	grouped := make(chan rxgo.Item)
	var grouping []*Document
	if fullScan {
		return query.Observe(ctx, pipeFullScan(ctx, input, query.Where, query.OrderBy), false)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for doc := range rxgo.FromEventSource(input, rxgo.WithContext(ctx), rxgo.WithCPUPool(), rxgo.WithObservationStrategy(rxgo.Eager)).
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
		}).Observe() {
			o := doc.V.(rxgo.GroupedObservable)
			wg.Add(1)
			go func() {
				defer wg.Done()
				reduced := <-o.Reduce(query.reducer()).Observe()
				mu.Lock()
				grouping = append(grouping, reduced.V.(*Document))
				mu.Unlock()
			}()
		}
	}()
	go func() {
		wg.Wait()
		for _, doc := range SortOrder(query.OrderBy, grouping) {
			grouped <- rxgo.Of(doc)
		}
		close(grouped)
	}()
	return rxgo.FromChannel(grouped, rxgo.WithContext(ctx), rxgo.WithCPUPool(), rxgo.WithObservationStrategy(rxgo.Eager)).
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
