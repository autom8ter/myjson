package wolverine

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/palantir/stacktrace"
	"github.com/reactivex/rxgo/v2"
	"github.com/spf13/cast"
)

type pipelineOpts struct {
	selectFields  []string
	groupByFields []string
	aggregates    []Aggregate
	wheres        []Where
	orderBy       OrderBy
	page          int
	limit         int
}

func prepareObservable(ctx context.Context, opts pipelineOpts, input chan rxgo.Item) rxgo.Observable {
	o := rxgo.FromEventSource(input, rxgo.WithContext(ctx))
	if len(opts.groupByFields) == 0 {
		return o
	}
	var grouped = make(chan rxgo.GroupedObservable)
	go func() {
		o.GroupByDynamic(func(item rxgo.Item) string {
			var values []string
			for _, field := range opts.groupByFields {
				values = append(values, cast.ToString(item.V.(*Document).Get(field)))
			}
			fmt.Println(values)
			return strings.Join(values, ".")
		}).ForEach(func(group interface{}) {
			fmt.Println("sending group", group.(rxgo.GroupedObservable).Key)
			grouped <- group.(rxgo.GroupedObservable)
			fmt.Println("sent group", group.(rxgo.GroupedObservable).Key)
		}, func(err error) {
			fmt.Println(err)
		}, func() {
			fmt.Println("done grouping")
			close(grouped)
		})
	}()
	groupOut := make(chan rxgo.Item)
	go func() {
		wg := sync.WaitGroup{}
		for group := range grouped {
			fmt.Println("key", group.Key)
			wg.Add(1)
			go func(group rxgo.GroupedObservable) {
				defer wg.Done()
				item, _ := group.Reduce(func(ctx context.Context, i interface{}, i2 interface{}) (interface{}, error) {
					var computed *Document
					for _, agg := range opts.aggregates {
						var (
							document *Document
							err      error
						)
						if i == nil {
							document, err = getReducer(agg.Function)(agg.Field, agg.ComputeField(), nil, i2.(*Document))
							if err != nil {
								return Page{}, stacktrace.Propagate(err, "")
							}
						} else {
							document, err = getReducer(agg.Function)(agg.Field, agg.ComputeField(), i.(*Document), i2.(*Document))
							if err != nil {
								return Page{}, stacktrace.Propagate(err, "")
							}
						}

						if computed == nil {
							computed = document
						} else {
							computed.Merge(document)
						}
					}
					fmt.Println("computed")
					return computed, nil
				}).Get()
				fmt.Println("here", item.V)
				groupOut <- item
			}(group)
		}
		wg.Wait()
		close(groupOut)
	}()
	return rxgo.FromEventSource(groupOut, rxgo.WithContext(ctx))
}

func queryStream(ctx context.Context, opts pipelineOpts, input chan rxgo.Item) []*Document {
	o := prepareObservable(ctx, opts, input)

	if len(opts.wheres) > 0 {
		o = o.Filter(func(i interface{}) bool {
			pass, err := i.(*Document).Where(opts.wheres)
			if err != nil {
				return false
			}
			return pass
		})
	}
	o = o.Skip(uint(opts.page * opts.limit))
	if opts.limit > 0 {
		o = o.Take(uint(opts.limit))
	}
	if len(opts.selectFields) > 0 {
		o = o.Map(func(ctx context.Context, i interface{}) (interface{}, error) {
			if len(opts.selectFields) > 0 {
				i.(*Document).Select(opts.selectFields)
			}
			return i, nil
		})
	}
	var results []*Document
	for result := range o.Observe() {
		fmt.Println("result observed", result.V)
		results = append(results, result.V.(*Document))
	}
	return results
}
