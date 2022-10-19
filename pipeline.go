package wolverine

import (
	"context"

	"github.com/reactivex/rxgo/v2"
)

type pipelineOpts struct {
	selectFields []string
	wheres       []Where
	orderBy      OrderBy
	page         int
	limit        int
}

func prepareObservable(ctx context.Context, opts pipelineOpts, input chan rxgo.Item) rxgo.Observable {
	o := rxgo.FromEventSource(input, rxgo.WithContext(ctx))
	return o
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
		results = append(results, result.V.(*Document))
	}
	return results
}
