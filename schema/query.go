package schema

import (
	"context"
	"github.com/reactivex/rxgo/v2"
)

// Query is a query against the NOSQL database - it does not support full text search
type Query struct {
	// Select is a list of fields to select from each record in the datbase(optional)
	Select []string `json:"select"`
	// Where is a list of where clauses used to filter records
	Where []Where `json:"where"`
	// Page is page index of the result set
	Page int `json:"page"`
	// Limit is the page size
	Limit int `json:"limit"`
	// Order by is the order to return results in. OrderBy requires an index on the field that the query is sorting on.
	OrderBy OrderBy `json:"order_by"`
}

func (query Query) Observe(ctx context.Context, input chan rxgo.Item, fullScan bool) rxgo.Observable {
	limit := 1000000
	if query.Limit > 0 {
		limit = query.Limit
	}
	if fullScan {
		return query.Observe(ctx, pipeFullScan(ctx, input, query.Where, query.OrderBy), false)
	}
	return rxgo.FromEventSource(input, rxgo.WithContext(ctx)).
		Filter(func(i interface{}) bool {
			pass, err := i.(*Document).Where(query.Where)
			if err != nil {
				return false
			}
			return pass
		}).
		Skip(uint(query.Page * limit)).
		Take(uint(limit)).
		Map(func(ctx context.Context, i interface{}) (interface{}, error) {
			if len(query.Select) > 0 {
				i.(*Document).Select(query.Select)
			}
			return i, nil
		})
}
