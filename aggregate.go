package wolverine

import (
	"context"
	"encoding/json"
	"github.com/palantir/stacktrace"
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

func ApplyReducers(ctx context.Context, a AggregateQuery, documents []*Document) (*Document, error) {
	var (
		aggregated *Document
	)
	for _, next := range documents {
		if aggregated == nil || !aggregated.Valid() {
			aggregated = next
		}
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
			if err := aggregated.Set(agg.Alias, current); err != nil {
				return nil, stacktrace.Propagate(err, "")
			}
		}
	}
	return aggregated, nil
}
