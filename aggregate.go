package brutus

import "github.com/palantir/stacktrace"

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
	// Field is the field to aggregate
	Field string `json:"field"`
	// Function is the aggregate function to use
	Function AggFunction `json:"function"`
	// Alias is the output field of the aggregation
	Alias string `json:"alias"`
}

// AggregateQuery is an aggregation query against the NOSQL database
type AggregateQuery struct {
	// From is the collection to aggregate
	From string
	// GroupBy are the columns to group data by
	GroupBy []string `json:"group_by"`
	// Where is a list of where clauses used to filter records
	Where []Where `json:"where"`
	// Aggregates are the aggregates used to reduce the resultset
	Aggregates []Aggregate
	// Page is page index of the result set
	Page int `json:"page"`
	// Limit is the page size
	Limit int `json:"limit"`
	// Order by is the order to return results in. OrderBy requires an index on the field that the query is sorting on.
	OrderBy OrderBy `json:"order_by"`
}

func (q AggregateQuery) Validate() error {
	if q.From == "" {
		return stacktrace.NewError("empty field: 'from'")
	}
	return nil
}
