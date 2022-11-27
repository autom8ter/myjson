package gokvkit

import (
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
)

// SelectField selects a field to return in a queries result set
type SelectField struct {
	// Field is the field to select
	Field string `json:"field"`
	// Function is a aggregate function to use
	Function Function `json:"function"`
	// As will be used to convert the field name into an alias (if it exists)
	As string `json:"as"`
}

// Query is a query against the NOSQL database - it does not support full text search
type Query struct {
	// From is the collection to query
	From string `json:"from"`
	// Select is a list of fields to select from each record in the datbase(optional)
	Select []SelectField `json:"select"`
	// GroupBy are the columns to group data by
	GroupBy []string `json:"group_by"`
	// Where is a list of where clauses used to filter records
	Where []Where `json:"where"`
	// Page is page index of the result set
	Page int `json:"page"`
	// Limit is the page size
	Limit int `json:"limit"`
	// OrderBy is the order to return results in. OrderBy requires an index on the field that the query is sorting on.
	OrderBy []OrderBy `json:"order_by"`
}

func (q Query) isAggregate() bool {
	isAggregate := false
	for _, a := range q.Select {
		if a.Function != "" && a.Function.IsAggregate() {
			isAggregate = true
		}
	}
	return isAggregate
}

// Validate validates the query and returns a validation error if one exists
func (q Query) Validate() error {
	if q.From == "" {
		return stacktrace.NewError("empty field: 'from'")
	}
	if len(q.Select) == 0 {
		return stacktrace.NewError("empty required field: 'select'")
	}
	isAggregate := false
	for _, a := range q.Select {
		if a.Field == "" {
			return stacktrace.NewError("empty required field: 'select.field'")
		}
		if a.Function != "" && a.Function.IsAggregate() {
			isAggregate = true
		}
	}
	if isAggregate {
		for _, a := range q.Select {
			if a.Function == "" || !a.Function.IsAggregate() {
				if !lo.Contains(q.GroupBy, a.Field) {
					return stacktrace.NewError("'%s', is required in the group_by clause when aggregating", a.Field)
				}
			}
		}
		for _, g := range q.GroupBy {
			if !lo.ContainsBy[SelectField](q.Select, func(f SelectField) bool {
				return f.Field == g
			}) {
				return stacktrace.NewError("'%s', is required in the select clause when aggregating", g)
			}
		}
	}
	return nil
}

// QueryBuilder is a utility for creating queries via chainable methods
type QueryBuilder struct {
	query *Query
}

// NewQueryBuilder creates a new QueryBuilder instance
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{query: &Query{}}
}

// Query returns the built query
func (q *QueryBuilder) Query() Query {
	return *q.query
}

// Select adds the SelectFiel(s) to the query
func (q *QueryBuilder) Select(fields ...SelectField) *QueryBuilder {
	q.query.Select = append(q.query.Select, fields...)
	return q
}

// From adds the From clause to the query
func (q *QueryBuilder) From(from string) *QueryBuilder {
	q.query.From = from
	return q
}

// Where adds the Where clause(s) to the query
func (q *QueryBuilder) Where(where ...Where) *QueryBuilder {
	q.query.Where = append(q.query.Where, where...)
	return q
}

// OrderBy adds the OrderBy clause(s) to the query
func (q *QueryBuilder) OrderBy(ob ...OrderBy) *QueryBuilder {
	q.query.OrderBy = append(q.query.OrderBy, ob...)
	return q
}

// Limit adds the Limit clause(s) to the query
func (q *QueryBuilder) Limit(limit int) *QueryBuilder {
	q.query.Limit = limit
	return q
}

// GroupBy adds the GroupBy clause(s) to the query
func (q *QueryBuilder) GroupBy(groups ...string) *QueryBuilder {
	q.query.GroupBy = append(q.query.GroupBy, groups...)
	return q
}
