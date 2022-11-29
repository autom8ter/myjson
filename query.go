package gokvkit

import (
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"time"
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

// OrderByDirection indicates whether results should be sorted in ascending or descending order
type OrderByDirection string

const (
	// ASC indicates ascending order
	ASC OrderByDirection = "ASC"
	// DESC indicates descending order
	DESC OrderByDirection = "DESC"
)

// OrderBy orders the result set by a given field in a given direction
type OrderBy struct {
	// Field is the field to sort on
	Field string `json:"field"`
	// Direction is the sort direction
	Direction OrderByDirection `json:"direction"`
}

// WhereOp is an operator used to compare a value to a records field value in a where clause
type WhereOp string

const (
	// Eq matches on equality
	Eq WhereOp = "eq"
	// Neq matches on inequality
	Neq WhereOp = "neq"
	// Gt matches on greater than
	Gt WhereOp = "gt"
	// Gte matches on greater than or equal to
	Gte WhereOp = "gte"
	// Lt matches on less than
	Lt WhereOp = "lt"
	// Lte matches on greater than or equal to
	Lte WhereOp = "lte"
	// Contains matches on text containing a substring
	Contains WhereOp = "contains"
	// In matches on an element being contained in a list
	In WhereOp = "in"
)

// Where is field-level filter for database queries
type Where struct {
	// Field is a field to compare against records field. For full text search, wrap the field in search(field1,field2,field3) and use a search operator
	Field string `json:"field"`
	// Op is an operator used to compare the field against the value.
	Op WhereOp `json:"op"`
	// Value is a value to compare against a records field value
	Value any `json:"value"`
}

// Page is a page of documents
type Page struct {
	// Documents are the documents that make up the page
	Documents Documents `json:"documents"`
	// Next page
	NextPage int `json:"next_page"`
	// Document count
	Count int `json:"count"`
	// Stats are statistics collected from a document aggregation query
	Stats PageStats `json:"stats"`
}

// PageStats are statistics collected from a query returning a page
type PageStats struct {
	// ExecutionTime is the execution time to get the page
	ExecutionTime time.Duration `json:"execution_time"`
	// OptimizerResult is the index that was used to get the page
	OptimizerResult OptimizerResult `json:"index_match"`
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
	for _, a := range q.Select {
		if a.Function != "" && a.Function.IsAggregate() {
			return true
		}
	}
	return false
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
