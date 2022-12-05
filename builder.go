package gokvkit

import (
	"github.com/autom8ter/gokvkit/model"
)

// QueryBuilder is a utility for creating queries via chainable methods
type QueryBuilder struct {
	query *model.Query
}

// NewQueryBuilder creates a new QueryBuilder instance
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{query: &model.Query{}}
}

// Query returns the built query
func (q *QueryBuilder) Query() model.Query {
	return *q.query
}

// Select adds the SelectFiel(s) to the query
func (q *QueryBuilder) Select(fields ...model.Select) *QueryBuilder {
	q.query.Select = append(q.query.Select, fields...)
	return q
}

// From adds the From clause to the query
func (q *QueryBuilder) From(from string) *QueryBuilder {
	q.query.From = from
	return q
}

// Where adds the Where clause(s) to the query
func (q *QueryBuilder) Where(where ...model.Where) *QueryBuilder {
	q.query.Where = append(q.query.Where, where...)
	return q
}

// OrderBy adds the OrderBy clause(s) to the query
func (q *QueryBuilder) OrderBy(ob ...model.OrderBy) *QueryBuilder {
	q.query.OrderBy = append(q.query.OrderBy, ob...)
	return q
}

// Limit adds the Limit clause(s) to the query
func (q *QueryBuilder) Limit(limit int) *QueryBuilder {
	q.query.Limit = &limit
	return q
}

// GroupBy adds the GroupBy clause(s) to the query
func (q *QueryBuilder) GroupBy(groups ...string) *QueryBuilder {
	q.query.GroupBy = append(q.query.GroupBy, groups...)
	return q
}
