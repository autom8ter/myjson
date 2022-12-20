package gokvkit

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
func (q *QueryBuilder) Select(fields ...Select) *QueryBuilder {
	q.query.Select = append(q.query.Select, fields...)
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

// Page adds the Page clause(s) to the query which controls pagination results
func (q *QueryBuilder) Page(page int) *QueryBuilder {
	q.query.Page = page
	return q
}

// GroupBy adds the GroupBy clause(s) to the query
func (q *QueryBuilder) GroupBy(groups ...string) *QueryBuilder {
	q.query.GroupBy = append(q.query.GroupBy, groups...)
	return q
}
