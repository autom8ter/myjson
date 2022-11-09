package brutus

import "github.com/palantir/stacktrace"

// Query is a query against the NOSQL database - it does not support full text search
type Query struct {
	// From is the collection to query
	From string `json:"from"`
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

func (q Query) Validate() error {
	if q.From == "" {
		return stacktrace.NewError("empty field: 'from'")
	}
	return nil
}
