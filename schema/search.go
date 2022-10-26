package schema

// SearchOp is an operator used within a where clause in a full text search query
type SearchOp string

const (
	// Prefix is a full text search type for finding records based on prefix matching. full text search operators can only be used
	// against collections that have full text search enabled
	Prefix SearchOp = "prefix"
	// Wildcard full text search type for finding records based on wildcard matching. full text search operators can only be used
	// against collections that have full text search enabled
	Wildcard SearchOp = "wildcard"
	// Fuzzy full text search type for finding records based on a fuzzy search. full text search operators can only be used
	// against collections that have full text search enabled
	Fuzzy SearchOp = "fuzzy"
	// Regex full text search type for finding records based on a regex matching. full text search operators can only be used
	// against collections that have full text search enabled
	Regex SearchOp = "regex"
	// Basic is a basic matcher that checks for an exact match.
	Basic SearchOp = "basic"
)

// Where is field-level filter for database queries
type SearchWhere struct {
	// Field is a field to compare against records field. For full text search, wrap the field in search(field1,field2,field3) and use a search operator
	Field string `json:"field"`
	// Op is an operator used to compare the field against the value.
	Op SearchOp `json:"op"`
	// Value is a value to compare against a records field value
	Value interface{} `json:"value"`
	// Boost boosts the score of records matching the where clause
	Boost float64 `json:"boost"`
}

// SearchQuery is a full text search query against the database
type SearchQuery struct {
	// Select is a list of Fields to select from each record in the datbase(optional)
	Select []string `json:"select"`
	// Where is a list of where clauses used to filter records based on full text search (required)
	Where []SearchWhere `json:"where"`
	// Filter filters out search results - filters are applied after the initial set of results are returned
	Filter []Where `json:"filter"`
	// Page is the page number of the search request. Total offset is limit*page : (limit*page)+limit
	Page int `json:"page"`
	// Limit limits the number of records returned by the query
	Limit int `json:"limit"`
}
