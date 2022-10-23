package schema

import "time"

// Page is a page of documents
type Page struct {
	// Documents are the documents that make up the page
	Documents []*Document `json:"documents"`
	// Next page
	NextPage int `json:"next_page"`
	// Document count
	Count int `json:"count"`
	// Stats are statistics collected from a document aggregation query
	Stats PageStats `json:"stats"`
}

// PageStats are statistics collected from a query returning a page
type PageStats struct {
	ExecutionTime time.Duration   `json:"execution_time"`
	IndexMatch    QueryIndexMatch `json:"index_match"`
}

// PageHandler handles a page of documents during pagination. If the handler returns false, pagination will discontinue
type PageHandler func(page Page) bool
