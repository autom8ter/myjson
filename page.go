package brutus

import (
	"time"
)

// Page is a page of documents
type Page struct {
	// Documents are the documents that make up the page
	Documents Documents `json:"documents"`
	// Next page
	NextPage int `json:"next_page,omitempty"`
	// Document count
	Count int `json:"count"`
	// Stats are statistics collected from a document aggregation query
	Stats PageStats `json:"stats"`
}

// PageStats are statistics collected from a query returning a page
type PageStats struct {
	// ExecutionTime is the execution time to get the page
	ExecutionTime time.Duration `json:"execution_time,omitempty"`
	// IndexMatch is the index that was used to get the page
	IndexMatch IndexMatch `json:"index_match,omitempty"`
}

// PageHandler handles a page of documents during pagination. If the handler returns false, pagination will discontinue
type PageHandler func(page Page) bool
