package model

import "time"

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
	// Optimization
	Optimization Optimization `json:"optimization,omitempty"`
}

// Optimization
type Optimization struct {
	// Index is the index the query optimizer chose
	Index Index `json:"index"`
	// MatchedFields are the fields that matched the index
	MatchedFields []string `json:"matched_fields"`
	// MatchedValues are the values that were matched to the index
	MatchedValues map[string]any `json:"matched_values"`
}
