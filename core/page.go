package core

import (
	"github.com/autom8ter/wolverine/internal/util"
	"time"
)

// Page is a page of documents
type Page struct {
	// Documents are the documents that make up the page
	Documents []*Document `json:"documents"`
	// Next page
	NextPage int `json:"next_page,omitempty"`
	// Document count
	Count int `json:"count"`
	// Stats are statistics collected from a document aggregation query
	Stats PageStats `json:"stats"`
}

func (p Page) String() string {
	return util.JSONString(&p)
}

// PageStats are statistics collected from a query returning a page
type PageStats struct {
	ExecutionTime time.Duration `json:"execution_time,omitempty"`
	IndexMatch    IndexMatch    `json:"index_match,omitempty"`
}

// PageHandler handles a page of documents during pagination. If the handler returns false, pagination will discontinue
type PageHandler func(page Page) bool
