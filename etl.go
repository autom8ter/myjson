package wolverine

import "context"

// ETLFunc takes a set of documents, performs a mutation, and then returns another set of documents
type ETLFunc func(ctx context.Context, docs Documents) (Documents, error)

// ETL in addition to ETLFunc are used to transform a set of documents in one collection to a set of documents in another
type ETL struct {
	// OutputCollection is the collection to dump ETL results into
	OutputCollection string
	// Query is the query used to fetch records for processing
	Query Query
}
