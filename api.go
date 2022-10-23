package wolverine

import (
	"context"
	"github.com/autom8ter/wolverine/schema"
	"io"
)

// DB is an embedded NOSQL database supporting a number of useful features including full text search, indexing, and streaming
type DB interface {
	// System is a database system manager
	System
	// Searcher is the database document search engine
	Searcher
	// Reader is a database document reader
	Reader
	// Writer is a database document writer
	Writer
	// Streamer is a database document streamer
	Streamer
	// Aggregator is a database document aggregator
	Aggregator
}

// System performs internal/system operations against the database
type System interface {
	// ReIndex reindexes the entire database
	ReIndex(ctx context.Context) error
	// ReIndex reindexes a specific collection in the database
	ReIndexCollection(ctx context.Context, collection string) error
	// Backup performs a full database backup
	Backup(ctx context.Context, w io.Writer) error
	// IncrementalBackup performs an incremental backup based on changes since the last time it ran
	IncrementalBackup(ctx context.Context, w io.Writer) error
	// Restore restores a database backup then reindexes the database
	Restore(ctx context.Context, r io.Reader) error
	// Migrate runs all migrations that have not yet run(idempotent). The order must remain the same over time for migrations to run properly.
	Migrate(ctx context.Context, migrations []Migration) error
	// GetCollections gets all of the registered collections in the database
	GetCollections(ctx context.Context) ([]*schema.Collection, error)
	// GetCollection gets a collection by name(if it exists)
	GetCollection(ctx context.Context, collection string) (*schema.Collection, error)
	// SetCollection sets a collection in the database
	SetCollection(ctx context.Context, collection *schema.Collection) error
	// SetCollections sets 1-many collections in the database
	SetCollections(ctx context.Context, collections []*schema.Collection) error
	// Close shuts down the database
	Close(ctx context.Context) error
}

type Searcher interface {
	// Search executes a full text search query against the database
	Search(ctx context.Context, collection string, q schema.SearchQuery) (schema.Page, error)
	// SearchPaginate paginates through each page of the query until the handlePage function returns false or there are no more results
	SearchPaginate(ctx context.Context, collection string, query schema.SearchQuery, handlePage schema.PageHandler) error
}

// Reader performs read operations against the database
type Reader interface {
	// Query queries the database for a list of documents
	Query(ctx context.Context, collection string, query schema.Query) (schema.Page, error)
	// QueryPaginate paginates through each page of the query until the handlePage function returns false or there are no more results
	QueryPaginate(ctx context.Context, collection string, query schema.Query, handlePage schema.PageHandler) error
	// Get gets a single record from the database
	Get(ctx context.Context, collection, id string) (*schema.Document, error)
	// GetAll gets a list of documents from the database by id
	GetAll(ctx context.Context, collection string, ids []string) ([]*schema.Document, error)
}

// Aggregator aggregates documents
type Aggregator interface {
	Aggregate(ctx context.Context, collection string, query schema.AggregateQuery) (schema.Page, error)
	// AggregatePaginate paginates through each page of the query until the handlePage function returns false or there are no more results
	//AggregatePaginate(ctx context.Context, collection string, query schema.AggregateQuery, handlePage schema.AggregatePageHandler) error
}

// Writer performs transactional write operations against the database
type Writer interface {
	// Set overwrites a single record in the database. If a record does not exist under the documents id, one will be created.
	Set(ctx context.Context, collection string, document *schema.Document) error
	// BatchSet overwrites many documents in the database. If a record does not exist under each record's id, one will be created.
	BatchSet(ctx context.Context, collection string, documents []*schema.Document) error
	// Update updates the fields of a single record in the database. This is not a full replace.
	Update(ctx context.Context, collection string, document *schema.Document) error
	// BatchUpdate updates the fields of many documents in the database. This is not a full replace.
	BatchUpdate(ctx context.Context, collection string, documents []*schema.Document) error
	// QueryUpdate updates documents that belong to the given query
	QueryUpdate(ctx context.Context, update *schema.Document, collection string, query schema.Query) error
	// Delete deletes a record from the database
	Delete(ctx context.Context, collection, id string) error
	// BatchDelete deletes many documents from the database
	BatchDelete(ctx context.Context, collection string, ids []string) error
	// QueryDelete deletes documents that belong to the given query
	QueryDelete(ctx context.Context, collection string, query schema.Query) error
}

// ChangeStreamHandler is a function executed on changes to documents which emit events
type ChangeStreamHandler func(ctx context.Context, event *schema.Event) error

// Streamer streams changes to documents in the database
type Streamer interface {
	// ChangeStream streams changes to documents to the given function until the context is cancelled or the function returns an error
	ChangeStream(ctx context.Context, collections []string, fn ChangeStreamHandler) error
}
