package myjson

import (
	"context"
	"encoding/json"
	"time"

	"github.com/autom8ter/myjson/kv"
)

// CollectionSchema is a database collection configuration
type CollectionSchema interface {
	// Collection is the collection name
	Collection() string
	// ValidateDocument validates the input document against the collection's JSON schema
	ValidateDocument(ctx context.Context, doc *Document) error
	// Indexing returns a copy the schemas indexing
	Indexing() map[string]Index
	// PrimaryIndex returns the collection's primary index
	PrimaryIndex() Index
	// PrimaryKey returns the collection's primary key
	PrimaryKey() string
	// GetPrimaryKey gets the document's primary key
	GetPrimaryKey(doc *Document) string
	// SetPrimaryKey sets the document's primary key
	SetPrimaryKey(doc *Document, id string) error
	// RequireQueryIndex returns whether the collection requires that queries are appropriately indexed
	RequireQueryIndex() bool
	// Properties returns a map of the schema's properties
	Properties() map[string]SchemaProperty
	// PropertyPaths returns a flattened map of the schema's properties - nested properties will be keyed in dot notation
	PropertyPaths() map[string]SchemaProperty
	// Triggers returns a map of triggers keyed by name that are assigned to the collection
	Triggers() []Trigger
	// IsReadOnly returns whether the collection is read only
	IsReadOnly() bool
	// Authz returns the collection's authz if it exists
	Authz() Authz
	// MarshalYAML returns the collection schema as yaml bytes
	MarshalYAML() ([]byte, error)
	// UnmarshalYAML refreshes the collection schema with the given json bytes
	UnmarshalYAML(bytes []byte) error
	json.Marshaler
	json.Unmarshaler
}

// ChangeStreamHandler handles changes to documents which are emitted as a change data capture stream
type ChangeStreamHandler func(ctx context.Context, cdc CDC) (bool, error)

// CollectionConfiguration is a map of collection names to collection schemas - it declarative represents the database collection configuration
type CollectionConfiguration map[string]string

// Database is a NoSQL database built on top of key value storage
type Database interface {
	// Collections returns a list of collection names that are registered in the database
	Collections(ctx context.Context) []string
	// GetSchema gets a collection schema by name (if it exists)
	GetSchema(ctx context.Context, collection string) CollectionSchema
	// HasCollection reports whether a collection exists in the database
	HasCollection(ctx context.Context, collection string) bool
	// Configure sets the database collection configurations. It will create/update/delete the necessary collections and indexes to
	// match the given configuration. Each element in the config should be a YAML string representing a CollectionSchema.
	// Collections are updated sequentially and will block until all collections are updated.
	Configure(ctx context.Context, config []string) error
	// Tx executes the given function against a new transaction.
	// if the function returns an error, all changes will be rolled back.
	// otherwise, the changes will be commited to the database
	Tx(ctx context.Context, opts kv.TxOpts, fn TxFunc) error
	// NewTx returns a new transaction. a transaction must call Commit method in order to persist changes
	NewTx(opts kv.TxOpts) (Txn, error)
	// ChangeStream streams changes to documents in the given collection. CDC Persistence must be enabled to use this method.
	ChangeStream(ctx context.Context, collection string, filter []Where, fn ChangeStreamHandler) error
	// Get gets a single document by id
	Get(ctx context.Context, collection, id string) (*Document, error)
	// ForEach scans the optimal index for a collection's documents passing its filters.
	// results will not be ordered unless an index supporting the order by(s) was found by the optimizer
	// Query should be used when order is more important than performance/resource-usage
	ForEach(ctx context.Context, collection string, opts ForEachOpts, fn ForEachFunc) (Explain, error)
	// Query queries a list of documents
	Query(ctx context.Context, collection string, query Query) (Page, error)
	// RunScript executes a javascript function within the script
	// The following global variables will be injected: 'db' - a database instance, 'ctx' - the context passed to RunScript, and 'params' - the params passed to RunScript
	RunScript(ctx context.Context, function string, script string, params map[string]any) (any, error)
	// RawKV returns the database key value provider - it should be used with caution and only when standard database functionality is insufficient.
	RawKV() kv.DB
	// Serve serves the database over the given transport
	Serve(ctx context.Context, t Transport) error
	// NewDoc creates a new document builder instance
	NewDoc() *DocBuilder
	// Close closes the database
	Close(ctx context.Context) error
}

// Optimizer selects the best index from a set of indexes based on where clauses
type Optimizer interface {
	// Optimize selects the optimal index to use based on the given where clauses
	Optimize(c CollectionSchema, where []Where) (Explain, error)
}

// Txn is a database transaction interface - it holds the methods used while using a transaction + commit,rollback,and close functionality
type Txn interface {
	// Commit commits the transaction to the database
	Commit(ctx context.Context) error
	// Rollback rollsback all changes made to the datbase
	Rollback(ctx context.Context) error
	// Close closes the transaction - it should be deferred after
	Close(ctx context.Context)
	Tx
}

// Tx is a database transaction interface - it holds the primary methods used while using a transaction
type Tx interface {
	// Cmd is a generic command that can be used to execute any command against the database
	Cmd(ctx context.Context, cmd TxCmd) TxResponse
	// Query executes a query against the database
	Query(ctx context.Context, collection string, query Query) (Page, error)
	// Get returns a document by id
	Get(ctx context.Context, collection string, id string) (*Document, error)
	// Create creates a new document - if the documents primary key is unset, it will be set as a sortable unique id
	Create(ctx context.Context, collection string, document *Document) (string, error)
	// Update updates a value in the database
	Update(ctx context.Context, collection, id string, document map[string]any) error
	// Set sets the specified key/value in the database
	Set(ctx context.Context, collection string, document *Document) error
	// Delete deletes the specified key from the database
	Delete(ctx context.Context, collection string, id string) error
	// ForEach scans the optimal index for a collection's documents passing its filters.
	// results will not be ordered unless an index supporting the order by(s) was found by the optimizer
	// Query should be used when order is more important than performance/resource-usage
	ForEach(ctx context.Context, collection string, opts ForEachOpts, fn ForEachFunc) (Explain, error)
	// TimeTravel sets the document to the value it was at the given timestamp.
	// If the document did not exist at the given timestamp, it will return the first version of the document
	TimeTravel(ctx context.Context, collection string, documentID string, timestamp time.Time) (*Document, error)
	// Revert reverts the document to the value it was at the given timestamp.
	// If the document did not exist at the given timestamp, it will persist the first version of the document
	Revert(ctx context.Context, collection string, documentID string, timestamp time.Time) error
	// DB returns the transactions underlying database
	DB() Database
}

// Transport serves the database over a network (optional for integration with different transport mechanisms)
type Transport interface {
	Serve(ctx context.Context, db Database) error
}
