package gokvkit

import (
	"context"
	"encoding/json"

	"github.com/autom8ter/gokvkit/kv"
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
	// MarshalYAML returns the collection schema as yaml bytes
	MarshalYAML() ([]byte, error)
	// UnmarshalYAML refreshes the collection schema with the given json bytes
	UnmarshalYAML(bytes []byte) error
	json.Marshaler
	json.Unmarshaler
}

// Database is a NoSQL database built on top of key value storage
type Database interface {
	// Collections returns a list of collection names that are registered in the collection
	Collections(ctx context.Context) []string
	// ConfigureCollection overwrites a single database collection configuration
	ConfigureCollection(ctx context.Context, collectionSchemaBytes []byte) error
	// GetSchema gets a collection schema by name (if it exists)
	GetSchema(ctx context.Context, collection string) CollectionSchema
	// HasCollection reports whether a collection exists in the database
	HasCollection(ctx context.Context, collection string) bool
	// DropCollection drops the collection and it's indexes from the database
	DropCollection(ctx context.Context, collection string) error
	// Tx executes the given function against a new transaction.
	// if the function returns an error, all changes will be rolled back.
	// otherwise, the changes will be commited to the database
	Tx(ctx context.Context, isUpdate bool, fn TxFunc) error
	// NewTx returns a new transaction. a transaction must call Commit method in order to persist changes
	NewTx(isUpdate bool) Txn
	// ChangeStream streams changes to documents in the given collection.
	ChangeStream(ctx context.Context, collection string) (<-chan CDC, error)
	// Get gets a single document by id
	Get(ctx context.Context, collection, id string) (*Document, error)
	// ForEach scans the optimal index for a collection's documents passing its filters.
	// results will not be ordered unless an index supporting the order by(s) was found by the optimizer
	// Query should be used when order is more important than performance/resource-usage
	ForEach(ctx context.Context, collection string, opts ForEachOpts, fn ForEachFunc) (Optimization, error)
	// Query queries a list of documents
	Query(ctx context.Context, collection string, query Query) (Page, error)
	// Get gets 1-many document by id(s)
	BatchGet(ctx context.Context, collection string, ids []string) (Documents, error)
	// RunScript runs a javascript function by name within the given script.
	// The javascript function must have the following signature: function ${name}(ctx, db, params)
	RunScript(ctx context.Context, name, script string, params map[string]any) (any, error)
	// RawKV returns the database key value provider - it should be used with caution and only when standard database functionality is insufficient.
	RawKV() kv.DB
	// Close closes the database
	Close(ctx context.Context) error
}

// Optimizer selects the best index from a set of indexes based on where clauses
type Optimizer interface {
	// Optimize selects the optimal index to use based on the given where clauses
	Optimize(c CollectionSchema, where []Where) (Optimization, error)
}

// Stream broadcasts and subscribes to entities.
// A Stream can be implemented in memory, or with message-queue services like sqs/pubsub/rabbitmq/nats/splunk/etc
type Stream[T any] interface {
	// Broadcast broadcasts the entity to the channel
	Broadcast(ctx context.Context, channel string, msg T)
	// Pull pulls entities off of the given channel as they are broadcast
	Pull(ctx context.Context, channel string) (<-chan T, error)
}

// Txn is a database transaction interface - it holds the methods used while using a transaction + commit,rollback,and close functionality
type Txn interface {
	// Commit commits the transaction to the database
	Commit(ctx context.Context) error
	// Rollback rollsback all changes made to the datbase
	Rollback(ctx context.Context)
	// Close closes the transaction - it should be deferred after
	Close(ctx context.Context)
	Tx
}

// Tx is a database transaction interface - it holds the methods used while using a transaction
type Tx interface {
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
	ForEach(ctx context.Context, collection string, opts ForEachOpts, fn ForEachFunc) (Optimization, error)
	// CDC returns the change data capture array associated with the transaction.
	// CDC's are persisted to the cdc collection when the transaction is commited.
	CDC() []CDC
	DB() Database
}
