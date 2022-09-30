package wolverine

import (
	"context"
	"io"
)

// WhereOp is an operator used to compare a value to a records field value in a where clause
type WhereOp string

// Valid returns true if it is a valid operation
func (o WhereOp) Valid() bool {
	switch o {
	case LteOp, LtOp, GteOp, GtOp, EqOp, NeqOp, ContainsOp, FuzzyOp, PrefixOp, TermOp:
		return true
	default:
		return false
	}
}

// IsSearch returns true if the operator requires full text search
func (o WhereOp) IsSearch() bool {
	switch o {
	case ContainsOp, FuzzyOp, PrefixOp, TermOp:
		return true
	default:
		return false
	}
}

const (
	// PrefixOp is a full text search type for finding records based on prefix matching. full text search operators can only be used
	// against collections that have full text search enabled
	PrefixOp WhereOp = "prefix"
	// ContainsOp full text search type for finding records based on contains matching. full text search operators can only be used
	// against collections that have full text search enabled
	ContainsOp WhereOp = "contains"
	// TermOp full text search type for finding records based on term matching. full text search operators can only be used
	// against collections that have full text search enabled
	TermOp WhereOp = "term"
	// FuzzyOp full text search type for finding records based on a fuzzy search. full text search operators can only be used
	// against collections that have full text search enabled
	FuzzyOp WhereOp = "fuzzy"
	// EqOp matches on equality
	EqOp WhereOp = "eq"
	// NeqOp matches on inequality
	NeqOp WhereOp = "neq"
	// GtOp matches on greater than
	GtOp WhereOp = "gt"
	// GteOp matches on greater than or equal to
	GteOp WhereOp = "gte"
	// LtOp matches on less than
	LtOp WhereOp = "lt"
	// LteOp matches on greater than or equal to
	LteOp WhereOp = "lte"
)

// Where is field-level filter for database queries
type Where struct {
	// Field is a field to compare against records field. For full text search, wrap the field in search(field1,field2,field3) and use a search operator
	Field string `json:"field"`
	// Op is an operator used to compare the field against the value.
	Op WhereOp `json:"op"`
	// Value is a value to compare against a records field value
	Value interface{} `json:"value"`
}

// OrderByDirection indicates whether results should be sorted in ascending or descending order
type OrderByDirection string

const (
	// ASC indicates ascending order
	ASC OrderByDirection = "ASC"
	// DESC indicates descending order
	DESC OrderByDirection = "DESC"
)

// OrderBy orders the result set by a given field in a given direction
type OrderBy struct {
	Field     string           `json:"field"`
	Direction OrderByDirection `json:"direction"`
}

// Query is a query against the NOSQL database
type Query struct {
	Select  []string `json:"select"`
	Where   []Where  `json:"where"`
	StartAt string   `json:"start_at"`
	Limit   int      `json:"limit"`
	OrderBy OrderBy  `json:"order_by"`
}

// Collection is a collection of records of a given type
type Collection struct {
	// Name is the unique name of the collection - it should not contain any special characters
	Name string `json:"name"`
	// Indexes is a list of indexes associated with the collection - indexes should be used to tune database performance
	Indexes []Index `json:"indexes"`
}

// Index is a database index used for quickly finding records with specific field values
type Index struct {
	// Fields is a list of fields that are indexed
	Fields []string `json:"fields"`
	// FullText is a boolean value indicating whether the fields will be full text search-able
	FullText bool `json:"full_text"`
}

// Migration is an atomic database migration
type Migration struct {
	Name     string
	Function func(ctx context.Context, db DB) error
}

// ReadHook is a function that is executed in response to an readable action taken against the database
// Hooks should be use to create side effects based on the context and the data associated with a database action
type ReadHook func(db DB, ctx context.Context, record Record) error

// WriteHook is a function that is executed in response to a writeable action taken against the database
// Hooks should be use to create side effects based on the context and the data associated with a database action
type WriteHook func(db DB, ctx context.Context, before, after Record) error

// CronJob is a function that runs against the database on a given crontab schedule
type CronJob struct {
	// Schedule is the cron schedule
	Schedule string
	// Function is the function executed by the cron
	Function func(ctx context.Context, db DB)
}

// Config configures a database instance
type Config struct {
	// Path is the path to database storage. Use 'inmem' to operate the database in memory only.
	Path string
	// Debug sets the database to debug level
	Debug bool
	// Migrate, if a true, has the database run any migrations that have not already run(idempotent).
	Migrate bool
	// ReIndex reindexes the database
	ReIndex bool
	// Collection configures each collection in the database
	Collections []Collection
	// CronJob are cron jobs that will run
	CronJobs []CronJob
	// Logger is a custom database logger(optional)
	Logger Logger
	// BeforeSet is a hook that is executed before a set operation
	BeforeSet WriteHook
	// BeforeSet is a hook that is executed before a set operation
	BeforeUpdate WriteHook
	// BeforeDelete is a hook that is executed before a delete operation
	BeforeDelete WriteHook
	// OnRead is a hook that is executed as a record is read
	OnRead ReadHook
	// OnStream is a hook that is executed as a record is sent to a stream
	OnStream ReadHook
	// AfterSet is a hook that is executed after a set operation
	AfterSet WriteHook
	// AfterUpdate is a hook that is executed after an update operation
	AfterUpdate WriteHook
	// AfterDelete is a hook that is executed after a delete operation
	AfterDelete WriteHook
	// Migrations are atomic database migrations to run on startup
	Migrations []Migration
}

// DB is an embedded NOSQL database supporting a number of useful features including full text search, indexing, and streaming
type DB interface {
	// System is a database system manager
	System
	// Reader is a database reader
	Reader
	// Writer is a database writer
	Writer
	// Streamer is a datbase streamer
	Streamer
	// Aggregator is a database aggregator
	Aggregator
	// Logger is a structured logger
	Logger
}

// System performs internal/system operations against the database
type System interface {
	// Config returns the config used to initialize the database
	Config() Config
	// ReIndex reindexes the entire database
	ReIndex(ctx context.Context) error
	// Backup performs a full database backup
	Backup(ctx context.Context, w io.Writer) error
	// IncrementalBackup performs an incremental backup based on changes since the last time it ran
	IncrementalBackup(ctx context.Context, w io.Writer) error
	// Restore restores a database backup then reindexes the database
	Restore(ctx context.Context, r io.Reader) error
	// Migrate runs all migrations that have not yet run(idempotent). The order must remain the same over time for migrations to run properly.
	Migrate(ctx context.Context, migrations []Migration) error
	// Close shuts down the database
	Close(ctx context.Context) error
}

// Reader performs read operations against the database
type Reader interface {
	// Query queries the database for a list of records
	Query(ctx context.Context, collection string, query Query) ([]Record, error)
	// Get gets a single record from the database
	Get(ctx context.Context, collection, id string) (Record, error)
	// GetAll gets a list of records from the database by id
	GetAll(ctx context.Context, collection string, ids []string) ([]Record, error)
}

// Writer performs transactional write operations against the database
type Writer interface {
	// Set overwrites a single record in the database. If a record does not exist under the records id, one will be created.
	Set(ctx context.Context, record Record) error
	// BatchSet overwrites many records in the database. If a record does not exist under each record's id, one will be created.
	BatchSet(ctx context.Context, records []Record) error
	// Update updates the fields of a single record in the database. This is not a full replace.
	Update(ctx context.Context, record Record) error
	// BatchUpdate updates the fields of many records in the database. This is not a full replace.
	BatchUpdate(ctx context.Context, records []Record) error
	// QueryUpdate updates records that belong to the given query
	QueryUpdate(ctx context.Context, update Record, collection string, query Query) error
	// Delete deletes a record from the database
	Delete(ctx context.Context, collection, id string) error
	// BatchDelete deletes many records from the database
	BatchDelete(ctx context.Context, collection string, ids []string) error
	// QueryDelete deletes records that belong to the given query
	QueryDelete(ctx context.Context, collection string, query Query) error
	// DropAll drops all of the collections
	DropAll(ctx context.Context, collection []string) error
}

// Streamer streams changes to records in the database
type Streamer interface {
	// Stream streams changes to records to the given function until the context is cancelled or the function returns an error
	Stream(ctx context.Context, collections []string, fn func(ctx context.Context, records []Record) error) error
}

// AggregateFunction is an aggregate function used within MapReduce
type AggregateFunction string

const (
	// AggregateSum calculates the sum
	AggregateSum AggregateFunction = "sum"
	// AggregateMin calculates the min
	AggregateMin AggregateFunction = "min"
	// AggregateMax calculates the max
	AggregateMax AggregateFunction = "max"
	// AggregateAvg calculates the avg
	AggregateAvg AggregateFunction = "avg"
	// AggregateCount calculates the count
	AggregateCount AggregateFunction = "count"
)

// Aggregate is an aggregate function applied to a field
type Aggregate struct {
	Function AggregateFunction `json:"function"`
	Field    string            `json:"field"`
}

// AggregateQuery is an aggregate query against a database collection
type AggregateQuery struct {
	GroupBy   []string    `json:"group_by"`
	Aggregate []Aggregate `json:"aggregate"`
	Where     []Where     `json:"where"`
	OrderBy   OrderBy     `json:"order_by"`
	Limit     int         `json:"limit"`
}

// Aggregator aggregates data
type Aggregator interface {
	// Aggregate
	Aggregate(ctx context.Context, collection string, query AggregateQuery) ([]Record, error)
}

// Logger is a structured logger
type Logger interface {
	Error(ctx context.Context, msg string, err error, tags map[string]interface{})
	Info(ctx context.Context, msg string, tags map[string]interface{})
	Debug(ctx context.Context, msg string, tags map[string]interface{})
	Warn(ctx context.Context, msg string, tags map[string]interface{})
}
