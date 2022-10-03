package wolverine

import (
	"context"

	"github.com/xeipuuv/gojsonschema"
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
	Indexes      []Index `json:"indexes"`
	JSONSchema   string  `json:"json_schema"`
	loadedSchema *gojsonschema.Schema
}

// Validate validates the document against the collections json schema (if it exists)
func (c Collection) Validate(doc *Document) (bool, error) {
	if c.loadedSchema == nil {
		return true, nil
	}
	documentLoader := gojsonschema.NewBytesLoader(doc.Bytes())
	result, err := c.loadedSchema.Validate(documentLoader)
	if err != nil {
		return false, err
	}
	if !result.Valid() {
		return false, nil
	}
	return true, nil
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
	BeforeSet WriteTrigger
	// BeforeSet is a hook that is executed before a set operation
	BeforeUpdate WriteTrigger
	// BeforeDelete is a hook that is executed before a delete operation
	BeforeDelete WriteTrigger
	// OnRead is a hook that is executed as a record is read
	OnRead ReadTrigger
	// OnStream is a hook that is executed as a record is sent to a stream
	OnStream ReadTrigger
	// AfterSet is a hook that is executed after a set operation
	AfterSet WriteTrigger
	// AfterUpdate is a hook that is executed after an update operation
	AfterUpdate WriteTrigger
	// AfterDelete is a hook that is executed after a delete operation
	AfterDelete WriteTrigger
	// Migrations are atomic database migrations to run on startup
	Migrations []Migration
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
