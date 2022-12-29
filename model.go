package gokvkit

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	_ "embed"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/util"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

// WhereOp is an operation belonging to a where clause
type WhereOp string

// WhereOpContains checks if a string field contains subtext
const WhereOpContains WhereOp = "contains"

// WhereOpEq is an equality check.
const WhereOpEq WhereOp = "eq"

// WhereOpGt is a check whether a value is greater than another
const WhereOpGt WhereOp = "gt"

// WhereOpGte is a check whether a value is greater than or equal to another
const WhereOpGte WhereOp = "gte"

// WhereOpIn is a check whether a value is one of a list of values
const WhereOpIn WhereOp = "in"

// WhereOpContainsAll is a check whether an array contains all of the given array values
const WhereOpContainsAll WhereOp = "containsAll"

// WhereOpContainsAny is a check whether an array contains any of the given array values
const WhereOpContainsAny WhereOp = "containsAny"

// WhereOpNeq is a non-equality check
const WhereOpNeq WhereOp = "neq"

// WhereOpLt is a check whether a value is less than another
const WhereOpLt WhereOp = "lt"

// WhereOpLte is a check whether a values is less than or equal to another
const WhereOpLte WhereOp = "lte"

// WhereOpHasPrefix is a check whether a string value has a prefix
const WhereOpHasPrefix WhereOp = "hasPrefix"

// WhereOpHasSuffix is a check whether a string value has a suffix
const WhereOpHasSuffix WhereOp = "hasSuffix"

// WhereOpRegex is a check whtether a string value matches a regex expression
const WhereOpRegex WhereOp = "regex"

// OrderByDirection is the direction of an order by clause
type OrderByDirection string

// OrderByDirectionAsc is ascending order
const OrderByDirectionAsc OrderByDirection = "asc"

// OrderByDirectionDesc is descending order
const OrderByDirectionDesc OrderByDirection = "desc"

// AggregateFunction is an agggregate function applied to a list of documents
type AggregateFunction string

// AggregateFunctionCount gets the count of a set of documents
const AggregateFunctionCount AggregateFunction = "count"

// AggregateFunctionMax gets the max value in a set of documents
const AggregateFunctionMax AggregateFunction = "max"

// AggregateFunctionMin gets the min value in a set of documents
const AggregateFunctionMin AggregateFunction = "min"

// AggregateFunctionSum gets the sum of values in a set of documents
const AggregateFunctionSum AggregateFunction = "sum"

// Query is a query against the NOSQL database
type Query struct {
	// Select selects fields - at least 1 select is required.
	// 1 select with Field: * gets all fields
	Select []Select `json:"select" validate:"min=1,required"`
	// Join joins the results to another collection
	Join []Join `json:"join,omitempty" validate:"dive"`
	// Where filters results. The optimizer will select the appropriate index based on where clauses
	Where []Where `json:"where,omitempty" validate:"dive"`
	// GroupBy groups results by a given list of fields
	GroupBy []string `json:"groupBy,omitempty"`
	// Page is the page of results - it is used with Limit for pagination
	Page int `json:"page" validate:"min=0"`
	// Limit is used to limit the number of results returned
	Limit int `json:"limit,omitempty" validate:"min=0"`
	// OrderBy orders results
	OrderBy []OrderBy `json:"orderBy,omitempty" validate:"dive"`
	// Having applies a final filter after any aggregations have occured
	Having []Where `json:"having,omitempty" validate:"dive"`
}

// OrderBy indicates how to order results returned from a query
type OrderBy struct {
	Direction OrderByDirection `json:"direction" validate:"oneof='desc' 'asc'"`
	Field     string           `json:"field"`
}

// Select is a field to select
type Select struct {
	Aggregate AggregateFunction `json:"aggregate,omitempty" validate:"oneof='count' 'max' 'min' 'sum'"`
	As        string            `json:"as,omitempty"`
	Field     string            `json:"field"`
}

// Where is a filter against documents returned from a query
type Where struct {
	Field string      `json:"field" validate:"required"`
	Op    WhereOp     `json:"op" validate:"oneof='eq' 'neq' 'gt' 'gte' 'lt' 'lte' 'contains' 'containsAny' 'containsAll' 'in'"`
	Value interface{} `json:"value" validate:"required"`
}

// Join is a join against another collection
type Join struct {
	Collection string  `json:"collection" validate:"required"`
	On         []Where `json:"on" validate:"required,min=1"`
	As         string  `json:"as,omitempty"`
}

// Validate validates the query and returns a validation error if one exists
func (q Query) Validate(ctx context.Context) error {
	if err := util.ValidateStruct(&q); err != nil {
		return errors.Wrap(err, errors.Validation, "")
	}
	if len(q.Select) == 0 {
		return errors.New(errors.Validation, "query validation error: at least one select is required")
	}
	isAggregate := false
	for _, a := range q.Select {
		if a.Field == "" {
			return errors.New(errors.Validation, "empty required field: 'select.field'")
		}
		if a.Aggregate != "" {
			isAggregate = true
		}
	}
	if isAggregate {
		for _, a := range q.Select {
			if a.Aggregate == "" {
				if !lo.Contains(q.GroupBy, a.Field) {
					return errors.New(errors.Validation, "'%s', is required in the group_by clause when aggregating", a.Field)
				}
			}
		}
		for _, g := range q.GroupBy {
			if !lo.ContainsBy[Select](q.Select, func(f Select) bool {
				return f.Field == g
			}) {
				return errors.New(errors.Validation, "'%s', is required in the select clause when aggregating", g)
			}
		}
	}
	return nil
}

type ctxKey int

const (
	metadataKey ctxKey = 0
)

// Metadata holds key value pairs associated with a go Context
type Metadata struct {
	tags sync.Map
}

// NewMetadata creates a Metadata with the given tags
func NewMetadata(tags map[string]any) *Metadata {
	m := &Metadata{}
	if tags != nil {
		m.SetAll(tags)
	}
	return m
}

// String return a json string of the context
func (m *Metadata) String() string {
	bits, _ := m.MarshalJSON()
	return string(bits)
}

// MarshalJSON returns the metadata values as json bytes
func (m *Metadata) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.Map())
}

// UnmarshalJSON decodes the metadata from json bytes
func (m *Metadata) UnmarshalJSON(bytes []byte) error {
	data := map[string]any{}
	if err := json.Unmarshal(bytes, &data); err != nil {
		return err
	}
	m.SetAll(data)
	return nil
}

// SetAll sets the key value fields on the metadata
func (m *Metadata) SetAll(data map[string]any) {
	for k, v := range data {
		m.tags.Store(k, v)
	}
}

// Set sets a key value pair on the metadata
func (m *Metadata) Set(key string, value any) {
	m.SetAll(map[string]any{
		key: value,
	})
}

// SetNamespace sets the namespace on the context metadata
// Data belonging to different namespaces are stored/indexed separately, though collections exist across namespaces
func (m *Metadata) SetNamespace(value string) {
	m.SetAll(map[string]any{
		"namespace": value,
	})
}

// GetNamespace gets the namespace from the metadata, or 'default' if it does not exist
// Data belonging to different namespaces are stored/indexed separately, though collections exist across namespaces
func (m *Metadata) GetNamespace() string {
	val, ok := m.tags.Load("namespace")
	if !ok {
		return "default"
	}
	return cast.ToString(val)
}

// Del deletes a key from the metadata
func (m *Metadata) Del(key string) {
	m.tags.Delete(key)
}

// Get gets a key from the metadata if it exists
func (m *Metadata) Get(key string) (any, bool) {
	return m.tags.Load(key)
}

// Exists returns true if the key exists in the metadata
func (m *Metadata) Exists(key string) bool {
	_, ok := m.tags.Load(key)
	return ok
}

// Map returns the metadata keyvalues as a map
func (m *Metadata) Map() map[string]any {
	data := map[string]any{}
	m.tags.Range(func(key, value any) bool {
		data[key.(string)] = value
		return true
	})
	return data
}

// ToContext adds the metadata to the input go context
func (m *Metadata) ToContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, metadataKey, m)
}

// GetMetadata gets metadata from the context if it exists
func GetMetadata(ctx context.Context) (*Metadata, bool) {
	m, ok := ctx.Value(metadataKey).(*Metadata)
	if ok {
		return m, true
	}
	return &Metadata{}, false
}

// Page is a page of documents
type Page struct {
	// Documents are the documents that make up the page
	Documents Documents `json:"documents"`
	// Next page
	NextPage int `json:"next_page"`
	// Document count
	Count int `json:"count"`
	// Stats are statistics collected from a document aggregation query
	Stats PageStats `json:"stats,omitempty"`
}

// PageStats are statistics collected from a query returning a page
type PageStats struct {
	// ExecutionTime is the execution time to get the page
	ExecutionTime time.Duration `json:"execution_time,omitempty"`
	// Optimization is the optimizer's output for the query that returned a page
	Optimization Optimization `json:"optimization,omitempty"`
}

// Optimization
type Optimization struct {
	// Collection
	Collection string `json:"collection"`
	// Index is the index the query optimizer chose
	Index Index `json:"index"`
	// MatchedFields are the fields that matched the index
	MatchedFields []string `json:"matched_fields"`
	// MatchedValues are the values that were matched to the index
	MatchedValues map[string]any `json:"matched_values,omitempty"`
	// SeekFields indicates that the given fields will be seeked
	SeekFields []string `json:"seek,omitempty"`
	// SeekValues are the values to seek
	SeekValues map[string]any `json:"seek_values,omitempty"`
	// Reverse indicates that the index should be scanned in reverse
	Reverse bool `json:"reverse,omitempty"`
}

// Action is an action that causes a mutation to the database
type Action string

const (
	// Create creates a document
	Create = "create"
	// Set sets a document's values in place
	Set = "set"
	// Update updates a set of fields on a document
	Update = "update"
	// Delete deletes a document
	Delete = "delete"
)

// Command is a command executed against the database that causes a change in state
type Command struct {
	Collection string    `json:"collection" validate:"required"`
	Action     Action    `json:"action" validate:"required,oneof='create' 'update' 'delete' 'set'"`
	Document   *Document `json:"document" validate:"required"`
	Timestamp  int64     `json:"timestamp" validate:"required"`
	Metadata   *Metadata `json:"metadata" validate:"required"`
}

// Index is a database index used to optimize queries against a collection
type Index struct {
	// Name is the indexes unique name in the collection
	Name string `json:"name" validate:"required,min=1"`
	// Fields to index - order matters
	Fields []string `json:"fields" validate:"required,min=1"`
	// Unique indicates that it's a unique index which will enforce uniqueness
	Unique bool `json:"unique"`
	// Unique indicates that it's a primary index
	Primary bool `json:"primary"`
	// ForeignKey indecates that it's an index for a foreign key
	ForeignKey *ForeignKey `json:"foreignKey,omitempty"`
}

// OnPersist is a hook function triggered whenever a command is persisted
type OnPersist struct {
	// Name is the name of the hook
	Name string `validate:"required"`
	// Before indicates whether the hook should execute before or after the command is persisted
	Before bool
	// Func is the function to execute
	Func func(ctx context.Context, tx Tx, command *Command) error `validate:"required"`
}

// OnInit is a hook function triggered whenever the database starts
type OnInit struct {
	// Name is the name of the hook
	Name string `validate:"required"`
	// Func is the function to execute
	Func func(ctx context.Context, db *defaultDB) error `validate:"required"`
}

// OnCommit is a hook function triggered before a transaction is commited
type OnCommit struct {
	// Name is the name of the hook
	Name string `validate:"required"`
	// Before indicates whether the hook should execute before or after the transaction is commited
	Before bool
	// Func is the function to execute
	Func func(ctx context.Context, tx Tx) error `validate:"required"`
}

// OnRollback is a hook function triggered whenever a transaction is rolled back
type OnRollback struct {
	// Name is the name of the hook
	Name string `validate:"required"`
	// Before indicates whether the hook should execute before or after the transaction is rolled back
	Before bool
	// Func is the function to execute
	Func func(ctx context.Context, tx Tx) `validate:"required"`
}

// JSONOp is an json field operation type
type JSONOp string

const (
	// JSONOpRemove removes a field from a json document
	JSONOpRemove JSONOp = "remove"
	// JSONOpAdd adds a json field to a json document
	JSONOpAdd JSONOp = "add"
	// JSONOpReplace replaces an existing field in a json document
	JSONOpReplace JSONOp = "replace"
)

// JSONFieldOp is an operation against a JSON field
type JSONFieldOp struct {
	// Path is the path to the field within the document
	Path string `json:"path"`
	// Op is the operation applied
	Op JSONOp `json:"op"`
	// Value is the value applied with the operation
	Value any `json:"value,omitempty"`
	// BeforeValue is the value before the operation was applied
	BeforeValue any `json:"beforeValue,omitempty"`
}

//go:embed cdc.yaml
var cdcSchema string

const cdcCollectionName = "cdc"

// CDC is a change data capture object used for tracking changes to documents over time
type CDC struct {
	// ID is the unique id of the cdc
	ID string `json:"_id" validate:"required"`
	// Collection is the collection the change was applied to
	Collection string `json:"collection" validate:"required"`
	// Action is the action applied to the document
	Action Action `json:"action" validate:"required,oneof='create' 'update' 'delete' 'set'"`
	// DocumentID is the document ID that was changed
	DocumentID string `json:"documentID" validate:"required"`
	// Diff is the difference between the previous and new version of the document
	Diff []JSONFieldOp `json:"diff,omitempty"`
	// Timestamp is the nanosecond timestamp the cdc was created at
	Timestamp int64 `json:"timestamp" validate:"required"`
	// Metadata is the context metadata when the change was made
	Metadata *Metadata `json:"metadata" validate:"required"`
}

// ForeignKey is a reference/relationship to another collection by primary key
type ForeignKey struct {
	// Collection is the foreign collection
	Collection string `json:"collection"`
	// Cascade indicates that the document should be deleted when the foreign key is deleted
	Cascade bool `json:"cascade"`
}

// SchemaProperty is a property belonging to a JSON Schema
type SchemaProperty struct {
	// Primary indicates the property is the primary key
	Primary bool `json:"x-primary,omitempty"`
	// Name is the name of the property
	Name string `json:"name" validate:"required"`
	// Description is the description of the property
	Description string `json:"description,omitempty"`
	// Type is the type of the property
	Type string `json:"type" validate:"required"`
	// Path is a dot notation path to the property
	Path string `json:"path" validate:"required"`
	// Unique indicates the field value is unique
	Unique bool `json:"x-unique,omitempty"`
	// ForeignKey is a relationship to another collection
	ForeignKey *ForeignKey `json:"x-foreign,omitempty"`
	// Index is a secondary index mapped by index name
	Index map[string]PropertyIndex `json:"x-index,omitempty"`
	// Properties are object properties
	Properties map[string]SchemaProperty `json:"properties,omitempty"`
}

// PropertyIndex is an index attached to a json schema property
type PropertyIndex struct {
	AdditionalFields []string `json:"additional_fields,omitempty"`
}

// ForEachOpts are options when executing db.ForEach against a collection
type ForEachOpts struct {
	Where []Where `json:"where,omitempty"`
	Join  []Join  `json:"join,omitempty"`
}

// JSFunction is javascript function compiled to a go function
type JSFunction func(ctx context.Context, db Database, params map[string]any) (any, error)

//go:embed migration.yaml
var migrationSchema string

const migrationCollectionName = "migration"

// Migration is an atomic database migration
type Migration struct {
	// ID is the unique id of the cdc
	ID string `json:"_id" validate:"required"`
	// Timestamp is the nanosecond timestamp the cdc was created at
	Timestamp int64 `json:"timestamp"`
	// Script is the script's content
	Script string `json:"script" validate:"required"`
	// Dirty indicates whether the migration failed or not
	Dirty bool `json:"dirty"`
	// An error message if one was encountered
	Error string `json:"error"`
}
