package myjson

import (
	"context"
	"time"

	// import embed package
	_ "embed"

	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/util"
	"github.com/samber/lo"
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

// String returns the query as a json string
func (q Query) String() string {
	return util.JSONString(q)
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

// GetMetadataValue gets a metadata value from the context if it exists
func GetMetadataValue(ctx context.Context, key string) any {
	m, ok := ctx.Value(metadataKey).(*Document)
	if ok {
		val := m.Get(key)
		if val == nil && key == "namespace" {
			return "default"
		}
		return val
	}
	if key == "namespace" {
		return "default"
	}
	return nil
}

// SetMetadataValues sets metadata key value pairs in the context
func SetMetadataValues(ctx context.Context, data map[string]any) context.Context {
	m, ok := ctx.Value(metadataKey).(*Document)
	if !ok {
		m = NewDocument()
		_ = m.Set("namespace", "default")
	}
	_ = m.SetAll(data)
	return context.WithValue(ctx, metadataKey, m)
}

// ExtractMetadata extracts metadata from the context and returns it
func ExtractMetadata(ctx context.Context) *Document {
	m, ok := ctx.Value(metadataKey).(*Document)
	if ok {
		return m
	}
	m = NewDocument()

	_ = m.Set("namespace", "default")
	return m
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
	// Explain is the optimizer's output for the query that returned a page
	Explain *Explain `json:"explain,omitempty"`
}

// Explain is the optimizer's output for a query
type Explain struct {
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
	// CreateAction creates a document
	CreateAction Action = "create"
	// SetAction sets a document's values in place
	SetAction Action = "set"
	// UpdateAction updates a set of fields on a document
	UpdateAction Action = "update"
	// DeleteAction deletes a document
	DeleteAction Action = "delete"
	// QueryAction queries documents
	QueryAction Action = "query"
	// ConfigureAction configures a collection of documents
	ConfigureAction Action = "configure"
	// ChangeStreamAction creates a change stream
	ChangeStreamAction Action = "changeStream"
)

// persistCommand is a command executed against the database that causes a change in state
type persistCommand struct {
	Collection string    `json:"collection" validate:"required"`
	Action     Action    `json:"action" validate:"required,oneof='create' 'update' 'delete' 'set'"`
	Document   *Document `json:"document" validate:"required"`
	Timestamp  int64     `json:"timestamp" validate:"required"`
	Metadata   *Document `json:"metadata" validate:"required"`
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
	ForeignKey *ForeignKey `json:"foreign_key,omitempty"`
}

type EventType string

const (
	OnSet    EventType = "on_set"
	OnDelete EventType = "on_delete"
	OnUpdate EventType = "on_update"
	OnCreate EventType = "on_create"
	OnQuery  EventType = "on_query"
)

// Trigger is a javasript function executed after a database event occurs
type Trigger struct {
	// Name is the unique name of the trigger
	Name string `json:"name" validate:"required"`
	// Order is used to sort triggers into an array where the lower order #s are executed before larger order #s
	Order int `json:"order"`
	// Events is an array of events that the trigger executes on
	Events []EventType `json:"events" validate:"min=1,required"`
	// Script is the javascript script to execute
	Script string `json:"script" validate:"required"`
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
	// DocumentID is the ID of the document that was changed
	DocumentID string `json:"documentID" validate:"required"`
	// Diff is the difference between the previous and new version of the document
	Diff []JSONFieldOp `json:"diff,omitempty"`
	// Timestamp is the nanosecond timestamp the cdc was created at
	Timestamp int64 `json:"timestamp" validate:"required"`
	// Metadata is the context metadata when the change was made
	Metadata *Document `json:"metadata" validate:"required"`
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

// TxCmd is a serializable transaction command
type TxCmd struct {
	// Create is a create command
	Create *CreateCmd `json:"create,omitempty"`
	// Get is a get command
	Get *GetCmd `json:"get,omitempty"`
	// Set is a set command
	Set *SetCmd `json:"set,omitempty"`
	// Update is an update command
	Update *UpdateCmd `json:"update,omitempty"`
	// Delete is a delete command
	Delete *DeleteCmd `json:"delete,omitempty"`
	// Query is a query command
	Query *QueryCmd `json:"query,omitempty"`
	// Commit is a commit command - it ends the transaction
	Commit *CommitCmd `json:"commit,omitempty"`
	// Rollback is a rollback command - it ends the transaction
	Rollback *RollbackCmd `json:"rollback,omitempty"`
}

// TxResponse is a serializable transaction response
type TxResponse struct {
	// Create is a create response - it returns the created document
	Create *Document `json:"create,omitempty"`
	// Get is a get response - it returns the document from the get request (if it exists)
	Get *Document `json:"get,omitempty"`
	// Set is a set response - it returns the document after the set was applied
	Set *Document `json:"set,omitempty"`
	// Update is an update response - it contains the document after the update was applied
	Update *Document `json:"update,omitempty"`
	// Delete is an empty delete response
	Delete *struct{} `json:"delete,omitempty"`
	// Query is a query response - it contains the documents returned from the query
	Query *Page `json:"page,omitempty"`
	// Commit is an empty commit response
	Commit *struct{} `json:"commit,omitempty"`
	// Rollback is an empty rollback response
	Rollback *struct{} `json:"rollback,omitempty"`
	// Error is an error response if an error was encountered
	Error *errors.Error `json:"error,omitempty"`
}

// DeleteCmd is a serializable delete command
type DeleteCmd struct {
	// Collection is the collection the document belongs to
	Collection string `json:"collection" validate:"required"`
	// ID is the unique id of the document
	ID string `json:"id" validate:"required"`
}

// GetCmd is a serializable get command
type GetCmd struct {
	// Collection is the collection the document belongs to
	Collection string `json:"collection" validate:"required"`
	// ID is the unique id of the document
	ID string `json:"id" validate:"required"`
}

// SetCmd is a serializable set command
type SetCmd struct {
	// Collection is the collection the document belongs to
	Collection string `json:"collection" validate:"required"`
	// Document is the document to set
	Document *Document `json:"document" validate:"required"`
}

// CreateCmd is a serializable create command
type CreateCmd struct {
	// Collection is the collection the document belongs to
	Collection string `json:"collection" validate:"required"`
	// Document is the document to set
	Document *Document `json:"document" validate:"required"`
}

// UpdateCmd is a serializable update command
type UpdateCmd struct {
	// Collection is the collection the document belongs to
	Collection string `json:"collection" validate:"required"`
	// ID is the unique id of the document
	ID string `json:"id" validate:"required"`
	// Update is the set of fields to set
	Update map[string]any `json:"update,omitempty"`
}

// QueryCmd is a serializable query command
type QueryCmd struct {
	// Collection is the collection the document belongs to
	Collection string `json:"collection" validate:"required"`
	// Query is the query to execute
	Query Query `json:"query,omitempty"`
}

// RollbackCmd is a serializable rollback command
type RollbackCmd struct{}

// CommitCmd is a serializable commit command
type CommitCmd struct{}

// Authz is a serializable authz object which represents the x-authorization section of a collection schema
type Authz struct {
	Rules []AuthzRule `json:"rules" validate:"min=1,required"`
}

// AuthzEffect is an effect of an authz rule
type AuthzEffect string

const (
	Allow AuthzEffect = "allow"
	Deny  AuthzEffect = "deny"
)

// AuthzRule
type AuthzRule struct {
	Effect AuthzEffect `json:"effect" validate:"required"`
	Action []Action    `json:"action" validate:"min=1,required"`
	Match  string      `json:"match" validate:"required"`
}
