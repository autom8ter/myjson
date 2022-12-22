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
)

type WhereOp string

const WhereOpContains WhereOp = "contains"
const WhereOpEq WhereOp = "eq"
const WhereOpGt WhereOp = "gt"
const WhereOpGte WhereOp = "gte"
const WhereOpIn WhereOp = "in"
const WhereOpContainsAll WhereOp = "containsAll"
const WhereOpContainsAny WhereOp = "containsAny"
const WhereOpNeq WhereOp = "neq"
const WhereOpLt WhereOp = "lt"
const WhereOpLte WhereOp = "lte"
const WhereOpHasPrefix WhereOp = "hasPrefix"
const WhereOpHasSuffix WhereOp = "hasSuffix"
const WhereOpRegex WhereOp = "regex"

type OrderByDirection string

const OrderByDirectionAsc OrderByDirection = "asc"
const OrderByDirectionDesc OrderByDirection = "desc"

type SelectAggregate string

const SelectAggregateCount SelectAggregate = "count"
const SelectAggregateMax SelectAggregate = "max"
const SelectAggregateMin SelectAggregate = "min"
const SelectAggregateSum SelectAggregate = "sum"

// Query is a query against the NOSQL database
type Query struct {
	Select  []Select  `json:"select" validate:"min=1,required"`
	Where   []Where   `json:"where,omitempty" validate:"dive"`
	GroupBy []string  `json:"groupBy,omitempty"`
	Page    int       `json:"page" validate:"min=0"`
	Limit   int       `json:"limit,omitempty" validate:"min=0"`
	OrderBy []OrderBy `json:"orderBy,omitempty" validate:"dive"`
}

type OrderBy struct {
	Direction OrderByDirection `json:"direction" validate:"oneof='desc' 'asc'"`
	Field     string           `json:"field"`
}

type Select struct {
	Aggregate SelectAggregate `json:"aggregate,omitempty" validate:"oneof='count' 'max' 'min' 'sum'"`
	As        string          `json:"as,omitempty"`
	Field     string          `json:"field"`
}

type Where struct {
	Field string      `json:"field" validate:"required"`
	Op    WhereOp     `json:"op" validate:"oneof='eq' 'neq' 'gt' 'gte' 'lt' 'lte' 'contains' 'containsAny' 'containsAll' 'in'"`
	Value interface{} `json:"value" validate:"required"`
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

func (q Query) IsAggregate() bool {
	for _, a := range q.Select {
		if a.Aggregate != "" {
			return true
		}
	}
	return false
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
	Timestamp  time.Time `json:"timestamp" validate:"required"`
	Metadata   *Metadata `json:"metadata" validate:"required"`
}

// Validate validates the command
func (c *Command) Validate() error {
	return errors.Wrap(util.ValidateStruct(c), errors.Validation, "")
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
	// IsBuilding indicates that the index is currently building
	IsBuilding bool `json:"isBuilding"`
}

// Validate validates the index
func (i Index) Validate() error {
	return errors.Wrap(util.ValidateStruct(&i), errors.Validation, "")
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

//go:embed cdc.yaml
var cdcSchema string

const cdcCollectionName = "cdc"

type JSONOp string

const (
	JSONOpRemove  JSONOp = "remove"
	JSONOpAdd     JSONOp = "add"
	JSONOpReplace JSONOp = "replace"
)

// JSONFieldOp
type JSONFieldOp struct {
	Path        string `json:"path"`
	Op          JSONOp `json:"op"`
	Value       any    `json:"value,omitempty"`
	BeforeValue any    `json:"beforeValue,omitempty"`
}

// CDC is a change data capture object used for tracking changes to documents over time
type CDC struct {
	ID         string        `json:"_id" validate:"required"`
	Collection string        `json:"collection" validate:"required"`
	Action     Action        `json:"action" validate:"required,oneof='create' 'update' 'delete' 'set'"`
	DocumentID string        `json:"documentID" validate:"required"`
	Diff       []JSONFieldOp `json:"diff,omitempty"`
	Timestamp  time.Time     `json:"timestamp" validate:"required"`
	Metadata   *Metadata     `json:"metadata" validate:"required"`
}
