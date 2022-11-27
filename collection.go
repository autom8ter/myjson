package gokvkit

import (
	"context"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cast"
	"reflect"
)

// Collection is database collection containing 1-many documents of the same type
type Collection struct {
	name        string
	primaryKey  string
	indexes     map[string]Index
	validators  []ValidatorHook
	sideEffects []SideEffectHook
	whereHooks  []WhereHook
	readHooks   []ReadHook
}

// CollectionOpt is an option for configuring a collection
type CollectionOpt func(c *Collection)

// WithIndex adds 1-many indexes to the collection configuration
func WithIndex(indexes ...Index) CollectionOpt {
	return func(c *Collection) {
		for _, i := range indexes {
			c.indexes[i.Name] = i
		}
	}
}

// WithValidatorHook adds  document validator(s) to the collection (see JSONSchema for an example)
func WithValidatorHooks(validators ...ValidatorHook) CollectionOpt {
	return func(c *Collection) {
		c.validators = append(c.validators, validators...)
	}
}

// WithReadHooks adds document read hook(s) to the collection (see JSONSchema for an example)
func WithReadHooks(readHooks ...ReadHook) CollectionOpt {
	return func(c *Collection) {
		c.readHooks = append(c.readHooks, readHooks...)
	}
}

// WithSideEffectBeforeHook adds a side effect to the collections configuration that executes on changes as documents are persisted
func WithSideEffects(sideEffects ...SideEffectHook) CollectionOpt {
	return func(c *Collection) {
		c.sideEffects = append(c.sideEffects, sideEffects...)
	}
}

// WithWhereHook adds a wherre effect to the collections configuration that executes on on where clauses before queries are executed.
func WithWhereHook(whereHook ...WhereHook) CollectionOpt {
	return func(c *Collection) {
		c.whereHooks = append(c.whereHooks, whereHook...)
	}
}

// NewCollection creates a new collection from the given options. If indexing.PrimaryKey is empty, it will default to _id.
func NewCollection(name string, primaryKey string, opts ...CollectionOpt) *Collection {
	c := &Collection{
		name:       name,
		primaryKey: primaryKey,
		indexes:    map[string]Index{},
	}
	for _, o := range opts {
		o(c)
	}
	hasPrimary := false
	for _, i := range c.indexes {
		if i.Collection == "" {
			i.Collection = c.name
		}
		if i.Primary {
			hasPrimary = true
		}
	}
	if !hasPrimary {
		c.indexes["primary_key_idx"] = Index{
			Collection: c.name,
			Name:       "primary_key_idx",
			Fields:     []string{primaryKey},
			Unique:     true,
			Primary:    true,
		}
	}
	return c
}

// Name returns the collections name
func (c *Collection) Name() string {
	return c.name
}

// PrimaryKey returns the collections primary key
func (c *Collection) PrimaryKey() string {
	return c.primaryKey
}

// Indexes returns the collections configured indexes
func (c *Collection) Indexes() []Index {
	var indexes []Index
	for _, i := range c.indexes {
		indexes = append(indexes, i)
	}
	return indexes
}

func (c *Collection) applyWhereHooks(ctx context.Context, db *DB, where []Where) ([]Where, error) {
	var err error
	for _, whereHook := range c.whereHooks {
		where, err = whereHook.Func(ctx, db, where)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
	}
	return where, nil
}

func (c *Collection) applyReadHooks(ctx context.Context, db *DB, doc *Document) (*Document, error) {
	var err error
	for _, readHook := range c.readHooks {
		doc, err = readHook.Func(ctx, db, doc)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
	}
	return doc, nil
}
func (c *Collection) applySideEffectHooks(ctx context.Context, db *DB, change *DocChange) (*DocChange, error) {
	var err error
	for _, sideEffect := range c.sideEffects {
		change, err = sideEffect.Func(ctx, db, change)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
	}
	return change, nil
}

func (c *Collection) applyValidationHooks(ctx context.Context, db *DB, d *DocChange) error {
	if len(c.validators) == 0 {
		return nil
	}
	for _, validator := range c.validators {
		if err := validator.Func(ctx, db, d); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	return nil
}

// SetPrimaryKey sets the documents primary key
func (c *Collection) SetPrimaryKey(d *Document, id string) error {
	return stacktrace.Propagate(d.Set(c.PrimaryKey(), id), "failed to set primary key")
}

// GetPrimaryKey gets the documents primary key(if it exists)
func (c *Collection) GetPrimaryKey(d *Document) string {
	if d == nil {
		return ""
	}
	return cast.ToString(d.Get(c.PrimaryKey()))
}

type indexDiff struct {
	toRemove []Index
	toAdd    []Index
	toUpdate []Index
}

func getIndexDiff(this, that map[string]Index) (indexDiff, error) {
	var (
		toRemove []Index
		toAdd    []Index
		toUpdate []Index
	)
	for _, index := range that {
		if i, ok := this[index.Name]; !ok {
			toAdd = append(toAdd, i)
		}
	}

	for _, current := range this {
		if i, ok := that[current.Name]; !ok {
			toRemove = append(toRemove, i)
		} else {
			if !reflect.DeepEqual(i.Fields, current.Fields) {
				toUpdate = append(toUpdate, i)
			}
		}
	}
	return indexDiff{
		toRemove: toRemove,
		toAdd:    toAdd,
		toUpdate: toUpdate,
	}, nil
}
