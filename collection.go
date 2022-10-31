package wolverine

import (
	"context"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cast"
	"reflect"
)

// Collection is database collection containing 1-many documents of the same type
type Collection struct {
	name       string
	primaryKey string
	indexes    map[string]Index
	validators []DocumentValidator
}

// DocumentValidator is used to validate all new and updated documents being persisted to a collection
type DocumentValidator func(ctx context.Context, d *Document) error

// CollectionOpt is an option for configuring a collection
type CollectionOpt func(c *Collection)

// WithIndex adds an index to the collection
func WithIndex(indexes ...Index) CollectionOpt {
	return func(c *Collection) {
		for _, i := range indexes {
			c.indexes[i.Name] = i
		}
	}
}

// WithValidator adds a document validator to the collection
func WithValidator(validator DocumentValidator) CollectionOpt {
	return func(c *Collection) {
		c.validators = append(c.validators, validator)
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

// Validate validates the input document - it is used by the CoreAPI to validate changes to documents
func (c *Collection) Validate(ctx context.Context, d *Document) error {
	if len(c.validators) == 0 {
		return nil
	}
	doc, err := NewDocumentFromBytes(d.Bytes())
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	for _, validator := range c.validators {
		if err := validator(ctx, doc); err != nil {
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
