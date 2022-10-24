package schema

import (
	"container/list"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/internal/util"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/xeipuuv/gojsonschema"
	"strings"
	"sync"
)

// LoadCollection loads a collection from the provided json schema
func LoadCollection(jsonSchema string) (*Collection, error) {
	c := &Collection{Schema: jsonSchema}
	return c, c.ParseSchema()
}

// Collection is a collection of records of a given type. It is
type Collection struct {
	mu sync.RWMutex
	// Schema is an extended json schema used to validate documents stored in the collection.
	// Custom properties include: collection, indexes, and full_text
	Schema       string `json:"schema"`
	indexing     *Indexing
	collection   string
	loadedSchema *gojsonschema.Schema
}

// ParseSchema parses the collection's json schema
func (c *Collection) ParseSchema() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	c.loadedSchema, err = gojsonschema.NewSchema(gojsonschema.NewStringLoader(c.Schema))
	if err != nil {
		return stacktrace.PropagateWithCode(err, errors.ErrSchemaLoad, "failed to load schema")
	}
	c.collection = cast.ToString(gjson.Get(c.Schema, "collection").Value())
	if c.collection == "" {
		return stacktrace.NewErrorWithCode(errors.ErrEmptySchemaCollection, "empty 'collection' schema property")
	}
	var indexing Indexing
	if gjson.Get(c.Schema, "indexing").Value() == nil {
		return stacktrace.NewErrorWithCode(errors.ErrTODO, "empty 'indexing' schema property: %s", c.collection)
	}

	if err := util.Decode(gjson.Get(c.Schema, "indexing").Value(), &indexing); err != nil {
		return stacktrace.PropagateWithCode(err, errors.ErrTODO, "failed to decode 'indexing' schema property: %s", c.collection)
	}
	for _, i := range indexing.Aggregate {
		i.mu = &sync.RWMutex{}
		i.metrics = map[string]map[Aggregate]*list.List{}
	}

	c.indexing = &indexing

	return nil
}

// Collection returns the name of the collection based on the schema's 'collection' field on the collection's schema
func (c *Collection) Collection() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.collection
}

// QueryIndexes returns the list of the indexes based on the schema's 'indexes' field on the collection's schema
func (c *Collection) Indexing() Indexing {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return *c.indexing
}

// Validate validates the document against the collections json schema (if it exists)
func (c *Collection) Validate(doc *Document) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var err error
	if c.Schema == "" {
		return true, stacktrace.PropagateWithCode(err, errors.ErrTODO, "empty schema")
	}
	documentLoader := gojsonschema.NewBytesLoader(doc.Bytes())
	result, err := c.loadedSchema.Validate(documentLoader)
	if err != nil {
		return false, err
	}
	if !result.Valid() {
		var errs []string
		for _, err := range result.Errors() {
			errs = append(errs, err.String())
		}
		return false, stacktrace.NewErrorWithCode(errors.ErrDocumentValidation, "%s", strings.Join(errs, ","))
	}
	return true, nil
}

func (c *Collection) OptimizeQueryIndex(where []Where, order OrderBy) (QueryIndexMatch, error) {
	var whereFields []string
	var whereValues = map[string]any{}
	for _, w := range where {
		if w.Op != "==" && w.Op != Eq {
			continue
		}
		whereFields = append(whereFields, w.Field)
		whereValues[w.Field] = w.Value
	}
	index, err := c.Indexing().GetQueryIndex(c, whereFields, order.Field)
	if err != nil {
		return QueryIndexMatch{}, stacktrace.Propagate(err, "")
	}
	return index, nil
}
