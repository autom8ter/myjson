package schema

import (
	"container/list"
	"fmt"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/internal/prefix"
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
	if indexing.PrimaryKey == "" {
		return stacktrace.PropagateWithCode(err, errors.ErrTODO, "missing 'primaryKey' from 'indexing' schema property: %s", c.collection)
	}
	if len(indexing.Search) > 1 {
		return stacktrace.PropagateWithCode(err, errors.ErrTODO, "up to a single search index is supported 'indexing.search': %s", c.collection)
	}
	if !gjson.Get(c.Schema, fmt.Sprintf("properties.%s", indexing.PrimaryKey)).Exists() {
		return stacktrace.PropagateWithCode(err, errors.ErrTODO, "primary key does not exist in properties: %s", c.collection)
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
	index, err := c.GetQueryIndex(whereFields, order.Field)
	if err != nil {
		return QueryIndexMatch{}, stacktrace.Propagate(err, "")
	}
	return index, nil
}

func (c *Collection) PrimaryQueryIndex() *prefix.PrefixIndexRef {
	return c.QueryIndexPrefix(QueryIndex{
		Fields: []string{c.indexing.PrimaryKey},
	})
}

func (c *Collection) GetPrimaryKeyRef(documentID string) ([]byte, error) {
	if documentID == "" {
		return nil, stacktrace.NewErrorWithCode(errors.ErrTODO, "empty document id for property: %s", c.indexing.PrimaryKey)
	}
	return c.QueryIndexPrefix(QueryIndex{
		Fields: []string{c.indexing.PrimaryKey},
	}).GetPrefix(map[string]any{
		c.indexing.PrimaryKey: documentID,
	}, documentID), nil
}

func (c *Collection) QueryIndexPrefix(i QueryIndex) *prefix.PrefixIndexRef {
	return prefix.NewPrefixedIndex(c.collection, i.Fields)
}

func (c *Collection) GetQueryIndex(whereFields []string, orderBy string) (QueryIndexMatch, error) {
	var (
		target  *QueryIndex
		matched int
		ordered bool
	)
	indexing := c.Indexing()
	if !indexing.HasQueryIndex() {
		return QueryIndexMatch{
			Ref:     c.PrimaryQueryIndex(),
			Fields:  []string{c.indexing.PrimaryKey},
			Ordered: orderBy == c.indexing.PrimaryKey || orderBy == "",
		}, nil
	}
	for _, index := range indexing.Query {
		isOrdered := index.Fields[0] == orderBy
		var totalMatched int
		for i, f := range whereFields {
			if index.Fields[i] == f {
				totalMatched++
			}
		}
		if totalMatched > matched || (!ordered && isOrdered) {
			target = index
			ordered = isOrdered
		}
	}
	if target != nil && len(target.Fields) > 0 {
		return QueryIndexMatch{
			Ref:     c.QueryIndexPrefix(*target),
			Fields:  target.Fields,
			Ordered: ordered,
		}, nil
	}
	return QueryIndexMatch{
		Ref:     c.PrimaryQueryIndex(),
		Fields:  []string{c.indexing.PrimaryKey},
		Ordered: orderBy == c.indexing.PrimaryKey || orderBy == "",
	}, nil
}

func (c *Collection) GetDocumentID(d *Document) string {
	return cast.ToString(d.Get(c.indexing.PrimaryKey))
}
