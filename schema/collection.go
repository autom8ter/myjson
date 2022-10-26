package schema

import (
	"encoding/json"
	"fmt"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/internal/prefix"
	"github.com/autom8ter/wolverine/internal/util"
	"github.com/palantir/stacktrace"
	"github.com/segmentio/ksuid"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/xeipuuv/gojsonschema"
	"gopkg.in/yaml.v3"
	"io/fs"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

// LoadCollection loads a collection from the provided json schema
func LoadCollection(jsonSchema string) (*Collection, error) {
	c := &Collection{Schema: jsonSchema}
	return c, c.ParseSchema()
}

// LoadCollectionsFromDir loads all yaml/json collections in the specified directory
func LoadCollectionsFromDir(collectionsDir string) ([]*Collection, error) {
	var collections []*Collection
	dir := os.DirFS(collectionsDir)
	if err := fs.WalkDir(dir, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		if d.IsDir() {
			return nil
		}

		switch {
		case strings.HasSuffix(path, ".yaml"):
			files, err := dir.Open(path)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			f, err := ioutil.ReadAll(files)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			data := map[string]interface{}{}
			if err := yaml.Unmarshal(f, &data); err != nil {
				return stacktrace.Propagate(err, "")
			}
			jsonData, err := json.Marshal(data)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			var collection = &Collection{Schema: string(jsonData)}

			if err := collection.ParseSchema(); err != nil {
				return stacktrace.Propagate(err, "")
			}
			collections = append(collections, collection)
		case strings.HasSuffix(path, ".json"):
			files, err := dir.Open(path)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			f, err := ioutil.ReadAll(files)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			var collection = &Collection{Schema: string(f)}
			if err := collection.ParseSchema(); err != nil {
				return stacktrace.Propagate(err, "")
			}
			collections = append(collections, collection)
		}
		return nil
	}); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return collections, nil
}

// Collection is a collection of records of a given type. It is
type Collection struct {
	mu sync.RWMutex
	// Schema is an extended json schema used to validate documents stored in the collection.
	// Custom properties include: collection, indexes, and full_text
	Schema        string `json:"schema"`
	indexing      *Indexing
	relationships *Relationships
	collection    string
	properties    gjson.Result
	loadedSchema  *gojsonschema.Schema
}

// ParseSchema parses the collection's json schema
func (c *Collection) ParseSchema() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	if c.relationships == nil || c.relationships.ForeignKeys == nil {
		c.relationships = &Relationships{ForeignKeys: map[string]ForeignKey{}}
	}
	c.loadedSchema, err = gojsonschema.NewSchema(gojsonschema.NewStringLoader(c.Schema))
	if err != nil {
		return stacktrace.PropagateWithCode(err, errors.ErrSchemaLoad, "failed to load schema")
	}
	c.collection = cast.ToString(gjson.Get(c.Schema, "@collection").Value())
	if c.collection == "" {
		return stacktrace.NewErrorWithCode(errors.ErrEmptySchemaCollection, "empty '@collection' schema property")
	}
	var (
		indexing Indexing
	)
	if gjson.Get(c.Schema, "@indexing").Value() == nil {
		return stacktrace.NewErrorWithCode(errors.ErrTODO, "empty '@indexing' schema property: %s", c.collection)
	}
	if err := util.Decode(gjson.Get(c.Schema, "@indexing").Value(), &indexing); err != nil {
		return stacktrace.PropagateWithCode(err, errors.ErrTODO, "failed to decode 'indexing' schema property: %s", c.collection)
	}
	if indexing.PrimaryKey == "" {
		return stacktrace.PropagateWithCode(err, errors.ErrTODO, "missing 'primaryKey' from '@indexing' schema property: %s", c.collection)
	}
	if len(indexing.Search) > 1 {
		return stacktrace.PropagateWithCode(err, errors.ErrTODO, "up to a single search index is supported '@indexing.search': %s", c.collection)
	}
	if !gjson.Get(c.Schema, fmt.Sprintf("properties.%s", indexing.PrimaryKey)).Exists() {
		return stacktrace.PropagateWithCode(err, errors.ErrTODO, "primary key field does not exist in properties: %s", c.collection)
	}
	c.properties = gjson.Get(c.Schema, "properties")
	for field, value := range c.properties.Map() {
		if fkey := value.Get("@foreignKey").Value(); fkey != "" {
			var foreignKey ForeignKey
			if err := util.Decode(fkey, &foreignKey); err != nil {
				return stacktrace.PropagateWithCode(err, errors.ErrTODO, "failed to decode '@foreignKey' schema property: %s", c.collection)
			}
			c.relationships.ForeignKeys[field] = foreignKey
		}
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

// Indexing returns the list of the indexes based on the schema's 'indexing' field on the collection's schema
func (c *Collection) Indexing() Indexing {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return *c.indexing
}

func (c *Collection) HasRelationships() bool {
	return len(c.Relationships().ForeignKeys) > 0
}

// Relationships
func (c *Collection) Relationships() Relationships {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return *c.relationships
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

// OptimizeQueryIndex selects the optimal index to use given the where/orderby clause
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
	index, err := c.getQueryIndex(whereFields, order.Field)
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

// GetPrimaryKeyRef gets a reference to the documents primary key
func (c *Collection) GetPrimaryKeyRef(documentID string) ([]byte, error) {
	if documentID == "" {
		return nil, stacktrace.NewErrorWithCode(errors.ErrTODO, "empty document id for property: %s", c.indexing.PrimaryKey)
	}
	return c.PrimaryQueryIndex().GetPrefix(map[string]any{
		c.indexing.PrimaryKey: documentID,
	}, documentID), nil
}

// SetID sets the documents primary key
func (c *Collection) SetID(d *Document) error {
	return stacktrace.Propagate(d.Set(c.indexing.PrimaryKey, ksuid.New().String()), "")
}

func (c *Collection) QueryIndexPrefix(i QueryIndex) *prefix.PrefixIndexRef {
	return prefix.NewPrefixedIndex(c.collection, i.Fields)
}

// GetQueryIndex gets the
func (c *Collection) getQueryIndex(whereFields []string, orderBy string) (QueryIndexMatch, error) {
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
