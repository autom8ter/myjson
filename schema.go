package gokvkit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/util"

	"github.com/qri-io/jsonschema"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type ForeignKey struct {
	Collection string `json:"collection"`
	Field      string `json:"field"`
	Cascade    bool   `json:"cascade"`
}

type SchemaProperty struct {
	Primary     bool                      `json:"primary,omitempty"`
	Name        string                    `json:"name" validate:"required"`
	Description string                    `json:"description,omitempty"`
	Type        string                    `json:"type" validate:"required"`
	Path        string                    `json:"path"`
	Properties  map[string]SchemaProperty `json:"properties,omitempty"`
	Unique      bool                      `json:"unique,omitempty"`
	ForeignKey  ForeignKey                `json:"foreignKey,omitempty"`
}

type collectionSchema struct {
	schema        *jsonschema.Schema
	raw           gjson.Result
	collection    string
	primaryIndex  Index
	indexing      map[string]Index
	properties    map[string]SchemaProperty
	propertyPaths map[string]SchemaProperty
	mu            sync.RWMutex
}

type schemaPath string

const (
	collectionPath   schemaPath = "x-collection"
	indexingPath     schemaPath = "x-indexing"
	requireIndexPath schemaPath = "x-require-index"
	propertiesPath   schemaPath = "properties"
)

func newCollectionSchema(yamlContent []byte) (CollectionSchema, error) {
	if len(yamlContent) == 0 {
		return nil, errors.New(errors.Validation, "empty schema content")
	}
	var (
		schema = &jsonschema.Schema{}
	)
	jsonContent, err := util.YAMLToJSON(yamlContent)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(jsonContent, schema); err != nil {
		return nil, errors.Wrap(err, 0, "failed to decode json schema")
	}
	r := gjson.ParseBytes(jsonContent)
	s := &collectionSchema{
		schema:        schema,
		raw:           r,
		collection:    r.Get(string(collectionPath)).String(),
		indexing:      map[string]Index{},
		properties:    map[string]SchemaProperty{},
		propertyPaths: map[string]SchemaProperty{},
	}
	for _, index := range s.raw.Get(string(indexingPath)).Map() {
		var i Index
		err = util.Decode(index.Value(), &i)
		if err != nil {
			return nil, err
		}
		if err := i.Validate(); err != nil {
			return nil, err
		}
		if i.Primary {
			s.primaryIndex = i
		}
		s.indexing[i.Name] = i
	}
	if err != nil {
		return nil, err
	}
	if len(s.primaryIndex.Fields) == 0 {
		return nil, errors.New(errors.Validation, "primary index is required")
	}
	loadProperties(s.raw.Get(string(propertiesPath)), s.raw.Get(string(propertiesPath)), s.properties, s.propertyPaths)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func loadProperties(og gjson.Result, result gjson.Result, props map[string]SchemaProperty, paths map[string]SchemaProperty) {
	if !og.IsObject() {
		loadProperties(result, result, props, paths)
		return
	}
	result.ForEach(func(key, value gjson.Result) bool {
		schema := SchemaProperty{
			Primary:     value.Get("x-primary").Bool(),
			Name:        key.String(),
			Description: value.Get("description").String(),
			Type:        value.Get("type").String(),
			Path:        value.Path(result.Raw),
			Unique:      value.Get("x-unique").Bool(),
			Properties:  map[string]SchemaProperty{},
		}
		if properties := value.Get("properties"); properties.Exists() && schema.Type == "object" {
			loadProperties(og, properties, schema.Properties, paths)
		}
		if fkey := value.Get("x-foreign"); fkey.Exists() && schema.Type != "object" {
			util.Decode(fkey.Map(), &schema.ForeignKey)
		}
		props[key.String()] = schema
		paths[schema.Path] = schema
		return true
	})
}

func (c *collectionSchema) refreshSchema(jsonContent []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(jsonContent) == 0 {
		return errors.New(errors.Validation, "empty schema content")
	}
	var (
		schema = &jsonschema.Schema{}
	)
	if err := json.Unmarshal(jsonContent, schema); err != nil {
		return errors.Wrap(err, errors.Validation, "failed to decode json schema")
	}
	c.raw = gjson.ParseBytes(jsonContent)
	c.schema = schema
	c.indexing = map[string]Index{}
	c.collection = c.raw.Get(string(collectionPath)).String()
	for _, index := range c.raw.Get(string(indexingPath)).Map() {
		var i Index
		err := util.Decode(index.Value(), &i)
		if err != nil {
			return errors.Wrap(err, errors.Validation, "failed to decode index")
		}
		if err := i.Validate(); err != nil {
			return err
		}
		if i.Primary {
			c.primaryIndex = i
		}
		c.indexing[i.Name] = i
	}
	if len(c.primaryIndex.Fields) == 0 {
		return errors.New(errors.Validation, "primary index is required")
	}
	for _, index := range c.raw.Get(string(indexingPath)).Map() {
		var i Index
		err := util.Decode(index.Value(), &i)
		if err != nil {
			return errors.Wrap(err, errors.Validation, "failed to decode index")
		}
		if err := i.Validate(); err != nil {
			return err
		}
		if i.Primary {
			c.primaryIndex = i
		}
		c.indexing[i.Name] = i
	}
	return nil
}

func (c *collectionSchema) MarshalYAML() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return util.JSONToYAML([]byte(c.raw.Raw))
}

func (c *collectionSchema) UnmarshalYAML(bytes []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	bits, err := util.YAMLToJSON(bytes)
	if err != nil {
		return err
	}
	return c.refreshSchema(bits)
}

func (c *collectionSchema) MarshalJSON() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return []byte(c.raw.Raw), nil
}

func (c *collectionSchema) UnmarshalJSON(bytes []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.refreshSchema(bytes)
}

func (c *collectionSchema) Collection() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.collection
}

func (c *collectionSchema) Indexing() map[string]Index {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var i = map[string]Index{}
	for k, v := range c.indexing {
		i[k] = v
	}
	return i
}

func (c *collectionSchema) SetIndex(index Index) error {
	if err := index.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if index.Name == c.primaryIndex.Name {
		return errors.New(errors.Forbidden, "forbidden from modifying the primary index: %s", index.Name)
	}
	raw, err := sjson.Set(c.raw.Raw, fmt.Sprintf("%s.%s", string(indexingPath), index.Name), index)
	if err != nil {
		return errors.Wrap(err, 0, "failed to set schema index: %s", index.Name)
	}
	c.raw = gjson.Parse(raw)
	c.indexing[index.Name] = index
	return nil
}

func (c *collectionSchema) DelIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if name == c.primaryIndex.Name {
		return errors.New(errors.Forbidden, "forbidden from deleting the primary index: %s", name)
	}
	raw, err := sjson.Delete(c.raw.Raw, fmt.Sprintf("%s.%s", string(indexingPath), name))
	if err != nil {
		return errors.Wrap(err, 0, "failed to delete schema index: %s", name)
	}
	c.raw = gjson.Parse(raw)
	delete(c.indexing, name)
	return nil
}

func (c *collectionSchema) ValidateDocument(ctx context.Context, doc *Document) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	kerrs, err := c.schema.ValidateBytes(ctx, doc.Bytes())
	if err != nil {
		return errors.Wrap(err, errors.Validation, "%v: failed to validate document", c.collection)
	}

	if kerrs != nil && len(kerrs) > 0 {
		return errors.New(errors.Validation, "%v: invalid document- %s", c.collection, util.JSONString(kerrs))
	}
	return nil
}

func (c *collectionSchema) PrimaryKey() string {
	fields := c.PrimaryIndex().Fields
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func (c *collectionSchema) GetPrimaryKey(doc *Document) string {
	if doc == nil {
		return ""
	}
	return doc.GetString(c.PrimaryKey())
}

func (c *collectionSchema) SetPrimaryKey(doc *Document, id string) error {
	pkey := c.PrimaryKey()
	return errors.Wrap(doc.Set(pkey, id), 0, "failed to set primary key")
}

func (c *collectionSchema) RequireQueryIndex() bool {
	return c.raw.Get(string(requireIndexPath)).Bool()
}

func (c *collectionSchema) PrimaryIndex() Index {
	return c.primaryIndex
}

func (c *collectionSchema) Properties() map[string]SchemaProperty {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var i = map[string]SchemaProperty{}
	for k, v := range c.properties {
		i[k] = v
	}
	return i
}

func (c *collectionSchema) ForeignKeys() map[string]SchemaProperty {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var i = map[string]SchemaProperty{}
	for k, v := range c.properties {
		if v.ForeignKey.Collection != "" {
			i[k] = v
		}
	}
	return i
}

func (c *collectionSchema) HasPropertyPath(p string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.properties[p].Name != ""
}
