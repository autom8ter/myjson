package gokvkit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/util"
	"github.com/qri-io/jsonschema"
	"github.com/tidwall/gjson"
)

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
	requireIndexPath schemaPath = "x-require-index"
	foreignKeyPath   schemaPath = "x-foreign"
	indexPath        schemaPath = "x-index"
	primaryPath      schemaPath = "x-primary"
	uniquePath       schemaPath = "x-unique"
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
	if err != nil {
		return nil, err
	}
	if err := s.loadProperties(s.raw.Get("properties")); err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	for _, f := range s.propertyPaths {
		if err := util.ValidateStruct(f); err != nil {
			return nil, err
		}
	}
	for _, i := range s.indexing {
		if err := util.ValidateStruct(i); err != nil {
			return nil, err
		}
	}
	if len(s.primaryIndex.Fields) == 0 {
		return nil, errors.New(errors.Validation, "primary index is required")
	}
	return s, nil
}

func (s *collectionSchema) loadProperties(r gjson.Result) error {
	if !r.Exists() {
		return nil
	}
	for key, value := range r.Map() {
		schema := SchemaProperty{
			Primary:     value.Get(string(primaryPath)).Bool(),
			Name:        key,
			Description: value.Get("description").String(),
			Type:        value.Get("type").String(),
			SchemaPath:  value.Path(s.raw.Raw),
			Path:        strings.ReplaceAll(value.Path(s.raw.Raw), "properties.", ""),
			Unique:      value.Get(string(uniquePath)).Bool(),
			Properties:  map[string]SchemaProperty{},
		}
		if properties := value.Get("properties"); properties.Exists() && schema.Type == "object" {
			if err := s.loadProperties(properties); err != nil {
				return err
			}
		}
		if fkey := value.Get(string(foreignKeyPath)); fkey.Exists() && schema.Type != "object" {
			var foreign ForeignKey
			if err := util.Decode(fkey.Value(), &foreign); err != nil {
				return errors.Wrap(err, errors.Validation, "failed to decode foreignKey on property: %s", fkey.String())
			}
			schema.ForeignKey = &foreign
		}
		if !schema.Primary && !schema.Unique && schema.ForeignKey == nil {
			var index PropertyIndex
			if i := value.Get(string(indexPath)); i.Exists() && schema.Type != "object" {
				if err := util.Decode(i.Value(), &i); err != nil {
					return errors.Wrap(err, errors.Validation, "failed to decode index on property: %s", i.String())
				}
				schema.Index = &index
				idxName := fmt.Sprintf("%s.idx", schema.Path)
				if len(schema.Index.AdditionalFields) > 0 {
					idxName = fmt.Sprintf("%s.%s.idx", schema.Path, strings.Join(schema.Index.AdditionalFields, "."))
				}
				fields := []string{schema.Path}
				fields = append(fields, schema.Index.AdditionalFields...)
				s.indexing[idxName] = Index{
					Name:    idxName,
					Fields:  fields,
					Unique:  false,
					Primary: false,
				}
			}
		}
		s.properties[key] = schema
		s.propertyPaths[schema.Path] = schema
		switch {
		case schema.Primary:
			if s.primaryIndex.Name != "" {
				return errors.New(errors.Validation, "multiple primary keys found")
			}
			idxName := fmt.Sprintf("%s.primaryidx", schema.Path)
			s.indexing[idxName] = Index{
				Name:    idxName,
				Fields:  []string{schema.Path},
				Unique:  true,
				Primary: true,
			}
			s.primaryIndex = s.indexing[idxName]
		case schema.Unique:
			idxName := fmt.Sprintf("%s.uniqueidx", schema.Path)
			s.indexing[idxName] = Index{
				Name:    idxName,
				Fields:  []string{schema.Path},
				Unique:  true,
				Primary: false,
			}
		case schema.ForeignKey != nil:
			idxName := fmt.Sprintf("%s.foreignidx", schema.Path)
			s.indexing[idxName] = Index{
				Name:    idxName,
				Fields:  []string{schema.Path},
				Unique:  schema.Unique,
				Primary: false,
			}
		}
	}
	return nil
}

func (c *collectionSchema) refreshSchema(jsonContent []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := newCollectionSchema(jsonContent)
	if err != nil {
		return err
	}
	newSchema := s.(*collectionSchema)
	c.raw = newSchema.raw
	c.schema = newSchema.schema
	c.indexing = newSchema.indexing
	c.propertyPaths = newSchema.propertyPaths
	c.properties = newSchema.properties
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
	return c.properties
}

func (c *collectionSchema) HasPropertyPath(p string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.propertyPaths[p].Name != ""
}
