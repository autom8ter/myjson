package gokvkit

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/util"
	"github.com/tidwall/gjson"
	"github.com/xeipuuv/gojsonschema"
)

type collectionSchema struct {
	schema        *gojsonschema.Schema
	raw           gjson.Result
	collection    string
	primaryIndex  Index
	indexing      map[string]Index
	properties    map[string]SchemaProperty
	propertyPaths map[string]SchemaProperty
	triggers      []Trigger
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
	refPrefix                   = "common."
)

func newCollectionSchema(yamlContent []byte) (CollectionSchema, error) {
	if len(yamlContent) == 0 {
		return nil, errors.New(errors.Validation, "empty schema content")
	}
	jsonContent, err := util.YAMLToJSON(yamlContent)
	if err != nil {
		return nil, err
	}
	schema, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(jsonContent))
	if err != nil {
		return nil, errors.Wrap(err, errors.Validation, "invalid json schema")
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
	if err := s.loadProperties(s.properties, s.raw.Get("properties")); err != nil {
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
	if triggers := s.raw.Get("triggers"); triggers.Exists() {
		for name, t := range triggers.Map() {
			var trig Trigger
			trig.Name = name
			if err := util.Decode(t.Value(), &trig); err != nil {
				return nil, errors.Wrap(err, errors.Validation, "invalid trigger")
			}
			if err := util.ValidateStruct(trig); err != nil {
				return nil, errors.Wrap(err, errors.Validation, "invalid trigger: %s", trig.Name)
			}
			s.triggers = append(s.triggers, trig)
		}
	}
	sort.Slice(s.triggers, func(i, j int) bool {
		return s.triggers[i].Order < s.triggers[j].Order
	})
	return s, nil
}

func (c *collectionSchema) loadRef(fieldPath string, ref string) (gjson.Result, error) {
	fieldPath = strings.ReplaceAll(fieldPath, "properties.", "")
	path := strings.TrimPrefix(ref, "#/")
	path = strings.ReplaceAll(path, "/", ".")
	if !strings.HasPrefix(path, refPrefix) {
		return gjson.Result{}, errors.New(errors.Validation, "references may only exist under #/common got: %s", path)
	}
	return c.raw.Get(path), nil
}

func (s *collectionSchema) loadProperties(properties map[string]SchemaProperty, r gjson.Result) error {
	if !r.Exists() {
		return nil
	}
	var err error
	for key, value := range r.Map() {
		path := strings.ReplaceAll(value.Path(s.raw.Raw), "properties.", "")
		if value.Get("$ref").Exists() {
			value, err = s.loadRef(value.Path(s.raw.Raw), value.Get("$ref").String())
			if err != nil {
				return err
			}

		}
		if strings.HasPrefix(path, refPrefix) {
			path = strings.TrimPrefix(path, refPrefix)
		}
		schema := SchemaProperty{
			Primary:     value.Get(string(primaryPath)).Bool(),
			Name:        key,
			Description: value.Get("description").String(),
			Type:        value.Get("type").String(),
			Unique:      value.Get(string(uniquePath)).Bool(),
			Path:        path,
			Properties:  map[string]SchemaProperty{},
		}

		if props := value.Get("properties"); props.Exists() && schema.Type == "object" {
			if err := s.loadProperties(schema.Properties, props); err != nil {
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
		if i := value.Get(string(indexPath)); i.Exists() && schema.Type != "object" {
			var indexing = map[string]PropertyIndex{}
			if i := value.Get(string(indexPath)); i.Exists() && schema.Type != "object" {
				if err := util.Decode(i.Value(), &indexing); err != nil {
					return errors.Wrap(err, errors.Validation, "failed to decode index on property: %s", i.String())
				}
				for name, idx := range indexing {
					fields := []string{path}
					fields = append(fields, idx.AdditionalFields...)
					s.indexing[name] = Index{
						Name:    name,
						Fields:  fields,
						Unique:  false,
						Primary: false,
					}
				}
			}
		}
		properties[key] = schema
		s.propertyPaths[path] = schema
		switch {
		case schema.Primary:
			if s.primaryIndex.Name != "" {
				return errors.New(errors.Validation, "multiple primary keys found")
			}
			idxName := fmt.Sprintf("%s.primaryidx", path)
			s.indexing[idxName] = Index{
				Name:    idxName,
				Fields:  []string{path},
				Unique:  true,
				Primary: true,
			}
			s.primaryIndex = s.indexing[idxName]
		case schema.Unique:
			idxName := fmt.Sprintf("%s.uniqueidx", path)
			s.indexing[idxName] = Index{
				Name:    idxName,
				Fields:  []string{path},
				Unique:  true,
				Primary: false,
			}
		case schema.ForeignKey != nil:
			idxName := fmt.Sprintf("%s.foreignidx", path)
			s.indexing[idxName] = Index{
				Name:       idxName,
				Fields:     []string{path},
				Unique:     schema.Unique,
				Primary:    false,
				ForeignKey: schema.ForeignKey,
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
	kerrs, err := c.schema.Validate(gojsonschema.NewBytesLoader(doc.Bytes()))
	if err != nil {
		return errors.Wrap(err, errors.Validation, "%v: failed to validate document", c.collection)
	}

	if kerrs != nil && len(kerrs.Errors()) > 0 {
		errmsg := ""
		for _, kerr := range kerrs.Errors() {
			errmsg += fmt.Sprintln(kerr)
		}
		return errors.New(errors.Validation, "%v: document validation error - %s", c.collection, errmsg)
	}
	if !kerrs.Valid() {
		return errors.New(errors.Validation, "%v: invalid document: %s", c.collection, doc.String())
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

func (c *collectionSchema) PropertyPaths() map[string]SchemaProperty {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.propertyPaths
}

func (c *collectionSchema) HasPropertyPath(p string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.propertyPaths[p].Name != ""
}

func (c *collectionSchema) Triggers() []Trigger {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.triggers
}
