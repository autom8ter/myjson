package gokvkit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/autom8ter/gokvkit/model"
	"github.com/qri-io/jsonschema"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type CollectionSchema interface {
	Collection() string
	ValidateDocument(ctx context.Context, doc *model.Document) error
	Indexing() map[string]model.Index
	SetIndex(index model.Index) error
	DelIndex(name string) error
	PrimaryIndex() model.Index
	// PrimaryKey returns the collections primary key
	PrimaryKey() string
	GetPrimaryKey(doc *model.Document) string
	SetPrimaryKey(doc *model.Document, id string) error
	RequireQueryIndex() bool
	Bytes() ([]byte, error)
}

type collectionSchema struct {
	schema       *jsonschema.Schema
	raw          gjson.Result
	collection   string
	primaryIndex model.Index
	indexing     map[string]model.Index
}

type schemaPath string

const (
	collectionPath   schemaPath = "x-collection"
	indexingPath     schemaPath = "x-indexing"
	requireIndexPath schemaPath = "x-require-index"
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
		schema:     schema,
		raw:        r,
		collection: r.Get(string(collectionPath)).String(),
		indexing:   map[string]model.Index{},
	}
	for _, index := range s.raw.Get(string(indexingPath)).Map() {
		var i model.Index
		err = util.Decode(index.Value(), &i)
		if err != nil {
			continue
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
	return s, nil
}

func (c *collectionSchema) Bytes() ([]byte, error) {
	return util.JSONToYAML([]byte(c.raw.Raw))
}

func (c *collectionSchema) Collection() string {
	return c.collection
}

func (c *collectionSchema) Indexing() map[string]model.Index {
	return c.indexing
}

func (c *collectionSchema) SetIndex(index model.Index) error {
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

func (c *collectionSchema) ValidateDocument(ctx context.Context, doc *model.Document) error {
	kerrs, err := c.schema.ValidateBytes(ctx, doc.Bytes())
	if err != nil {
		return errors.Wrap(err, errors.Validation, "%v: failed to validate document", c.collection)
	}
	if kerrs != nil && len(kerrs) > 0 {
		return errors.New(errors.Validation, "%v: invalid document- %v", c.collection, util.JSONString(kerrs))
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

func (c *collectionSchema) GetPrimaryKey(doc *model.Document) string {
	if doc == nil {
		return ""
	}
	return doc.GetString(c.PrimaryKey())
}

func (c *collectionSchema) SetPrimaryKey(doc *model.Document, id string) error {
	pkey := c.PrimaryKey()
	return errors.Wrap(doc.Set(pkey, id), 0, "failed to set primary key")
}

func (c *collectionSchema) RequireQueryIndex() bool {
	return c.raw.Get(string(requireIndexPath)).Bool()
}

func (c *collectionSchema) PrimaryIndex() model.Index {
	return c.primaryIndex
}
