package gokvkit

import (
	"context"
	"encoding/json"
	"time"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/autom8ter/gokvkit/model"
	"github.com/qri-io/jsonschema"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
)

type collectionSchema struct {
	yamlRaw    []byte
	collection string
	indexing   map[string]model.Index
	required   []string
	properties map[string]any
	schema     *jsonschema.Schema
	raw        gjson.Result
}

type schemaPath string

const (
	collectionPath schemaPath = "x-collection"
	indexingPath   schemaPath = "x-indexing"
)

func newCollectionSchema(schemaContent []byte) (*collectionSchema, error) {
	if len(schemaContent) == 0 {
		return nil, errors.New(errors.Validation, "empty schema content")
	}
	var (
		schema = &jsonschema.Schema{}
	)
	jsonContent, err := util.YAMLToJSON(schemaContent)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(jsonContent, schema); err != nil {
		return nil, errors.Wrap(err, 0, "failed to decode json schema")
	}

	var c = &collectionSchema{
		schema:  schema,
		yamlRaw: schemaContent,
	}
	r := gjson.ParseBytes(jsonContent)

	if !r.Get(string(collectionPath)).Exists() {
		return nil, errors.New(errors.Validation, "schema does not have 'x-collection' property")
	}
	c.raw = r
	if !r.Get("properties").Exists() {
		return nil, errors.New(errors.Validation, "schema does not have 'properties' property")
	}
	if !r.Get(string(indexingPath)).Exists() {
		return nil, errors.New(errors.Validation, "schema does not have 'properties' property")
	}
	c.collection = r.Get(string(collectionPath)).String()
	if !r.Get(string(indexingPath)).IsObject() {
		return nil, errors.New(errors.Validation, "'indexing' property must be an object")
	}
	if err := util.Decode(r.Get(string(indexingPath)).Value(), &c.indexing); err != nil {
		return nil, err
	}
	c.properties = cast.ToStringMap(r.Get(string(collectionPath)).Value())
	required, ok := r.Get("required").Value().([]any)
	if ok {
		c.required = cast.ToStringSlice(required)
	}
	return c, nil
}

func (j *collectionSchema) MarshalJSON() ([]byte, error) {
	return []byte(j.raw.Raw), nil
}

func (j *collectionSchema) UnmarshalJSON(bytes []byte) error {
	n, err := newCollectionSchema(bytes)
	if err != nil {
		return err
	}
	*j = *n
	return nil
}

func (j *collectionSchema) validateCommand(ctx context.Context, command *model.Command) error {
	if command.Metadata == nil {
		md, _ := model.GetMetadata(ctx)
		command.Metadata = md
	}
	if command.Timestamp.IsZero() {
		command.Timestamp = time.Now()
	}
	if err := command.Validate(); err != nil {
		return err
	}
	switch command.Action {
	case model.Update, model.Create, model.Set:
		if command.After != nil {
			kerrs := j.schema.Validate(ctx, command.After.Value()).Errs
			if kerrs != nil && len(*kerrs) > 0 {
				return errors.New(errors.Validation, "%v", util.JSONString(*kerrs))
			}
		}
	case model.Delete:
		if command.DocID == "" {
			return errors.New(errors.Validation, "empty document id")
		}
	}
	return nil
}
