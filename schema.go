package gokvkit

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/autom8ter/gokvkit/model"
	"github.com/palantir/stacktrace"
	"github.com/qri-io/jsonschema"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"gopkg.in/yaml.v2"
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

func newCollectionSchema(schemaContent []byte) (*collectionSchema, error) {
	if len(schemaContent) == 0 {
		return nil, stacktrace.NewError("empty schema content")
	}
	var (
		schema = &jsonschema.Schema{}
	)
	var body map[interface{}]interface{}
	if err := yaml.Unmarshal(schemaContent, &body); err != nil {
		return nil, stacktrace.Propagate(err, "failed to decode json schema from yaml")
	}
	jsonContent, err := json.Marshal(util.ConvertMap(body))
	if err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	if err := json.Unmarshal(jsonContent, schema); err != nil {
		return nil, stacktrace.Propagate(err, "failed to decode json schema")
	}

	var c = &collectionSchema{
		schema:  schema,
		yamlRaw: schemaContent,
	}
	r := gjson.ParseBytes(jsonContent)

	if !r.Get("collection").Exists() {
		return nil, stacktrace.NewError("schema does not have 'collection' property")
	}
	c.raw = r
	if !r.Get("properties").Exists() {
		return nil, stacktrace.NewError("schema does not have 'properties' property")
	}
	if !r.Get("indexing").Exists() {
		return nil, stacktrace.NewError("schema does not have 'properties' property")
	}
	c.collection = r.Get("collection").String()
	if !r.Get("indexing").IsObject() {
		return nil, stacktrace.NewError("'indexing' property must be an object")
	}
	if err := util.Decode(r.Get("indexing").Value(), &c.indexing); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	c.properties = cast.ToStringMap(r.Get("collection").Value())
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
		return stacktrace.Propagate(err, "")
	}
	*j = *n
	return nil
}

func (j *collectionSchema) validateCommand(ctx context.Context, command *model.Command) error {
	switch command.Action {
	case model.Update, model.Create, model.Set:
		if command.After != nil {
			kerrs := j.schema.Validate(ctx, command.After).Errs
			if kerrs != nil && len(*kerrs) > 0 {
				return fmt.Errorf(util.JSONString(&kerrs))
			}
		}
	}
	return nil
}
