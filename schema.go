package gokvkit

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/palantir/stacktrace"
	"github.com/qri-io/jsonschema"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
)

type collectionSchema struct {
	collection string
	indexing   map[string]Index
	required   []string
	properties map[string]any
	schema     *jsonschema.Schema
	raw        gjson.Result
}

func newCollectionSchema(schemaContent []byte) (*collectionSchema, error) {
	schema := &jsonschema.Schema{}
	if err := json.Unmarshal(schemaContent, schema); err != nil {
		return nil, stacktrace.Propagate(err, "failed to decode json schema")
	}
	var c = &collectionSchema{
		schema: schema,
	}
	r := gjson.ParseBytes(schemaContent)
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
	if err := Decode(r.Get("indexing").Value(), &c.indexing); err != nil {
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

func (j *collectionSchema) validateCommand(ctx context.Context, command *Command) error {
	switch command.Action {
	case UpdateDocument, CreateDocument, SetDocument:
		if command.Change != nil {
			kerrs, err := j.schema.ValidateBytes(ctx, command.Change.Bytes())
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if len(kerrs) > 0 {
				return fmt.Errorf(JSONString(&kerrs))
			}
		}
	}
	return nil
}
