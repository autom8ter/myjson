package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/autom8ter/wolverine/internal/util"
	"github.com/go-playground/validator/v10"
	"github.com/palantir/stacktrace"
	"github.com/qri-io/jsonschema"
	"github.com/tidwall/gjson"
)

type JSONSchemaConfig struct {
	Collection  string                `json:"collection" validate:"required,lowercase,alpha"`
	PrimaryKey  string                `json:"primaryKey" validate:"required"`
	Indexing    Indexing              `json:"indexing" validate:"required"`
	ForeignKeys map[string]ForeignKey `json:"foreignKeys"`
}

func (j JSONSchemaConfig) Validate() error {
	validate := validator.New()
	return validate.Struct(&j)
}

type JSONSchema interface {
	Validate(ctx context.Context, bits []byte) error
	Config() JSONSchemaConfig
	json.Marshaler
	json.Unmarshaler
}

type collectionSchema struct {
	schema *jsonschema.Schema
	raw    gjson.Result
	config JSONSchemaConfig
}

func NewJSONSchema(schemaData []byte) (JSONSchema, error) {
	parsed := gjson.ParseBytes(schemaData)
	if parsed.Get("type").String() != "object" {
		return nil, stacktrace.NewError("'type' must be 'object'")
	}
	rs := &jsonschema.Schema{}
	if err := json.Unmarshal(schemaData, rs); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	var config JSONSchemaConfig
	configExtract := parsed.Get("@config")
	if !configExtract.Exists() {
		return nil, stacktrace.NewError("missing property: @config")
	}
	if err := json.Unmarshal([]byte(configExtract.Raw), &config); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	if config.ForeignKeys == nil {
		config.ForeignKeys = map[string]ForeignKey{}
	}
	properties := parsed.Get("properties")
	for k, v := range properties.Map() {
		switch {
		case v.Get("@foreign").Exists():
			var fkey ForeignKey
			if err := util.Decode(v.Get("@foreign").Value(), &fkey); err != nil {
				return nil, stacktrace.Propagate(err, "failed to decode foreign key")
			}
			config.ForeignKeys[k] = fkey
		case v.Get("@primary").Exists() && config.PrimaryKey == "":
			config.PrimaryKey = v.Get("@primary").String()
		}
	}
	if config.PrimaryKey == "" {
		return nil, stacktrace.NewError("missing property: @config.primaryKey")
	}
	if config.Collection == "" {
		return nil, stacktrace.NewError("missing property: @config.collection")
	}
	return &collectionSchema{
		schema: rs,
		raw:    parsed,
		config: config,
	}, nil
}

func (c *collectionSchema) Config() JSONSchemaConfig {
	return c.config
}

func (c *collectionSchema) MarshalJSON() ([]byte, error) {
	return c.schema.MarshalJSON()
}

func (c *collectionSchema) UnmarshalJSON(bytes []byte) error {
	return c.schema.UnmarshalJSON(bytes)
}

func (c *collectionSchema) Validate(ctx context.Context, bits []byte) error {
	kerrs, err := c.schema.ValidateBytes(ctx, bits)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	if len(kerrs) > 0 {
		return fmt.Errorf("schema validation error: %s", util.JSONString(&kerrs))
	}
	return nil
}
