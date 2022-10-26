package core

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

// JSONSchemaConfig holds custom properties extracted from a JSON schema file
type JSONSchemaConfig struct {
	// Collection is the object's collection name - it is extracted from the schemas @config.collection field
	Collection string `json:"collection" validate:"required,lowercase,alpha"`
	// PrimaryKey is the objects unique identifier which is gathered from the firt property specifying '@primary: true'
	PrimaryKey string `json:"primaryKey" validate:"required"`
	// Indexing is the object's configured indexing - it is extracted from the schemas @config.indexing field
	Indexing Indexing `json:"indexing" validate:"required"`
}

// Validate validates the json schema config
func (j JSONSchemaConfig) Validate() error {
	validate := validator.New()
	return validate.Struct(&j)
}

// JSONSchema is a custom json schema used by collections for configuration and type validation
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

// NewJSONSchema creates a new json schema from the given bytes
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
	properties := parsed.Get("properties")
	for k, v := range properties.Map() {
		switch {
		case v.Get("@primary").Exists() && config.PrimaryKey == "":
			config.PrimaryKey = k
		}
	}
	if config.PrimaryKey == "" {
		return nil, stacktrace.NewError("missing property: @config.primaryKey")
	}
	if config.Collection == "" {
		return nil, stacktrace.NewError("missing property: @config.collection")
	}
	c := &collectionSchema{
		schema: rs,
		raw:    parsed,
		config: config,
	}
	return c, config.Validate()
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
