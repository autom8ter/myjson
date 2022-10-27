package core

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/autom8ter/wolverine/internal/util"
	"github.com/nqd/flat"
	"github.com/palantir/stacktrace"
	"github.com/qri-io/jsonschema"
	"github.com/tidwall/gjson"
	"strings"
)

// JSONSchema is a custom json schema used by collections for configuration and type validation
type JSONSchema interface {
	// Validate validates the input json document
	Validate(ctx context.Context, jsonDocument []byte) error
	// Collection returns the collection name
	Collection() string
	// PrimaryKey returns the schemas primary key name
	PrimaryKey() string
	// Indexing returns the schema's indexing configuration
	Indexing() Indexing
	// GetFlag gets a flag from the schema
	GetFlag(name string) bool
	// GetAnnotation gets an annotation from the schema
	GetAnnotation(name string) string
	fmt.Stringer
	json.Marshaler
	json.Unmarshaler
}

type collectionSchema struct {
	schema *jsonschema.Schema
	raw    gjson.Result
	// Collection is the object's collection name - it is extracted from the schemas @config.collection field
	collection string
	// PrimaryKey is the objects unique identifier which is gathered from the firt property specifying '@primary: true'
	primaryKey string
	// Indexing is the object's configured indexing - it is extracted from the schemas @config.indexing field
	indexing Indexing
	// Flags are arbitrary key boolean pairs
	flags map[string]bool
	// Annotations are arbitrary key value pairs
	annotations      map[string]string
	uniqueProperties map[string]struct{}
	fields           map[string]*field
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
	c := &collectionSchema{
		schema:      rs,
		raw:         parsed,
		collection:  parsed.Get("@collection").String(),
		primaryKey:  "",
		indexing:    Indexing{},
		flags:       map[string]bool{},
		annotations: map[string]string{},
		fields:      map[string]*field{},
	}
	fields, err := getFields(parsed.Get("properties"))
	if err != nil {
		return nil, stacktrace.Propagate(err, "failed to parse properties")
	}
	c.fields = fields
	//flatProps, err := flat.Flatten(cast.ToStringMap(c.raw.Get("properties").Value()), nil)
	//if err != nil {
	//	return nil, stacktrace.Propagate(err, "")
	//}
	//for field, value := range flatProps {
	//	fieldSplit := strings.Split(field, ".")
	//	leaf := fieldSplit[len(fieldSplit)-1]
	//	switch leaf {
	//	case "@primary":
	//		c.primaryKey = fieldSplit[len(fieldSplit)-2]
	//	case "@unique":
	//		c.uniqueProperties[fieldSplit[len(fieldSplit)-2]] = struct{}{}
	//	}
	//}
	for k, v := range c.fields {
		if v.Props["@primary"] != nil {
			c.primaryKey = k
		}
		if v.Props["@unique"] != nil {
			c.uniqueProperties[k] = struct{}{}
		}
	}
	if c.primaryKey == "" {
		return nil, stacktrace.NewError("missing primary key: @primary")
	}
	if c.collection == "" {
		return nil, stacktrace.NewError("missing collection: @collection")
	}
	if parsed.Get("@flags").Exists() {
		if err := util.Decode(parsed.Get("@flags").Value(), &c.flags); err != nil {
			return nil, stacktrace.Propagate(err, "failed to decode @flags")
		}
	}
	if parsed.Get("@annotations").Exists() {
		if err := util.Decode(parsed.Get("@annotations").Value(), &c.flags); err != nil {
			return nil, stacktrace.Propagate(err, "failed to decode @annotations")
		}
	}
	if parsed.Get("@indexing").Exists() {
		if err := util.Decode(parsed.Get("@indexing").Value(), &c.indexing); err != nil {
			return nil, stacktrace.Propagate(err, "failed to decode @indexing")
		}
	}
	return c, nil
}

func (c *collectionSchema) Collection() string {
	return c.collection
}

func (c *collectionSchema) PrimaryKey() string {
	return c.primaryKey
}

func (c *collectionSchema) Indexing() Indexing {
	return c.indexing
}

func (c *collectionSchema) GetFlag(name string) bool {
	return c.flags[name]
}

func (c *collectionSchema) GetAnnotation(name string) string {
	return c.annotations[name]
}

func (c *collectionSchema) MarshalJSON() ([]byte, error) {
	return c.schema.MarshalJSON()
}

func (c *collectionSchema) UnmarshalJSON(bytes []byte) error {
	return c.schema.UnmarshalJSON(bytes)
}

func (c *collectionSchema) String() string {
	return util.JSONString(c)
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

type field struct {
	Selector string                 `json:"selector"`
	Props    map[string]interface{} `json:"props"`
}

func getFields(result gjson.Result) (map[string]*field, error) {
	flattened, err := flat.Flatten(result.Value().(map[string]interface{}), nil)
	if err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	var fields = map[string]*field{}
	for k, v := range flattened {
		path := strings.Split(k, ".")
		leaf := path[len(path)-1]
		selector := strings.ReplaceAll(strings.Join(path[:len(path)-1], "."), "properties.", "")
		if _, ok := fields[selector]; !ok {
			fields[selector] = &field{Selector: selector, Props: map[string]interface{}{}}
		}
		fields[selector].Props[leaf] = v
	}
	return fields, nil
}
