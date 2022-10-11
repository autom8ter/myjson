package wolverine

import (
	"fmt"
	"strings"

	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/xeipuuv/gojsonschema"

	"github.com/autom8ter/wolverine/internal/prefix"
)

// LoadCollection loads a collection from the provided json schema
func LoadCollection(jsonSchema string) (*Collection, error) {
	c := &Collection{JSONSchema: jsonSchema}
	loadedSchema, err := gojsonschema.NewSchema(gojsonschema.NewStringLoader(c.JSONSchema))
	if err != nil {
		return nil, err
	}
	c.loadedSchema = loadedSchema
	return c, nil
}

// Collection is a collection of records of a given type
type Collection struct {
	// JSONSchema is a json schema used to validate documents stored in the collection
	JSONSchema   string `json:"json_schema"`
	loadedSchema *gojsonschema.Schema
}

func (c *Collection) Collection() string {
	return cast.ToString(gjson.Get(c.JSONSchema, "collection").Value())
}

func (c *Collection) Indexes() []Index {
	var is []Index
	indexes := gjson.Get(c.JSONSchema, "indexes").Array()
	for _, i := range indexes {
		in := &Index{}
		fields := i.Get("fields").Array()
		for _, field := range fields {
			in.Fields = append(in.Fields, cast.ToString(field.Value()))
		}
		is = append(is, *in)
	}
	return is
}

// Validate validates the document against the collections json schema (if it exists)
func (c *Collection) Validate(doc *Document) (bool, error) {
	var err error
	if c.JSONSchema == "" {
		return true, nil
	}
	if c.loadedSchema == nil {
		c.loadedSchema, err = gojsonschema.NewSchema(gojsonschema.NewStringLoader(c.JSONSchema))
		if err != nil {
			return false, err
		}
	}
	documentLoader := gojsonschema.NewBytesLoader(doc.Bytes())
	result, err := c.loadedSchema.Validate(documentLoader)
	if err != nil {
		return false, err
	}
	if !result.Valid() {
		var errs []string
		for _, err := range result.Errors() {
			errs = append(errs, err.String())
		}
		return false, fmt.Errorf("%s", strings.Join(errs, ","))
	}
	return true, nil
}

func (c Collection) FullText() bool {
	return gjson.Get(c.JSONSchema, "full_text").Bool()
}

// Index is a database index used for quickly finding records with specific field values
type Index struct {
	// Fields is a list of fields that are indexed
	Fields []string `json:"fields"`
}

func (i Index) prefix(collection string) *prefix.PrefixIndexRef {
	return prefix.NewPrefixedIndex(collection, i.Fields)
}
