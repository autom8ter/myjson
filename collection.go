package wolverine

import (
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/xeipuuv/gojsonschema"

	"github.com/autom8ter/wolverine/internal/prefix"
)

// LoadCollection loads a collection from the provided json schema
func LoadCollection(jsonSchema string) (*Collection, error) {
	c := &Collection{Schema: jsonSchema}
	loadedSchema, err := gojsonschema.NewSchema(gojsonschema.NewStringLoader(c.Schema))
	if err != nil {
		return nil, stacktrace.PropagateWithCode(err, ErrSchemaLoad, "failed to load schema")
	}
	c.loadedSchema = loadedSchema
	if c.Collection() == "" {
		return nil, stacktrace.NewErrorWithCode(ErrEmptySchemaCollection, "empty 'collection' property in jsonSchema")
	}
	return c, nil
}

// Collection is a collection of records of a given type. It is
type Collection struct {
	// Schema is an extended json schema used to validate documents stored in the collection.
	// Custom properties include: collection, indexes, and full_text
	Schema       string `json:"schema"`
	loadedSchema *gojsonschema.Schema
}

// ParseSchema parses the collection's json schema
func (c *Collection) ParseSchema() error {
	var err error
	c.loadedSchema, err = gojsonschema.NewSchema(gojsonschema.NewStringLoader(c.Schema))
	if err != nil {
		return stacktrace.PropagateWithCode(err, ErrSchemaLoad, "failed to load schema")
	}
	if c.Collection() == "" {
		return stacktrace.NewErrorWithCode(ErrEmptySchemaCollection, "empty 'collection' property in jsonSchema")
	}
	return nil
}

// Collection returns the name of the collection based on the schema's 'collection' field on the collection's schema
func (c *Collection) Collection() string {
	return cast.ToString(gjson.Get(c.Schema, "collection").Value())
}

// Indexes returns the list of the indexes based on the schema's 'indexes' field on the collection's schema
func (c *Collection) Indexes() []Index {
	var is []Index
	indexes := gjson.Get(c.Schema, "indexes").Array()
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
	if c.Schema == "" {
		return true, nil
	}
	if c.loadedSchema == nil {
		c.loadedSchema, err = gojsonschema.NewSchema(gojsonschema.NewStringLoader(c.Schema))
		if err != nil {
			return false, stacktrace.PropagateWithCode(err, ErrSchemaLoad, "")
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
		return false, stacktrace.NewErrorWithCode(ErrDocumentValidation, "%s", strings.Join(errs, ","))
	}
	return true, nil
}

// FullText returns true/false based on the 'full_text' field on the collection's schema
func (c Collection) FullText() bool {
	return gjson.Get(c.Schema, "full_text").Bool()
}

// Index is a database index used for quickly finding records with specific field values
type Index struct {
	// Fields is a list of fields that are indexed
	Fields []string `json:"fields"`
}

func (i Index) prefix(collection string) *prefix.PrefixIndexRef {
	return prefix.NewPrefixedIndex(collection, i.Fields)
}
