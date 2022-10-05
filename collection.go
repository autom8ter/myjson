package wolverine

import "github.com/xeipuuv/gojsonschema"

// Collection is a collection of records of a given type
type Collection struct {
	// Name is the unique name of the collection - it should not contain any special characters
	Name string `json:"name"`
	// Indexes is a list of indexes associated with the collection - indexes should be used to tune database performance
	Indexes      []Index `json:"indexes"`
	JSONSchema   string  `json:"json_schema"`
	loadedSchema *gojsonschema.Schema
}

// Validate validates the document against the collections json schema (if it exists)
func (c Collection) Validate(doc *Document) (bool, error) {
	if c.loadedSchema == nil {
		return true, nil
	}
	documentLoader := gojsonschema.NewBytesLoader(doc.Bytes())
	result, err := c.loadedSchema.Validate(documentLoader)
	if err != nil {
		return false, err
	}
	if !result.Valid() {
		return false, nil
	}
	return true, nil
}

// Index is a database index used for quickly finding records with specific field values
type Index struct {
	// Fields is a list of fields that are indexed
	Fields []string `json:"fields"`
	// FullText is a boolean value indicating whether the fields will be full text search-able
	FullText bool `json:"full_text"`
}
