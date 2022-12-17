package model

import (
	"bytes"

	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/nqd/flat"
)

// Index is a database index used to optimize queries against a collection
type Index struct {
	// Name is the indexes unique name in the collection
	Name string `json:"name"`
	// Fields to index - order matters
	Fields []string `json:"fields"`
	// Unique indicates that it's a unique index which will enforce uniqueness
	Unique bool `json:"unique"`
	// Unique indicates that it's a primary index
	Primary bool `json:"primary"`
	// IsBuilding indicates that the index is currently building
	IsBuilding bool `json:"isBuilding"`
}

// indexPrefix is a reference to a prefix within an index
type IndexPrefix interface {
	// Append appends a field value to an index prefix
	Append(field string, value any) IndexPrefix
	// Path returns the full path of the prefix
	Path() []byte
	// Fields returns the fields contained in the index prefix
	Fields() []FieldValue
	// DocumentID returns the document id set as the suffix of the prefix when Path() is called
	// This allows the index to seek to the position of an individual document
	DocumentID() string
	// SetDocumentID sets the document id as the suffix of the prefix when Path() is called
	// This allows the index to seek to the position of an individual document
	SetDocumentID(id string) IndexPrefix
}

// FieldValue is a key value pair
type FieldValue struct {
	Field string `json:"field"`
	Value any    `json:"value"`
}

func (i Index) SeekPrefix(collection string, fields map[string]any) IndexPrefix {
	fields, _ = flat.Flatten(fields, nil)
	var prefix = IndexPrefix(IndexPathPrefix{
		prefix: [][]byte{
			[]byte("index"),
			[]byte(collection),
			[]byte(i.Name),
		},
	})
	if i.Fields == nil {
		return prefix
	}
	for _, k := range i.Fields {
		if v, ok := fields[k]; ok {
			prefix = prefix.Append(k, v)
		}
	}
	return prefix
}

type IndexPathPrefix struct {
	prefix     [][]byte
	documentID string
	fields     [][]byte
	fieldMap   []FieldValue
}

func (p IndexPathPrefix) Append(field string, value any) IndexPrefix {
	fields := append(p.fields, []byte(field), util.EncodeIndexValue(value))
	fieldMap := append(p.fieldMap, FieldValue{
		Field: field,
		Value: value,
	})
	return IndexPathPrefix{
		prefix:   p.prefix,
		fields:   fields,
		fieldMap: fieldMap,
	}
}

func (p IndexPathPrefix) SetDocumentID(id string) IndexPrefix {
	return IndexPathPrefix{
		prefix:     p.prefix,
		documentID: id,
		fields:     p.fields,
		fieldMap:   p.fieldMap,
	}
}

func (p IndexPathPrefix) Path() []byte {
	var path = append(p.prefix, p.fields...)
	if p.documentID != "" {
		path = append(path, []byte(p.documentID))
	}
	return bytes.Join(path, []byte("\x00"))
}

func (i IndexPathPrefix) DocumentID() string {
	return i.documentID
}

func (i IndexPathPrefix) Fields() []FieldValue {
	return i.fieldMap
}
