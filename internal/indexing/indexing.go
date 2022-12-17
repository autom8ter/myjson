package indexing

import (
	"bytes"

	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/autom8ter/gokvkit/model"
	"github.com/nqd/flat"
)

// FieldValue is a key value pair
type FieldValue struct {
	Field string `json:"field"`
	Value any    `json:"value"`
}

// SeekPrefix seeks to a given IndexPathPrefix
func SeekPrefix(collection string, i model.Index, fields map[string]any) IndexPathPrefix {
	fields, _ = flat.Flatten(fields, nil)
	var prefix = IndexPathPrefix{
		prefix: [][]byte{
			[]byte("index"),
			[]byte(collection),
			[]byte(i.Name),
		},
	}
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

func (p IndexPathPrefix) Append(field string, value any) IndexPathPrefix {
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

func (p IndexPathPrefix) SetDocumentID(id string) IndexPathPrefix {
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
