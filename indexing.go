package gokvkit

import (
	"bytes"

	"github.com/autom8ter/gokvkit/util"
	"github.com/nqd/flat"
)

// indexFieldValue is a key value pair
type indexFieldValue struct {
	Field string `json:"field"`
	Value any    `json:"value"`
}

func seekPrefix(collection string, i Index, fields map[string]any) indexPathPrefix {
	fields, _ = flat.Flatten(fields, nil)
	var prefix = indexPathPrefix{
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

type indexPathPrefix struct {
	prefix     [][]byte
	documentID string
	fields     [][]byte
	fieldMap   []indexFieldValue
}

func (p indexPathPrefix) Append(field string, value any) indexPathPrefix {
	fields := append(p.fields, []byte(field), util.EncodeIndexValue(value))
	fieldMap := append(p.fieldMap, indexFieldValue{
		Field: field,
		Value: value,
	})
	return indexPathPrefix{
		prefix:   p.prefix,
		fields:   fields,
		fieldMap: fieldMap,
	}
}

func (p indexPathPrefix) SetDocumentID(id string) indexPathPrefix {
	return indexPathPrefix{
		prefix:     p.prefix,
		documentID: id,
		fields:     p.fields,
		fieldMap:   p.fieldMap,
	}
}

func (p indexPathPrefix) Path() []byte {
	var path = append(p.prefix, p.fields...)
	if p.documentID != "" {
		path = append(path, []byte(p.documentID))
	}
	return bytes.Join(path, []byte("\x00"))
}

func (i indexPathPrefix) DocumentID() string {
	return i.documentID
}

func (i indexPathPrefix) Fields() []indexFieldValue {
	return i.fieldMap
}

func indexPrefix(collection, index string) []byte {
	path := [][]byte{
		[]byte("index"),
		[]byte(collection),
		[]byte(index),
	}
	return bytes.Join(path, []byte("\x00"))
}

func collectionPrefix(collection string) []byte {
	path := [][]byte{
		[]byte("index"),
		[]byte(collection),
	}
	return bytes.Join(path, []byte("\x00"))
}
