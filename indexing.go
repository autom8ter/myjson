package gokvkit

import (
	"bytes"
	"context"

	"github.com/autom8ter/gokvkit/util"
	"github.com/nqd/flat"
)

var nullByte = []byte("\x00")

// indexFieldValue is a key value pair
type indexFieldValue struct {
	Field string `json:"field"`
	Value any    `json:"value"`
}

func seekPrefix(ctx context.Context, collection string, i Index, fields map[string]any) indexPathPrefix {
	md, _ := GetMetadata(ctx)
	fields, _ = flat.Flatten(fields, nil)
	var prefix = indexPathPrefix{
		prefix: [][]byte{
			[]byte(md.GetNamespace()),
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
	prefix    [][]byte
	seekValue any
	fields    [][]byte
	fieldMap  []indexFieldValue
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

func (p indexPathPrefix) Seek(value any) indexPathPrefix {
	return indexPathPrefix{
		prefix:    p.prefix,
		seekValue: value,
		fields:    p.fields,
		fieldMap:  p.fieldMap,
	}
}

func (p indexPathPrefix) Path() []byte {
	var path = append(p.prefix, p.fields...)
	if p.seekValue != nil {
		path = append(path, util.EncodeIndexValue(p.seekValue))
	}
	return bytes.Join(path, nullByte)
}

func (i indexPathPrefix) SeekValue() any {
	return i.seekValue
}

func (i indexPathPrefix) Fields() []indexFieldValue {
	return i.fieldMap
}

func indexPrefix(ctx context.Context, collection, index string) []byte {
	md, _ := GetMetadata(ctx)
	path := [][]byte{
		[]byte(md.GetNamespace()),
		[]byte("index"),
		[]byte(collection),
		[]byte(index),
	}
	return bytes.Join(path, nullByte)
}

func collectionPrefix(ctx context.Context, collection string) []byte {
	md, _ := GetMetadata(ctx)
	path := [][]byte{
		[]byte(md.GetNamespace()),
		[]byte("index"),
		[]byte(collection),
	}
	return bytes.Join(path, nullByte)
}
