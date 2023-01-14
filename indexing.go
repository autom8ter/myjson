package myjson

import (
	"bytes"
	"context"

	"github.com/autom8ter/myjson/util"
	"github.com/nqd/flat"
	"github.com/spf13/cast"
)

var nullByte = []byte("\x00")

// indexFieldValue is a key value pair
type indexFieldValue struct {
	Field string `json:"field"`
	Value any    `json:"value"`
}

func seekPrefix(ctx context.Context, collection string, i Index, fields map[string]any) indexPathPrefix {
	fields, _ = flat.Flatten(fields, nil)
	var prefix = indexPathPrefix{
		prefix: [][]byte{
			[]byte(cast.ToString(GetMetadataValue(ctx, "namespace"))),
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

func (i indexPathPrefix) Append(field string, value any) indexPathPrefix {
	fields := append(i.fields, []byte(field), util.EncodeIndexValue(value))
	fieldMap := append(i.fieldMap, indexFieldValue{
		Field: field,
		Value: value,
	})
	return indexPathPrefix{
		prefix:   i.prefix,
		fields:   fields,
		fieldMap: fieldMap,
	}
}

func (i indexPathPrefix) Seek(value any) indexPathPrefix {
	return indexPathPrefix{
		prefix:    i.prefix,
		seekValue: value,
		fields:    i.fields,
		fieldMap:  i.fieldMap,
	}
}

func (i indexPathPrefix) Path() []byte {
	var path = append(i.prefix, i.fields...)
	if i.seekValue != nil {
		path = append(path, util.EncodeIndexValue(i.seekValue))
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
	path := [][]byte{
		[]byte(cast.ToString(GetMetadataValue(ctx, "namespace"))),
		[]byte("index"),
		[]byte(collection),
		[]byte(index),
	}
	return bytes.Join(path, nullByte)
}

func collectionPrefix(ctx context.Context, collection string) []byte {
	path := [][]byte{
		[]byte(cast.ToString(GetMetadataValue(ctx, "namespace"))),
		[]byte("index"),
		[]byte(collection),
	}
	return bytes.Join(path, nullByte)
}
