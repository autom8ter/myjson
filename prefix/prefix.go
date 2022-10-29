package prefix

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/nqd/flat"
	"github.com/spf13/cast"
)

type IndexProvider func(collection string, fields []string) Index

type Index interface {
	Collection() string
	Fields() []string
	GetPrefix(fields map[string]any) PrefixRef
}

type PrefixRef interface {
	Prefix() []byte
	Seek(id []byte) []byte
}

type PrefixIndexRef struct {
	collection    string
	initialPrefix []string
	fields        []string
}

func (d PrefixIndexRef) Collection() string {
	return d.collection
}

func (d PrefixIndexRef) Fields() []string {
	return d.fields
}

func NewPrefixedIndex(collection string, fields []string) *PrefixIndexRef {
	return &PrefixIndexRef{
		collection:    collection,
		initialPrefix: []string{"index", collection},
		fields:        fields,
	}
}

type indexRef struct {
	path [][]byte
}

func (i indexRef) Prefix() []byte {
	return bytes.Join(i.path, []byte("."))
}

func (i indexRef) Seek(id []byte) []byte {
	i.path = append(i.path, id)
	return bytes.Join(i.path, []byte("."))
}

// GetPrefix
func (d PrefixIndexRef) GetPrefix(fields map[string]any) PrefixRef {
	fields, _ = flat.Flatten(fields, nil)
	var path [][]byte
	for _, i := range d.initialPrefix {
		path = append(path, []byte(i))
	}
	for _, k := range d.fields {
		if v, ok := fields[k]; ok {
			path = append(path, encodeValue(k), encodeValue(v))
		} else {
			path = append(path, encodeValue(k), encodeValue(v))
		}
	}
	return indexRef{path: path}
}

func encodeValue(value any) []byte {
	if value == nil {
		return []byte("")
	}
	switch value := value.(type) {
	case bool:
		return encodeValue(cast.ToString(value))
	case string:
		return []byte(value)
	case int, int64, int32, float64, float32, uint64, uint32, uint16:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, cast.ToUint64(value))
		return buf
	default:
		bits, _ := json.Marshal(value)
		if len(bits) == 0 {
			bits = []byte(cast.ToString(value))
		}
		return bits
	}
}

func PrefixNextKey(k []byte) []byte {
	buf := make([]byte, len(k))
	copy(buf, k)
	var i int
	for i = len(k) - 1; i >= 0; i-- {
		buf[i]++
		if buf[i] != 0 {
			break
		}
	}
	if i == -1 {
		buf = make([]byte, 0)
	}
	return buf
}
