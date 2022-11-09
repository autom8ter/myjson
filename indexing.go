package brutus

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/nqd/flat"
	"github.com/spf13/cast"
)

// Index is a database index used to optimize queries against a collection
type Index struct {
	// Collection is the collection the index belongs to
	Collection string `json:"collection"`
	// Name is the indexes unique name in the collection
	Name string `json:"name"`
	// Fields to index - order matters
	Fields []string `json:"fields"`
	// Unique indicates that it's a unique index which will enforce uniqueness
	Unique bool `json:"unique"`
	// Unique indicates that it's a primary index
	Primary bool `json:"primary"`
}

// Prefix is a reference to a prefix within an index
type Prefix interface {
	// Append appends a field value to an index prefix
	Append(field string, value any) Prefix
	// Path returns the full path of the prefix
	Path() []byte
	// Fields returns the fields contained in the index prefix
	Fields() []FieldValue
	// DocumentID returns the document id set as the suffix of the prefix when Path() is called
	// This allows the index to seek to the position of an individual document
	DocumentID() string
	// SetDocumentID sets the document id as the suffix of the prefix when Path() is called
	// This allows the index to seek to the position of an individual document
	SetDocumentID(id string) Prefix
}

// FieldValue is a key value pair
type FieldValue struct {
	Field string `json:"field"`
	Value any    `json:"value"`
}

func (i Index) Seek(fields map[string]any) Prefix {
	fields, _ = flat.Flatten(fields, nil)
	var prefix = Prefix(indexPathPrefix{
		prefix: [][]byte{
			[]byte("index"),
			[]byte(i.Collection),
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

type indexPathPrefix struct {
	prefix     [][]byte
	documentID string
	fields     [][]byte
	fieldMap   []FieldValue
}

func (p indexPathPrefix) Append(field string, value any) Prefix {
	fields := append(p.fields, []byte(field), encodeIndexValue(value))
	fieldMap := append(p.fieldMap, FieldValue{
		Field: field,
		Value: value,
	})
	return indexPathPrefix{
		prefix:   p.prefix,
		fields:   fields,
		fieldMap: fieldMap,
	}
}

func (p indexPathPrefix) SetDocumentID(id string) Prefix {
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

func (i indexPathPrefix) Fields() []FieldValue {
	return i.fieldMap
}

func encodeIndexValue(value any) []byte {
	if value == nil {
		return []byte("")
	}
	switch value := value.(type) {
	case bool:
		return encodeIndexValue(cast.ToString(value))
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
