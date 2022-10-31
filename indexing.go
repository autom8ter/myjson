package wolverine

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

// IndexMatch is an index matched to a read request
type IndexMatch struct {
	// Ref is the matching index
	Ref Index `json:"ref"`
	// MatchedFields is the fields that match the index
	MatchedFields []string `json:"matchedFields"`
	// IsOrdered returns true if the index delivers results in the order of the query.
	// If the index order does not match the query order, a full table scan is necessary to retrieve the result set.
	IsOrdered      bool `json:"isOrdered"`
	IsPrimaryIndex bool `json:"isPrimaryIndex"`
	// Values are the original values used to target the index
	Values map[string]any `json:"values"`
}

// Prefix is a reference to a prefix within an index
type Prefix interface {
	Append(field string, value any) Prefix
	Path() []byte
	NextPrefix() []byte
	Fields() []FieldValue
	DocumentID() string
	SetDocumentID(id string) Prefix
}

// FieldValue is a key value pair
type FieldValue struct {
	Field string `json:"field"`
	Value any    `json:"value"`
}

func (i Index) Seek(fields map[string]any) Prefix {
	fields, _ = flat.Flatten(fields, nil)
	var prefix = indexPathPrefix{
		prefix: [][]byte{
			[]byte("index"),
			[]byte(i.Collection),
			[]byte(i.Name),
		},
	}
	if i.Fields == nil {
		return prefix
	}
	for _, k := range i.Fields {
		if v, ok := fields[k]; ok {
			prefix.Append(k, v)
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
	p.fields = append(p.fields, []byte(field), encodeIndexValue(value))
	p.fieldMap = append(p.fieldMap, FieldValue{
		Field: field,
		Value: value,
	})
	return p
}

func (p indexPathPrefix) SetDocumentID(id string) Prefix {
	p.documentID = id
	return p
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

// NextPrefix returns the next prefix
func (p indexPathPrefix) NextPrefix() []byte {
	k := p.Path()
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
