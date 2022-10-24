package schema

import (
	"encoding/json"
	"github.com/autom8ter/wolverine/internal/util"
	"github.com/nqd/flat"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"io"
)

// Document is a database document with special attributes.
// required attributes: _id(string), _collection(string)
type Document struct {
	result *gjson.Result
}

// UnmarshalJSON satisfies the json Unmarshaler interface
func (d *Document) UnmarshalJSON(bytes []byte) error {
	if !gjson.ValidBytes(bytes) {
		return stacktrace.NewError("invalid json")
	}
	parsed := gjson.ParseBytes(bytes)
	d.result = &parsed
	return nil
}

// MarshalJSON satisfies the json Marshaler interface
func (d *Document) MarshalJSON() ([]byte, error) {
	return d.Bytes(), nil
}

// NewDocument creates a new json document
func NewDocument() *Document {
	parsed := gjson.Parse("{}")
	return &Document{
		result: &parsed,
	}
}

// NewDocumentFromBytes creates a new document from the given json bytes
func NewDocumentFromBytes(json []byte) (*Document, error) {
	if !gjson.ValidBytes(json) {
		return nil, stacktrace.NewError("invalid json")
	}
	return &Document{
		result: lo.ToPtr(gjson.ParseBytes(json)),
	}, nil
}

// NewDocumentFrom creates a new document from the given value - the value must be json compatible
func NewDocumentFrom(value any) (*Document, error) {
	bits, err := json.Marshal(value)
	if err != nil {
		return nil, stacktrace.NewError("failed to json encode value: %#v", value)
	}
	return NewDocumentFromBytes(bits)
}

// Valid returns whether the document is valid
func (d *Document) Valid() bool {
	return gjson.ValidBytes(d.Bytes())
}

// String returns the document as a json string
func (d *Document) String() string {
	return d.result.Raw
}

// Bytes returns the document as json bytes
func (d *Document) Bytes() []byte {
	return []byte(d.result.Raw)
}

// Value returns the document as a map
func (d *Document) Value() map[string]any {
	return d.result.Value().(map[string]interface{})
}

// Clone allocates a new document with identical values
func (d *Document) Clone() *Document {
	raw := d.result.Raw
	return &Document{result: lo.ToPtr(gjson.Parse(raw))}
}

// Select returns the document with only the selected fields populated
func (d *Document) Select(fields []string) *Document {
	if len(fields) == 0 || fields[0] == "*" {
		return d
	}
	patch := map[string]interface{}{}
	for _, f := range fields {
		patch[f] = d.Get(f)
	}
	unflat, _ := flat.Unflatten(patch, nil)
	doc, _ := NewDocumentFrom(unflat)
	*d = *doc
	return doc
}

// Get gets a field on the document. Get has GJSON syntax support and supports dot notation
func (d *Document) Get(field string) any {
	return d.result.Get(field).Value()
}

// GetString gets a string field value on the document. Get has GJSON syntax support and supports dot notation
func (d *Document) GetString(field string) string {
	return d.result.Get(field).String()
}

// GetBool gets a bool field value on the document. GetBool has GJSON syntax support and supports dot notation
func (d *Document) GetBool(field string) bool {
	return d.result.Get(field).Bool()
}

// GetFloat gets a bool field value on the document. GetFloat has GJSON syntax support and supports dot notation
func (d *Document) GetFloat(field string) float64 {
	return d.result.Get(field).Float()
}

// Set sets a field on the document. Dot notation is supported.
func (d *Document) Set(field string, val any) {
	switch val := val.(type) {
	case gjson.Result:
		result, _ := sjson.Set(d.result.Raw, field, val.Value())
		d.result = lo.ToPtr(gjson.Parse(result))
	default:
		result, _ := sjson.Set(d.result.Raw, field, val)
		d.result = lo.ToPtr(gjson.Parse(result))
	}
}

// SetAll sets all fields on the document. Dot notation is supported.
func (d *Document) SetAll(values map[string]any) {
	for k, val := range values {
		d.Set(k, val)
	}
}

// Merge merges the doument with the provided document. This is not an overwrite.
func (d *Document) Merge(with *Document) {
	if with == nil {
		return
	}
	withMap := with.Value()
	withFlat, err := flat.Flatten(withMap, nil)
	if err != nil {
		panic(err)
	}
	for k, v := range withFlat {
		d.Set(k, v)
	}
}

// Del deletes a field from the document
func (d *Document) Del(field string) {
	result, err := sjson.Delete(d.result.Raw, field)
	if err != nil {
		panic(err)
	}
	d.result = lo.ToPtr(gjson.Parse(result))
}

// Where executes the where clauses against the document and returns true if it passes the clauses
func (d *Document) Where(wheres []Where) (bool, error) {
	for _, w := range wheres {
		switch w.Op {
		case "==", Eq:
			if w.Value != d.Get(w.Field) {
				return false, nil
			}
		case "!=", Neq:
			if w.Value == d.Get(w.Field) {
				return false, nil
			}
		case ">", Gt:
			if d.GetFloat(w.Field) <= cast.ToFloat64(w.Value) {
				return false, nil
			}
		case ">=", Gte:
			if d.GetFloat(w.Field) < cast.ToFloat64(w.Value) {
				return false, nil
			}
		case "<", Lt:
			if d.GetFloat(w.Field) >= cast.ToFloat64(w.Value) {
				return false, nil
			}
		case "<=", Lte:
			if d.GetFloat(w.Field) > cast.ToFloat64(w.Value) {
				return false, nil
			}
		default:
			return false, stacktrace.NewError("invalid operator: %s", w.Op)
		}
	}
	return true, nil
}

// Scan scans the json document into the value
func (d *Document) Scan(value any) error {
	return util.Decode(d.Value(), &value)
}

// Encode encodes the json document to the io writer
func (d *Document) Encode(w io.Writer) error {
	_, err := w.Write(d.Bytes())
	if err != nil {
		return stacktrace.Propagate(err, "failed to encode document")
	}
	return nil
}
