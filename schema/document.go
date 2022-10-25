package schema

import (
	"encoding/json"
	"github.com/autom8ter/wolverine/internal/util"
	"github.com/nqd/flat"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"io"
	"strings"
)

// Document is a database document with special attributes.
// required attributes: _id(string), _collection(string)
type Document struct {
	result gjson.Result
}

// MarshalJSON satisfies the json Marshaler interface
func (d Document) MarshalJSON() ([]byte, error) {
	return d.Bytes(), nil
}

// NewDocument creates a new json document
func NewDocument() Document {
	parsed := gjson.Parse("{}")
	return Document{
		result: parsed,
	}
}

// NewDocumentFromBytes creates a new document from the given json bytes
func NewDocumentFromBytes(json []byte) (Document, error) {
	if !gjson.ValidBytes(json) {
		return Document{}, stacktrace.NewError("invalid json")
	}
	d := Document{
		result: gjson.ParseBytes(json),
	}
	if !d.Valid() {
		return Document{}, stacktrace.NewError("invalid document")
	}
	return d, nil
}

// NewDocumentFrom creates a new document from the given value - the value must be json compatible
func NewDocumentFrom(value any) (Document, error) {
	var err error
	switch value.(type) {
	case map[string]any:
		value, err = flat.Unflatten(value.(map[string]any), nil)
		if err != nil {
			return Document{}, stacktrace.NewError("failed to unflatten map: %#v", value)
		}
	}
	bits, err := json.Marshal(value)
	if err != nil {
		return Document{}, stacktrace.NewError("failed to json encode value: %#v", value)
	}
	return NewDocumentFromBytes(bits)
}

// Valid returns whether the document is valid
func (d Document) Valid() bool {
	return gjson.ValidBytes(d.Bytes()) && !d.result.IsArray()
}

// String returns the document as a json string
func (d Document) String() string {
	return d.result.Raw
}

// Bytes returns the document as json bytes
func (d Document) Bytes() []byte {
	return []byte(d.result.Raw)
}

// Value returns the document as a map
func (d Document) Value() map[string]any {
	return cast.ToStringMap(d.result.Value())
}

// Clone allocates a new document with identical values
func (d Document) Clone() Document {
	raw := d.result.Raw
	return Document{result: gjson.Parse(raw)}
}

// Select returns the document with only the selected fields populated
func (d Document) Select(fields []string) Document {
	if len(fields) == 0 || fields[0] == "*" {
		return d
	}
	patch := map[string]interface{}{}
	for _, f := range fields {
		patch[f] = d.Get(f)
	}
	unflat, _ := flat.Unflatten(patch, nil)
	doc, _ := NewDocumentFrom(unflat)
	return doc
}

// Get gets a field on the document. Get has GJSON syntax support and supports dot notation
func (d Document) Get(field string) any {
	return d.result.Get(field).Value()
}

// GetString gets a string field value on the document. Get has GJSON syntax support and supports dot notation
func (d Document) GetString(field string) string {
	return cast.ToString(d.result.Get(field).Value())
}

// GetBool gets a bool field value on the document. GetBool has GJSON syntax support and supports dot notation
func (d Document) GetBool(field string) bool {
	return d.result.Get(field).Bool()
}

// GetFloat gets a bool field value on the document. GetFloat has GJSON syntax support and supports dot notation
func (d Document) GetFloat(field string) float64 {
	return d.result.Get(field).Float()
}

// GetArray gets an array field on the document. Get has GJSON syntax support and supports dot notation
func (d Document) GetArray(field string) []any {
	return cast.ToSlice(d.result.Get(field).Value())
}

// Set sets a field on the document. Dot notation is supported.
func (d Document) Set(field string, val any) (Document, error) {
	var (
		result string
		err    error
	)

	switch val := val.(type) {
	case gjson.Result:
		result, err = sjson.Set(d.result.Raw, field, val.Value())
	case []byte:
		result, err = sjson.SetRaw(d.result.Raw, field, string(val))
	default:
		result, err = sjson.Set(d.result.Raw, field, val)
	}
	if err != nil {
		return Document{}, stacktrace.Propagate(err, "")
	}
	doc := Document{result: gjson.Parse(result)}
	if !doc.Valid() {
		return Document{}, stacktrace.NewError("invalid document")
	}
	return doc, nil
}

// SetAll sets all fields on the document. Dot notation is supported.
func (d Document) SetAll(values map[string]any) (Document, error) {
	flattened, err := flat.Flatten(values, nil)
	if err != nil {
		return Document{}, stacktrace.Propagate(err, "")
	}
	dflat, err := flat.Flatten(d.Value(), nil)
	if err != nil {
		return Document{}, stacktrace.Propagate(err, "")
	}
	for k, val := range flattened {
		dflat[k] = val
	}
	doc, err := NewDocumentFrom(dflat)
	if err != nil {
		return Document{}, stacktrace.Propagate(err, "")
	}
	return doc, nil
}

// Merge merges the doument with the provided document. This is not an overwrite.
func (d Document) Merge(with Document) (Document, error) {
	if !with.Valid() {
		return d, nil
	}
	withMap := with.Value()
	withFlat, err := flat.Flatten(withMap, nil)
	if err != nil {
		panic(err)
	}
	return d.SetAll(withFlat)
}

// Del deletes a field from the document
func (d Document) Del(field string) Document {
	result, err := sjson.Delete(d.result.Raw, field)
	if err != nil {
		panic(err)
	}
	return Document{result: gjson.Parse(result)}
}

// Where executes the where clauses against the document and returns true if it passes the clauses
func (d Document) Where(wheres []Where) (bool, error) {
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
		case In:
			bits, _ := json.Marshal(w.Value)
			arr := gjson.ParseBytes(bits).Array()
			value := d.Get(w.Field)
			match := false
			for _, element := range arr {
				if element.Value() == value {
					match = true
				}
			}
			if !match {
				return false, nil
			}

		case Contains:
			if !strings.Contains(d.GetString(w.Field), cast.ToString(w.Value)) {
				return false, nil
			}
		default:
			return false, stacktrace.NewError("invalid operator: %s", w.Op)
		}
	}
	return true, nil
}

// Scan scans the json document into the value
func (d Document) Scan(value any) error {
	return util.Decode(d.Value(), &value)
}

// Encode encodes the json document to the io writer
func (d Document) Encode(w io.Writer) error {
	_, err := w.Write(d.Bytes())
	if err != nil {
		return stacktrace.Propagate(err, "failed to encode document")
	}
	return nil
}
