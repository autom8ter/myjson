package wolverine

import (
	"encoding/json"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/nqd/flat"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type Stats struct {
	ExecutionTime time.Duration `json:"execution_time"`
}

// Page is a page of documents
type Page struct {
	Documents []*Document `json:"documents"`
	NextPage  int         `json:"next_page"`
	Count     int         `json:"count"`
	Stats     Stats       `json:"stats"`
}

// Document is a database document with special attributes.
// required attributes: _id(string), _collection(string)
type Document struct {
	result *gjson.Result
}

// UnmarshalJSON satisfies the json Unmarshaler interface
func (d *Document) UnmarshalJSON(bytes []byte) error {
	parsed := gjson.ParseBytes(bytes)
	d.result = &parsed
	return d.Validate()
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

func NewDocumentFromBytes(json []byte) (*Document, error) {
	if !gjson.ValidBytes(json) {
		return nil, stacktrace.NewError("invalid json")
	}
	return &Document{
		result: lo.ToPtr(gjson.ParseBytes(json)),
	}, nil
}

func NewDocumentFromMap(value map[string]interface{}) (*Document, error) {
	value, err := flat.Unflatten(value, nil)
	if err != nil {
		return nil, stacktrace.Propagate(err, "failed to flatten map")
	}
	return NewDocumentFromAny(value)
}

func NewDocumentFromAny(value any) (*Document, error) {
	bits, err := json.Marshal(value)
	if err != nil {
		return nil, stacktrace.NewError("failed to json encode value: %#v", value)
	}
	d := &Document{
		result: lo.ToPtr(gjson.ParseBytes(bits)),
	}
	return d, nil
}

// Empty returns whether the document is empty
func (d *Document) Empty() bool {
	return d.result == nil || d.result.Raw == ""
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
	doc, _ := NewDocumentFromMap(unflat)
	*d = *doc
	return doc
}

// Validate returns an error if the documents collection, id, or fields are empty
func (d *Document) Validate() error {
	if d.GetID() == "" {
		return stacktrace.NewError("document validation: empty _id")
	}
	return nil
}

// GetID gets the id from the document
func (d *Document) GetID() string {
	return d.result.Get("_id").String()
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

// SetID sets the id on the document
func (d *Document) SetID(id string) {
	d.Set("_id", id)
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

// ScanJSON scans the json document into the value
func (d *Document) ScanJSON(value any) error {
	switch value.(type) {
	case proto.Message:
		return stacktrace.Propagate(json.Unmarshal([]byte(d.String()), &value), "failed to scan document")
	default:
		return stacktrace.Propagate(json.Unmarshal([]byte(d.String()), &value), "failed to scan document")
	}
}

// Encode encodes the json document to the io writer
func (d *Document) Encode(w io.Writer) error {
	_, err := w.Write(d.Bytes())
	if err != nil {
		return stacktrace.Propagate(err, "failed to encode document")
	}
	return nil
}
