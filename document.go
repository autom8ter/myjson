package wolverine

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nqd/flat"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Document is a database document with special attributes.
// required attributes: _id(string), _collection(string)
type Document struct {
	result *gjson.Result
}

func NewDocument() *Document {
	parsed := gjson.Parse("{}")
	return &Document{
		result: &parsed,
	}
}

func NewDocumentFromBytes(json []byte) (*Document, error) {
	if !gjson.ValidBytes(json) {
		return nil, fmt.Errorf("invalid json: %s", string(json))
	}
	return &Document{
		result: lo.ToPtr(gjson.ParseBytes(json)),
	}, nil
}

func NewDocumentFromMap(value map[string]interface{}) (*Document, error) {
	value, err := flat.Unflatten(value, nil)
	if err != nil {
		return nil, err
	}
	return NewDocumentFromAny(value)
}

func NewDocumentFromAny(value any) (*Document, error) {
	bits, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to json encode value: %#v", value)
	}
	d := &Document{
		result: lo.ToPtr(gjson.ParseBytes(bits)),
	}
	return d, nil
}

func (d Document) Empty() bool {
	return d.result == nil || d.result.Raw == ""
}

// String returns the document as a json string
func (r Document) String() string {
	return r.result.Raw
}

// Bytes returns the document as json bytes
func (r Document) Bytes() []byte {
	return []byte(r.result.Raw)
}

// Value returns the document
func (d Document) Value() map[string]any {
	return d.result.Value().(map[string]interface{})
}

// Clone allocates a new document with identical values
func (r Document) Clone() Document {
	raw := r.result.Raw
	return Document{result: lo.ToPtr(gjson.Parse(raw))}
}

// Select returns the document with only the selected fields populated
func (r Document) Select(fields []string) Document {
	if len(fields) == 0 || fields[0] == "*" {
		return r
	}
	patch := map[string]interface{}{}
	for _, f := range fields {
		patch[f] = r.Get(f)
	}
	unflat, _ := flat.Unflatten(patch, nil)
	doc, _ := NewDocumentFromMap(unflat)
	return *doc
}

// Validate returns an error if the documents collection, id, or fields are empty
func (r Document) Validate() error {
	if r.GetCollection() == "" {
		return errors.New("document validation: empty _collection")
	}
	if r.GetID() == "" {
		return errors.New("document validation: empty _id")
	}
	return nil
}

// GetCollection gets the collection from the document
func (r Document) GetCollection() string {
	return r.result.Get("_collection").String()
}

// GetID gets the id from the document
func (r Document) GetID() string {
	return r.result.Get("_id").String()
}

// Get gets a field on the document. Get has GJSON syntax support and supports dot notation
func (r Document) Get(field string) any {
	return r.result.Get(field).Value()
}

// GetString gets a string field value on the document. Get has GJSON syntax support and supports dot notation
func (r Document) GetString(field string) string {
	return r.result.Get(field).String()
}

// GetBool gets a bool field value on the document. GetBool has GJSON syntax support and supports dot notation
func (r Document) GetBool(field string) bool {
	return r.result.Get(field).Bool()
}

// GetFloat gets a bool field value on the document. GetFloat has GJSON syntax support and supports dot notation
func (r Document) GetFloat(field string) float64 {
	return r.result.Get(field).Float()
}

// Set sets a field on the document. Dot notation is supported.
func (r *Document) Set(field string, val any) {
	switch val := val.(type) {
	case gjson.Result:
		result, _ := sjson.Set(r.result.Raw, field, val.Value())
		r.result = lo.ToPtr(gjson.Parse(result))
	default:
		result, _ := sjson.Set(r.result.Raw, field, val)
		r.result = lo.ToPtr(gjson.Parse(result))
	}
}

// SetAll sets all fields on the document. Dot notation is supported.
func (r *Document) SetAll(values map[string]any) {
	for k, val := range values {
		r.Set(k, val)
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
func (r *Document) Del(field string) {
	result, err := sjson.Delete(r.result.Raw, field)
	if err != nil {
		panic(err)
	}
	r.result = lo.ToPtr(gjson.Parse(result))
}

// SetCollection sets the collection on the document
func (r *Document) SetCollection(collection string) {
	r.Set("_collection", collection)
}

// SetID sets the id on the document
func (r *Document) SetID(id string) {
	r.Set("_id", id)
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
		case "in":

		default:
			return false, fmt.Errorf("invalid operator: %s", w.Op)
		}
	}
	return true, nil
}

// ScanJSON scans the json document into the value
func (d *Document) ScanJSON(value any) error {
	return json.Unmarshal([]byte(d.String()), &value)
}
