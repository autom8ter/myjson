package gokvkit

import (
	"encoding/json"
	"io"
	"strings"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/internal/util"
	flat2 "github.com/nqd/flat"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Document is a concurrency safe JSON document
type Document struct {
	result gjson.Result
}

// UnmarshalJSON satisfies the json Unmarshaler interface
func (d *Document) UnmarshalJSON(bytes []byte) error {
	doc, err := NewDocumentFromBytes(bytes)
	if err != nil {
		return err
	}
	*d = *doc
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
		result: parsed,
	}
}

// NewDocumentFromBytes creates a new document from the given json bytes
func NewDocumentFromBytes(json []byte) (*Document, error) {
	if !gjson.ValidBytes(json) {
		return nil, errors.New(errors.Validation, "invalid json: %s", string(json))
	}
	d := &Document{
		result: gjson.ParseBytes(json),
	}
	if !d.Valid() {
		return nil, errors.New(errors.Validation, "invalid document")
	}
	return d, nil
}

// NewDocumentFrom creates a new document from the given value - the value must be json compatible
func NewDocumentFrom(value any) (*Document, error) {
	var err error
	bits, err := json.Marshal(value)
	if err != nil {
		return nil, errors.New(errors.Validation, "failed to json encode value: %#v", value)
	}
	return NewDocumentFromBytes(bits)
}

// Valid returns whether the document is valid
func (d *Document) Valid() bool {
	return gjson.ValidBytes(d.Bytes()) && !d.result.IsArray()
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
	return cast.ToStringMap(d.result.Value())
}

// Clone allocates a new document with identical values
func (d *Document) Clone() *Document {
	raw := d.result.Raw
	return &Document{result: gjson.Parse(raw)}
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
	return cast.ToBool(d.Get(field))
}

// GetFloat gets a bool field value on the document. GetFloat has GJSON syntax support and supports dot notation
func (d *Document) GetFloat(field string) float64 {
	return cast.ToFloat64(d.Get(field))
}

// GetArray gets an array field on the document. Get has GJSON syntax support and supports dot notation
func (d *Document) GetArray(field string) []any {
	return cast.ToSlice(d.Get(field))
}

// Set sets a field on the document. Dot notation is supported.
func (d *Document) Set(field string, val any) error {
	return d.SetAll(map[string]any{
		field: val,
	})
}

func (d *Document) set(field string, val any) error {
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
		return err
	}
	if !gjson.Valid(result) {
		return errors.New(errors.Validation, "invalid document")
	}
	d.result = gjson.Parse(result)
	return nil
}

// SetAll sets all fields on the document. Dot notation is supported.
func (d *Document) SetAll(values map[string]any) error {
	var err error
	for k, v := range values {
		err = d.set(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// Merge merges the doument with the provided document. This is not an overwrite.
func (d *Document) Merge(with *Document) error {
	if !with.Valid() {
		return errors.New(errors.Validation, "invalid document")
	}
	withMap := with.Value()
	flattened, err := flat2.Flatten(withMap, nil)
	if err != nil {
		return err
	}
	return d.SetAll(flattened)
}

// Del deletes a field from the document
func (d *Document) Del(field string) error {
	return d.DelAll(field)
}

// Del deletes a field from the document
func (d *Document) DelAll(fields ...string) error {
	for _, field := range fields {
		result, err := sjson.Delete(d.result.Raw, field)
		if err != nil {
			return err
		}
		d.result = gjson.Parse(result)
	}
	return nil
}

// Where executes the where clauses against the document and returns true if it passes the clauses
func (d *Document) Where(wheres []Where) (bool, error) {
	var selfRefPrefix = "$."
	for _, w := range wheres {
		var (
			isSelf    = strings.HasPrefix(cast.ToString(w.Value), selfRefPrefix)
			selfField = strings.TrimPrefix(cast.ToString(w.Value), selfRefPrefix)
		)

		switch w.Op {
		case WhereOpEq:
			if isSelf && d.Get(w.Field) != d.Get(selfField) {
				return false, nil
			}
			if w.Value != d.Get(w.Field) {
				return false, nil
			}

		case WhereOpNeq:
			if isSelf && d.Get(w.Field) == d.Get(selfField) {
				return false, nil
			}
			if w.Value == d.Get(w.Field) {
				return false, nil
			}
		case WhereOpLt:
			if isSelf && d.GetFloat(w.Field) >= d.GetFloat(selfField) {
				return false, nil
			}
			if d.GetFloat(w.Field) >= cast.ToFloat64(w.Value) {
				return false, nil
			}
		case WhereOpLte:
			if isSelf && d.GetFloat(w.Field) > d.GetFloat(selfField) {
				return false, nil
			}
			if d.GetFloat(w.Field) > cast.ToFloat64(w.Value) {
				return false, nil
			}
		case WhereOpGt:
			if isSelf && d.GetFloat(w.Field) <= d.GetFloat(selfField) {
				return false, nil
			}
			if d.GetFloat(w.Field) <= cast.ToFloat64(w.Value) {
				return false, nil
			}
		case WhereOpGte:
			if isSelf && d.GetFloat(w.Field) < d.GetFloat(selfField) {
				return false, nil
			}
			if d.GetFloat(w.Field) < cast.ToFloat64(w.Value) {
				return false, nil
			}
		case WhereOpIn:
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

		case WhereOpContains:
			fieldVal := d.Get(w.Field)
			switch fieldVal := fieldVal.(type) {
			case []bool:
				if !lo.Contains(fieldVal, cast.ToBool(w.Value)) {
					return false, nil
				}
			case []float64:
				if !lo.Contains(fieldVal, cast.ToFloat64(w.Value)) {
					return false, nil
				}
			case []string:
				if !lo.Contains(fieldVal, cast.ToString(w.Value)) {
					return false, nil
				}
			case string:
				if !strings.Contains(fieldVal, cast.ToString(w.Value)) {
					return false, nil
				}
			default:
				if !strings.Contains(util.JSONString(fieldVal), util.JSONString(w.Value)) {
					return false, nil
				}
			}

		case WhereOpContainsAll:
			fieldVal := cast.ToStringSlice(d.Get(w.Field))
			for _, v := range cast.ToStringSlice(w.Value) {
				if !lo.Contains(fieldVal, v) {
					return false, nil
				}
			}
		case WhereOpContainsAny:
			fieldVal := cast.ToStringSlice(d.Get(w.Field))
			for _, v := range cast.ToStringSlice(w.Value) {
				if lo.Contains(fieldVal, v) {
					return true, nil
				}
			}
		default:
			return false, errors.New(errors.Validation, "invalid operator: '%s'", w.Op)
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
		return errors.Wrap(err, 0, "failed to encode document")
	}
	return nil
}

// Documents is an array of documents
type Documents []*Document

// Slice slices the documents into a subarray of documents
func (documents Documents) Slice(start, end int) Documents {
	return lo.Slice[*Document](documents, start, end)
}

// Filter applies the filter function against the documents
func (documents Documents) Filter(predicate func(document *Document, i int) bool) Documents {
	return lo.Filter[*Document](documents, predicate)
}

// Map applies the mapper function against the documents
func (documents Documents) Map(mapper func(t *Document, i int) *Document) Documents {
	return lo.Map[*Document, *Document](documents, mapper)
}

// ForEach applies the function to each document in the documents
func (documents Documents) ForEach(fn func(next *Document, i int)) {
	lo.ForEach[*Document](documents, fn)
}
