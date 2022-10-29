package wolverine

import (
	"encoding/json"
	flat2 "github.com/nqd/flat"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"io"
	"sort"
	"strings"
	"sync"
)

// Ref is a reference to a document
type Ref struct {
	Collection string `json:"collection"`
	ID         string `json:"id"`
}

// Document is a database document with special attributes.
// required attributes: _id(string), _collection(string)
type Document struct {
	result gjson.Result
	mu     sync.RWMutex
}

// MarshalJSON satisfies the json Marshaler interface
func (d *Document) MarshalJSON() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
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
		return nil, stacktrace.NewError("invalid json")
	}
	d := &Document{
		result: gjson.ParseBytes(json),
	}
	if !d.Valid() {
		return nil, stacktrace.NewError("invalid document")
	}
	return d, nil
}

// NewDocumentFrom creates a new document from the given value - the value must be json compatible
func NewDocumentFrom(value any) (*Document, error) {
	var err error
	bits, err := json.Marshal(value)
	if err != nil {
		return nil, stacktrace.NewError("failed to json encode value: %#v", value)
	}
	return NewDocumentFromBytes(bits)
}

// Valid returns whether the document is valid
func (d *Document) Valid() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return gjson.ValidBytes(d.Bytes()) && !d.result.IsArray()
}

// String returns the document as a json string
func (d *Document) String() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.result.Raw
}

// Bytes returns the document as json bytes
func (d *Document) Bytes() []byte {
	return []byte(d.result.Raw)
}

// Value returns the document as a map
func (d *Document) Value() map[string]any {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return cast.ToStringMap(d.result.Value())
}

// Clone allocates a new document with identical values
func (d *Document) Clone() *Document {
	d.mu.RLock()
	defer d.mu.RUnlock()
	raw := d.result.Raw
	return &Document{result: gjson.Parse(raw)}
}

// Select returns the document with only the selected fields populated
func (d *Document) Select(fields []string) error {
	if len(fields) == 0 || fields[0] == "*" {
		return nil
	}
	var (
		selected = NewDocument()
	)

	patch := map[string]interface{}{}
	for _, f := range fields {
		patch[f] = d.Get(f)
	}
	err := selected.SetAll(patch)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	d.result = selected.result
	return nil
}

// Get gets a field on the document. Get has GJSON syntax support and supports dot notation
func (d *Document) Get(field string) any {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.result.Get(field).Value()
}

// GetString gets a string field value on the document. Get has GJSON syntax support and supports dot notation
func (d *Document) GetString(field string) string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return cast.ToString(d.result.Get(field).Value())
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
		return stacktrace.Propagate(err, "")
	}
	if !gjson.Valid(result) {
		return stacktrace.NewError("invalid document")
	}
	d.result = gjson.Parse(result)
	return nil
}

// SetAll sets all fields on the document. Dot notation is supported.
func (d *Document) SetAll(values map[string]any) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var err error
	for k, v := range values {
		err = d.set(k, v)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	return nil
}

// Merge merges the doument with the provided document. This is not an overwrite.
func (d *Document) Merge(with *Document) error {
	if !with.Valid() {
		return stacktrace.NewError("invalid document")
	}
	withMap := with.Value()
	flattened, err := flat2.Flatten(withMap, nil)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	return d.SetAll(flattened)
}

// Del deletes a field from the document
func (d *Document) Del(field string) error {
	return d.DelAll(field)
}

// Del deletes a field from the document
func (d *Document) DelAll(fields ...string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, field := range fields {
		result, err := sjson.Delete(d.result.Raw, field)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		d.result = gjson.Parse(result)
	}
	return nil
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
func (d *Document) Scan(value any) error {
	return Decode(d.Value(), &value)
}

// Encode encodes the json document to the io writer
func (d *Document) Encode(w io.Writer) error {
	_, err := w.Write(d.Bytes())
	if err != nil {
		return stacktrace.Propagate(err, "failed to encode document")
	}
	return nil
}

type Documents []*Document

func (documents Documents) GroupBy(fields []string) map[string][]*Document {
	return lo.GroupBy[*Document](documents, func(d *Document) string {
		var values []string
		for _, g := range fields {
			values = append(values, cast.ToString(d.Get(g)))
		}
		return strings.Join(values, ".")
	})
}

func (documents Documents) Slice(start, end int) Documents {
	return lo.Slice[*Document](documents, start, end)
}

func (documents Documents) Filter(predicate func(document *Document, i int) bool) Documents {
	return lo.Filter[*Document](documents, predicate)
}

func (documents Documents) Map(mapper func(t *Document, i int) *Document) Documents {
	return lo.Map[*Document, *Document](documents, mapper)
}

func (documents Documents) Reduce(reducer func(accumulated, next *Document, i int) *Document) *Document {
	return lo.Reduce[*Document](documents, reducer, NewDocument())
}

func (d Documents) OrderBy(orderBy OrderBy) Documents {
	if orderBy.Field == "" {
		return d
	}
	if orderBy.Direction == DESC {
		sort.Slice(d, func(i, j int) bool {
			return compareField(orderBy.Field, d[i], d[j])
		})
	} else {
		sort.Slice(d, func(i, j int) bool {
			return !compareField(orderBy.Field, d[i], d[j])
		})
	}
	return d
}