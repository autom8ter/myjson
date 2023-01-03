package myjson

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/util"
	"github.com/huandu/xstrings"
	flat2 "github.com/nqd/flat"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func init() {
	for k, v := range modifiers {
		gjson.AddModifier(k, v)
	}
}

var modifiers = map[string]func(json, arg string) string{
	"snakeCase": func(json, arg string) string {
		return xstrings.ToSnakeCase(json)
	},
	"camelCase": func(json, arg string) string {
		return xstrings.ToCamelCase(json)
	},
	"kebabCase": func(json, arg string) string {
		return xstrings.ToKebabCase(json)
	},
	"upper": func(json, arg string) string {
		return strings.ToUpper(json)
	},
	"lower": func(json, arg string) string {
		return strings.ToLower(json)
	},
	"replaceAll": func(json, arg string) string {
		args := gjson.Parse(arg)
		return strings.ReplaceAll(json, args.Get("old").String(), args.Get("new").String())
	},
	"trim": func(json, arg string) string {
		return strings.ReplaceAll(json, " ", "")
	},
	"dateTrunc": func(json, arg string) string {
		json, _ = strconv.Unquote(json)
		t := cast.ToTime(json)
		yr, month, day := t.Date()
		switch arg {
		case "month":
			return strconv.Quote(time.Date(yr, month, 1, 0, 0, 0, 0, time.UTC).String())
		case "day":
			return strconv.Quote(time.Date(yr, month, day, 0, 0, 0, 0, time.UTC).String())
		case "year":
			return strconv.Quote(time.Date(yr, time.January, 1, 0, 0, 0, 0, time.UTC).String())
		default:
			return json
		}
	},
	"unix": func(json, arg string) string {
		json, _ = strconv.Unquote(json)
		t := cast.ToTime(json)
		if t.IsZero() {
			return json
		}
		return strconv.Quote(fmt.Sprint(t.Unix()))
	},
	"unixMilli": func(json, arg string) string {
		json, _ = strconv.Unquote(json)
		t := cast.ToTime(json)
		if t.IsZero() {
			return json
		}
		return strconv.Quote(fmt.Sprint(t.UnixMilli()))
	},
	"unixNano": func(json, arg string) string {
		json, _ = strconv.Unquote(json)
		t := cast.ToTime(json)
		if t.IsZero() {
			return json
		}
		return strconv.Quote(fmt.Sprint(t.UnixNano()))
	},
}

const selfRefPrefix = "$"

// Document is a concurrency safe JSON document
type Document struct {
	result gjson.Result
	mu     sync.RWMutex
}

// UnmarshalJSON satisfies the json Unmarshaler interface
func (d *Document) UnmarshalJSON(bytes []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	doc, err := NewDocumentFromBytes(bytes)
	if err != nil {
		return err
	}
	d.result = doc.result
	return nil
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
		mu:     sync.RWMutex{},
	}
}

// NewDocumentFromBytes creates a new document from the given json bytes
func NewDocumentFromBytes(json []byte) (*Document, error) {
	if !gjson.ValidBytes(json) {
		return nil, errors.New(errors.Validation, "invalid json: %s", string(json))
	}
	d := &Document{
		result: gjson.ParseBytes(json),
		mu:     sync.RWMutex{},
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
	d.mu.RLock()
	defer d.mu.RUnlock()
	return gjson.ValidBytes(d.Bytes()) && !d.result.IsArray()
}

// String returns the document as a json string
func (d *Document) String() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.result.String()
}

// Bytes returns the document as json bytes
func (d *Document) Bytes() []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
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

// Get gets a field on the document. Get has GJSON syntax support and supports dot notation
func (d *Document) Get(field string) any {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.result.Get(field).Exists() {
		return d.result.Get(field).Value()
	}
	return nil
}

// GetString gets a string field value on the document. Get has GJSON syntax support and supports dot notation
func (d *Document) GetString(field string) string {
	return cast.ToString(d.Get(field))
}

// Exists returns true if the fieldPath has a value in the json document
func (d *Document) Exists(field string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.result.Get(field).Exists()
}

// GetBool gets a bool field value on the document. GetBool has GJSON syntax support and supports dot notation
func (d *Document) GetBool(field string) bool {
	return cast.ToBool(d.Get(field))
}

// GetFloat gets a float64 field value on the document. GetFloat has GJSON syntax support and supports dot notation
func (d *Document) GetFloat(field string) float64 {
	return cast.ToFloat64(d.Get(field))
}

// GetTime gets a time.Time field value on the document. GetTime has GJSON syntax support and supports dot notation
func (d *Document) GetTime(field string) time.Time {
	return cast.ToTime(d.GetString(field))
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

var setOpts = &sjson.Options{
	Optimistic:     true,
	ReplaceInPlace: true,
}

func (d *Document) set(field string, val any) error {
	var (
		result []byte
		err    error
	)
	switch val := val.(type) {
	case gjson.Result:
		result, err = sjson.SetBytesOptions([]byte(d.result.Raw), field, val.Value(), setOpts)
	case []byte:
		result, err = sjson.SetRawBytesOptions([]byte(d.result.Raw), field, val, setOpts)
	default:
		result, err = sjson.SetBytesOptions([]byte(d.result.Raw), field, val, setOpts)
	}
	if err != nil {
		return err
	}
	if d.result.Raw != string(result) {
		d.result = gjson.ParseBytes(result)
	}
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
			return err
		}
	}
	return nil
}

// Overwrite resets the document with the given values. Dot notation is supported.
func (d *Document) Overwrite(values map[string]any) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	nd := NewDocument()
	for k, v := range values {
		if err := nd.Set(k, v); err != nil {
			return err
		}
	}
	d.result = nd.result
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

func (d *Document) MergeJoin(with *Document, alias string) error {
	if !with.Valid() {
		return errors.New(errors.Validation, "invalid document")
	}
	withMap := with.Value()
	flattened, err := flat2.Flatten(withMap, nil)
	if err != nil {
		return err
	}
	for k, v := range flattened {
		if err := d.Set(fmt.Sprintf("%s.%s", alias, k), v); err != nil {
			return err
		}
	}
	return nil
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
			return err
		}
		d.result = gjson.Parse(result)
	}
	return nil
}

// Where executes the where clauses against the document and returns true if it passes the clauses.
// If the value of a where clause is prefixed with $. it will compare where.field to the same document's $.{field}.
func (d *Document) Where(wheres []Where) (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, w := range wheres {
		var (
			isSelf    = strings.HasPrefix(cast.ToString(w.Value), selfRefPrefix)
			selfField = strings.TrimPrefix(cast.ToString(w.Value), selfRefPrefix)
		)

		switch w.Op {
		case WhereOpEq:
			if isSelf {
				if d.Get(w.Field) != d.Get(selfField) || w.Value == "null" && d.Get(selfField) != nil {
					return false, nil
				}
			} else {
				if w.Value != d.Get(w.Field) || w.Value == "null" && d.Get(w.Field) != nil {
					return false, nil
				}
			}
		case WhereOpNeq:
			if isSelf {
				if d.Get(w.Field) == d.Get(selfField) || w.Value == "null" && d.Get(selfField) == nil {
					return false, nil
				}
			} else {
				if w.Value == d.Get(w.Field) || w.Value == "null" && d.Get(w.Field) == nil {
					return false, nil
				}
			}
		case WhereOpLt:
			if isSelf {
				if d.GetFloat(w.Field) >= d.GetFloat(selfField) {
					return false, nil
				}
			} else {
				if d.GetFloat(w.Field) >= cast.ToFloat64(w.Value) {
					return false, nil
				}
			}
		case WhereOpLte:
			if isSelf {
				if d.GetFloat(w.Field) > d.GetFloat(selfField) {
					return false, nil
				}
			} else {
				if d.GetFloat(w.Field) > cast.ToFloat64(w.Value) {
					return false, nil
				}
			}
		case WhereOpGt:
			if isSelf {
				if d.GetFloat(w.Field) <= d.GetFloat(selfField) {
					return false, nil
				}
			} else {
				if d.GetFloat(w.Field) <= cast.ToFloat64(w.Value) {
					return false, nil
				}
			}
		case WhereOpGte:
			if isSelf {
				if d.GetFloat(w.Field) < d.GetFloat(selfField) {
					return false, nil
				}
			} else {
				if d.GetFloat(w.Field) < cast.ToFloat64(w.Value) {
					return false, nil
				}
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
		case WhereOpHasPrefix:
			fieldVal := d.GetString(w.Field)
			if !strings.HasPrefix(fieldVal, cast.ToString(w.Value)) {
				return false, nil
			}
		case WhereOpHasSuffix:
			fieldVal := d.GetString(w.Field)
			if !strings.HasSuffix(fieldVal, cast.ToString(w.Value)) {
				return false, nil
			}
		case WhereOpRegex:
			fieldVal := d.Get(w.Field)
			match, _ := regexp.Match(cast.ToString(w.Value), []byte(cast.ToString(fieldVal)))
			if !match {
				return false, nil
			}
		default:
			return false, errors.New(errors.Validation, "unsupported operator: %s", w.Op)
		}
	}
	return true, nil
}

// Diff calculates a json diff between the document and the input document
func (d *Document) Diff(before *Document) []JSONFieldOp {
	var ops []JSONFieldOp
	if before == nil {
		before = NewDocument()
	}
	var (
		beforePaths = before.FieldPaths()
		afterPaths  = d.FieldPaths()
	)

	for _, path := range beforePaths {
		exists := d.result.Get(path).Exists()
		switch {
		case !exists:
			ops = append(ops, JSONFieldOp{
				Path:        path,
				Op:          JSONOpRemove,
				Value:       nil,
				BeforeValue: before.Get(path),
			})
		case exists && !reflect.DeepEqual(d.Get(path), before.Get(path)):
			ops = append(ops, JSONFieldOp{
				Path:        path,
				Op:          JSONOpReplace,
				Value:       d.Get(path),
				BeforeValue: before.Get(path),
			})
		}
	}
	for _, path := range afterPaths {
		exists := before.result.Get(path).Exists()
		switch {
		case !exists:
			ops = append(ops, JSONFieldOp{
				Path:        path,
				Op:          JSONOpAdd,
				Value:       d.Get(path),
				BeforeValue: nil,
			})
		}
	}
	return ops
}

// FieldPaths returns the paths to fields & nested fields in dot notation format
func (d *Document) FieldPaths() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	paths := &[]string{}
	d.paths(d.result, paths)
	return *paths
}

func (d *Document) paths(result gjson.Result, pathValues *[]string) {
	result.ForEach(func(key, value gjson.Result) bool {
		if value.IsObject() {
			d.paths(value, pathValues)
		} else {
			*pathValues = append(*pathValues, value.Path(d.result.Raw))
		}
		return true
	})
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
