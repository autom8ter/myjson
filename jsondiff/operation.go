package jsondiff

import (
	"encoding/json"
	"strings"
	"unsafe"

	"github.com/tidwall/gjson"
)

// JSON Diff operation types.
// These are defined in RFC 6902 section 4.
// https://datatracker.ietf.org/doc/html/rfc6902#section-4
const (
	OperationAdd     = "add"
	OperationReplace = "replace"
	OperationRemove  = "remove"
	OperationMove    = "move"
	OperationCopy    = "copy"
	OperationTest    = "test"
)

const (
	fromFieldLen  = len(`,"from":""`)
	valueFieldLen = len(`,"value":`)
	opBaseLen     = len(`{"op":"","path":""}`)
)

// Diff represents a series of JSON Diff operations.
type Diff []Operation

// Operation represents a single RFC6902 JSON Diff operation.
type Operation struct {
	Type     string      `json:"op"`
	From     pointer     `json:"from,omitempty"`
	Path     pointer     `json:"path"`
	OldValue interface{} `json:"-"`
	Value    interface{} `json:"value,omitempty"`
}

// String implements the fmt.Stringer interface.
func (o Operation) String() string {
	b, err := json.Marshal(o)
	if err != nil {
		return "<invalid operation>"
	}
	return string(b)
}

type jsonNull struct{}

// MarshalJSON implements the json.Marshaler interface.
func (jn jsonNull) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

// MarshalJSON implements the json.Marshaler interface.
func (o Operation) MarshalJSON() ([]byte, error) {
	type op Operation

	if !o.marshalWithValue() {
		o.Value = nil
	} else {
		// Generic check that passes for nil and type nil interface values.
		if (*[2]uintptr)(unsafe.Pointer(&o.Value))[1] == 0 {
			o.Value = jsonNull{}
		}
	}
	if !o.hasFrom() {
		o.From = emptyPtr
	}
	return json.Marshal(op(o))
}

// jsonLength returns the length in bytes that the
// operation would occupy when marshaled as JSON.
func (o Operation) jsonLength(targetBytes []byte) int {
	l := opBaseLen + len(o.Type) + len(o.Path)

	if o.marshalWithValue() {
		valueLen := len(targetBytes)
		if !o.Path.isRoot() {
			r := gjson.GetBytes(targetBytes, o.Path.toJSONPath())
			valueLen = len(r.Raw)
		}
		l += valueFieldLen + valueLen
	}
	if o.hasFrom() {
		l += fromFieldLen + len(o.From)
	}
	return l
}

func (o Operation) hasFrom() bool {
	switch o.Type {
	case OperationAdd, OperationReplace, OperationTest:
		return false
	default:
		return true
	}
}

func (o Operation) marshalWithValue() bool {
	switch o.Type {
	case OperationCopy, OperationMove, OperationRemove:
		return false
	default:
		return true
	}
}

// String implements the fmt.Stringer interface.
func (p *Diff) String() string {
	if p == nil || len(*p) == 0 {
		return ""
	}
	sb := strings.Builder{}

	for i, op := range *p {
		if i != 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(op.String())
	}
	return sb.String()
}

func (p *Diff) remove(idx int) Diff {
	return (*p)[:idx+copy((*p)[idx:], (*p)[idx+1:])]
}

func (p *Diff) append(typ string, from, path pointer, src, tgt interface{}) Diff {
	return append(*p, Operation{
		Type:     typ,
		From:     from,
		Path:     path,
		OldValue: src,
		Value:    tgt,
	})
}

func (p *Diff) jsonLength(targetBytes []byte) int {
	length := 0
	if p == nil {
		return length
	}
	for _, op := range *p {
		length += op.jsonLength(targetBytes)
	}
	// Count comma-separators if the patch
	// has more than one operation.
	if len(*p) > 1 {
		length += len(*p) - 1
	}
	return length
}
