package model

import "fmt"
import "reflect"
import "encoding/json"

// Query is a query against the NOSQL database - it does not support full text
// search
type Query struct {
	// GroupBy corresponds to the JSON schema field "groupBy".
	GroupBy []string `yaml:"groupBy,omitempty"`

	// Limit corresponds to the JSON schema field "limit".
	Limit *int `yaml:"limit,omitempty"`

	// OrderBy corresponds to the JSON schema field "orderBy".
	OrderBy []OrderBy `yaml:"orderBy,omitempty"`

	// Page corresponds to the JSON schema field "page".
	Page *int `yaml:"page,omitempty"`

	// Select corresponds to the JSON schema field "select".
	Select []Select `yaml:"select"`

	// Where corresponds to the JSON schema field "where".
	Where []Where `yaml:"where,omitempty"`
}

// orderBy orders results by a field and a direction
type OrderBy struct {
	// Direction corresponds to the JSON schema field "direction".
	Direction OrderByDirection `yaml:"direction"`

	// Field corresponds to the JSON schema field "field".
	Field string `yaml:"field"`
}

type OrderByDirection string

const OrderByDirectionAsc OrderByDirection = "asc"
const OrderByDirectionDesc OrderByDirection = "desc"

// select is a list of fields to select from each record in the datbase(optional)
type Select struct {
	// an aggregate function to apply against the field
	Aggregate *SelectAggregate `yaml:"aggregate,omitempty"`

	// as is outputs the value of the field as an alias
	As *string `yaml:"as,omitempty"`

	// the select's field
	Field string `yaml:"field"`

	// a function to apply against the field
	Function *SelectFunction `yaml:"function,omitempty"`
}

type SelectAggregate string

const SelectAggregateCount SelectAggregate = "count"
const SelectAggregateMax SelectAggregate = "max"
const SelectAggregateMin SelectAggregate = "min"
const SelectAggregateSum SelectAggregate = "sum"

type SelectFunction string

const SelectFunctionToLower SelectFunction = "toLower"
const SelectFunctionToUpper SelectFunction = "toUpper"

// where is a filter applied against a query
type Where struct {
	// Field corresponds to the JSON schema field "field".
	Field string `yaml:"field"`

	// Op corresponds to the JSON schema field "op".
	Op WhereOp `yaml:"op"`

	// Value corresponds to the JSON schema field "value".
	Value interface{} `yaml:"value"`
}

type WhereOp string

const WhereOpContains WhereOp = "contains"
const WhereOpEq WhereOp = "eq"
const WhereOpGt WhereOp = "gt"
const WhereOpGte WhereOp = "gte"
const WhereOpIn WhereOp = "in"

// UnmarshalJSON implements json.Unmarshaler.
func (j *OrderByDirection) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	var ok bool
	for _, expected := range enumValues_OrderByDirection {
		if reflect.DeepEqual(v, expected) {
			ok = true
			break
		}
	}
	if !ok {
		return fmt.Errorf("invalid value (expected one of %#v): %#v", enumValues_OrderByDirection, v)
	}
	*j = OrderByDirection(v)
	return nil
}

var enumValues_WhereOp = []interface{}{
	"eq",
	"neq",
	"gt",
	"gte",
	"lt",
	"lte",
	"in",
	"contains",
}

// UnmarshalJSON implements json.Unmarshaler.
func (j *WhereOp) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	var ok bool
	for _, expected := range enumValues_WhereOp {
		if reflect.DeepEqual(v, expected) {
			ok = true
			break
		}
	}
	if !ok {
		return fmt.Errorf("invalid value (expected one of %#v): %#v", enumValues_WhereOp, v)
	}
	*j = WhereOp(v)
	return nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (j *SelectAggregate) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	var ok bool
	for _, expected := range enumValues_SelectAggregate {
		if reflect.DeepEqual(v, expected) {
			ok = true
			break
		}
	}
	if !ok {
		return fmt.Errorf("invalid value (expected one of %#v): %#v", enumValues_SelectAggregate, v)
	}
	*j = SelectAggregate(v)
	return nil
}

const WhereOpNeq WhereOp = "neq"

// UnmarshalJSON implements json.Unmarshaler.
func (j *Select) UnmarshalJSON(b []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if v, ok := raw["field"]; !ok || v == nil {
		return fmt.Errorf("field field: required")
	}
	type Plain Select
	var plain Plain
	if err := json.Unmarshal(b, &plain); err != nil {
		return err
	}
	*j = Select(plain)
	return nil
}

var enumValues_SelectAggregate = []interface{}{
	"sum",
	"count",
	"max",
	"min",
}

const WhereOpLt WhereOp = "lt"
const WhereOpLte WhereOp = "lte"

// UnmarshalJSON implements json.Unmarshaler.
func (j *OrderBy) UnmarshalJSON(b []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if v, ok := raw["direction"]; !ok || v == nil {
		return fmt.Errorf("field direction: required")
	}
	if v, ok := raw["field"]; !ok || v == nil {
		return fmt.Errorf("field field: required")
	}
	type Plain OrderBy
	var plain Plain
	if err := json.Unmarshal(b, &plain); err != nil {
		return err
	}
	*j = OrderBy(plain)
	return nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (j *SelectFunction) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	var ok bool
	for _, expected := range enumValues_SelectFunction {
		if reflect.DeepEqual(v, expected) {
			ok = true
			break
		}
	}
	if !ok {
		return fmt.Errorf("invalid value (expected one of %#v): %#v", enumValues_SelectFunction, v)
	}
	*j = SelectFunction(v)
	return nil
}

var enumValues_SelectFunction = []interface{}{
	"toLower",
	"toUpper",
}

// UnmarshalJSON implements json.Unmarshaler.
func (j *Where) UnmarshalJSON(b []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if v, ok := raw["field"]; !ok || v == nil {
		return fmt.Errorf("field field: required")
	}
	if v, ok := raw["op"]; !ok || v == nil {
		return fmt.Errorf("field op: required")
	}
	if v, ok := raw["value"]; !ok || v == nil {
		return fmt.Errorf("field value: required")
	}
	type Plain Where
	var plain Plain
	if err := json.Unmarshal(b, &plain); err != nil {
		return err
	}
	*j = Where(plain)
	return nil
}

var enumValues_OrderByDirection = []interface{}{
	"asc",
	"desc",
}

// UnmarshalJSON implements json.Unmarshaler.
func (j *Query) UnmarshalJSON(b []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if v, ok := raw["select"]; !ok || v == nil {
		return fmt.Errorf("field select: required")
	}
	type Plain Query
	var plain Plain
	if err := json.Unmarshal(b, &plain); err != nil {
		return err
	}
	*j = Query(plain)
	return nil
}
