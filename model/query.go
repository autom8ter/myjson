package model

import (
	"context"
	"fmt"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/samber/lo"
)
import "reflect"
import "encoding/json"

// Query is a query against the NOSQL database - it does not support full text
// search
type Query struct {
	// GroupBy corresponds to the JSON schema field "groupBy".
	GroupBy []string `json:"groupBy,omitempty"`

	// Limit corresponds to the JSON schema field "limit".
	Limit *int `json:"limit,omitempty"`

	// OrderBy corresponds to the JSON schema field "orderBy".
	OrderBy []OrderBy `json:"orderBy,omitempty"`

	// Page corresponds to the JSON schema field "page".
	Page *int `json:"page,omitempty"`

	// Select corresponds to the JSON schema field "select".
	Select []Select `json:"select"`

	// Where corresponds to the JSON schema field "where".
	Where []Where `json:"where,omitempty"`
}

// orderBy orders results by a field and a direction
type OrderBy struct {
	// Direction corresponds to the JSON schema field "direction".
	Direction OrderByDirection `json:"direction"`

	// Field corresponds to the JSON schema field "field".
	Field string `json:"field"`
}

type OrderByDirection string

const OrderByDirectionAsc OrderByDirection = "asc"
const OrderByDirectionDesc OrderByDirection = "desc"

// select is a list of fields to select from each record in the datbase(optional)
type Select struct {
	// an aggregate function to apply against the field
	Aggregate *SelectAggregate `json:"aggregate,omitempty"`

	// as is outputs the value of the field as an alias
	As *string `json:"as,omitempty"`

	// the select's field
	Field string `json:"field"`

	// a function to apply against the field
	Function *SelectFunction `json:"function,omitempty"`
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
	Field string `json:"field"`

	// Op corresponds to the JSON schema field "op".
	Op WhereOp `json:"op"`

	// Value corresponds to the JSON schema field "value".
	Value interface{} `json:"value"`
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

// Validate validates the query and returns a validation error if one exists
func (q Query) Validate(ctx context.Context) error {
	if len(q.Select) == 0 {
		return errors.Wrap(nil, errors.Validation, "query validation error: at least one select is required")
	}
	isAggregate := false
	for _, a := range q.Select {
		if a.Field == "" {
			return errors.Wrap(nil, errors.Validation, "empty required field: 'select.field'")
		}
		if a.Aggregate != nil {
			if a.Function != nil {
				return errors.Wrap(nil, errors.Validation, "select cannot have both a function and an aggregate")
			}
			isAggregate = true
		}
	}
	if isAggregate {
		for _, a := range q.Select {
			if a.Aggregate == nil {
				if !lo.Contains(q.GroupBy, a.Field) {
					return errors.Wrap(nil, errors.Validation, "'%s', is required in the group_by clause when aggregating", a.Field)
				}
			}
		}
		for _, g := range q.GroupBy {
			if !lo.ContainsBy[Select](q.Select, func(f Select) bool {
				return f.Field == g
			}) {
				return errors.Wrap(nil, errors.Validation, "'%s', is required in the select clause when aggregating", g)
			}
		}
	}
	return nil
}

func (q Query) IsAggregate() bool {
	for _, a := range q.Select {
		if !util.IsNil(a.Aggregate) {
			return true
		}
	}
	return false
}
