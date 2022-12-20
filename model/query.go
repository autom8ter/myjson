package model

import (
	"context"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/samber/lo"
)

type WhereOp string

const WhereOpContains WhereOp = "contains"
const WhereOpEq WhereOp = "eq"
const WhereOpGt WhereOp = "gt"
const WhereOpGte WhereOp = "gte"
const WhereOpIn WhereOp = "in"
const WhereOpContainsAll WhereOp = "containsAll"
const WhereOpContainsAny WhereOp = "containsAny"
const WhereOpNeq WhereOp = "neq"
const WhereOpLt WhereOp = "lt"
const WhereOpLte WhereOp = "lte"

type OrderByDirection string

const OrderByDirectionAsc OrderByDirection = "asc"
const OrderByDirectionDesc OrderByDirection = "desc"

type SelectAggregate string

const SelectAggregateCount SelectAggregate = "count"
const SelectAggregateMax SelectAggregate = "max"
const SelectAggregateMin SelectAggregate = "min"
const SelectAggregateSum SelectAggregate = "sum"

// Query is a query against the NOSQL database
type Query struct {
	Select  []Select  `json:"select" validate:"min=1,required"`
	Where   []Where   `json:"where,omitempty" validate:"dive"`
	GroupBy []string  `json:"groupBy,omitempty"`
	Page    int       `json:"page" validate:"min=0"`
	Limit   int       `json:"limit,omitempty" validate:"min=0"`
	OrderBy []OrderBy `json:"orderBy,omitempty" validate:"dive"`
}

type OrderBy struct {
	Direction OrderByDirection `json:"direction" validate:"oneof='desc' 'asc'"`
	Field     string           `json:"field"`
}

type Select struct {
	Aggregate SelectAggregate `json:"aggregate,omitempty" validate:"oneof='count' 'max' 'min' 'sum'"`
	As        string          `json:"as,omitempty"`
	Field     string          `json:"field"`
}

type Where struct {
	Field string      `json:"field" validate:"required"`
	Op    WhereOp     `json:"op" validate:"oneof='eq' 'neq' 'gt' 'gte' 'lt' 'lte' 'contains' 'containsAny' 'containsAll' 'in'"`
	Value interface{} `json:"value" validate:"required"`
}

// Validate validates the query and returns a validation error if one exists
func (q Query) Validate(ctx context.Context) error {
	if err := util.ValidateStruct(&q); err != nil {
		return errors.Wrap(err, errors.Validation, "")
	}
	if len(q.Select) == 0 {
		return errors.New(errors.Validation, "query validation error: at least one select is required")
	}
	isAggregate := false
	for _, a := range q.Select {
		if a.Field == "" {
			return errors.New(errors.Validation, "empty required field: 'select.field'")
		}
		if a.Aggregate != "" {
			isAggregate = true
		}
	}
	if isAggregate {
		for _, a := range q.Select {
			if a.Aggregate == "" {
				if !lo.Contains(q.GroupBy, a.Field) {
					return errors.New(errors.Validation, "'%s', is required in the group_by clause when aggregating", a.Field)
				}
			}
		}
		for _, g := range q.GroupBy {
			if !lo.ContainsBy[Select](q.Select, func(f Select) bool {
				return f.Field == g
			}) {
				return errors.New(errors.Validation, "'%s', is required in the select clause when aggregating", g)
			}
		}
	}
	return nil
}

func (q Query) IsAggregate() bool {
	for _, a := range q.Select {
		if a.Aggregate != "" {
			return true
		}
	}
	return false
}
