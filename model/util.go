package model

import (
	"context"
	"fmt"
	"net/http"

	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
)

func (q Query) IsAggregate() bool {
	for _, a := range q.Select {
		if !util.IsNil(a.Aggregate) {
			return true
		}
	}
	return false
}

// Validate validates the query and returns a validation error if one exists
func (q Query) Validate(ctx context.Context) error {
	if len(q.Select) == 0 {
		return stacktrace.NewErrorWithCode(http.StatusBadRequest, "query validation error: at least one select is required")
	}
	isAggregate := false
	for _, a := range q.Select {
		if a.Field == "" {
			return stacktrace.NewErrorWithCode(http.StatusBadRequest, "empty required field: 'select.field'")
		}
		if a.Aggregate != nil {
			isAggregate = true
		}
	}
	if isAggregate {
		for _, a := range q.Select {
			if a.Aggregate == nil {
				if !lo.Contains(q.GroupBy, a.Field) {
					return stacktrace.NewErrorWithCode(http.StatusBadRequest, "'%s', is required in the group_by clause when aggregating", a.Field)
				}
			}
		}
		for _, g := range q.GroupBy {
			if !lo.ContainsBy[Select](q.Select, func(f Select) bool {
				return f.Field == g
			}) {
				return stacktrace.NewErrorWithCode(http.StatusBadRequest, "'%s', is required in the select clause when aggregating", g)
			}
		}
	}
	return nil
}

func defaultAs(function SelectAggregate, field string) string {
	if function != "" {
		return fmt.Sprintf("%s_%s", function, field)
	}
	return field
}

func compareField(field string, i, j *Document) bool {
	iFieldVal := i.result.Get(field)
	jFieldVal := j.result.Get(field)
	switch i.result.Get(field).Value().(type) {
	case bool:
		return iFieldVal.Bool() && !jFieldVal.Bool()
	case float64:
		return iFieldVal.Float() > jFieldVal.Float()
	case string:
		return iFieldVal.String() > jFieldVal.String()
	default:
		return util.JSONString(iFieldVal.Value()) > util.JSONString(jFieldVal.Value())
	}
}
