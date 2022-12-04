package model

import (
	"context"
	"fmt"
	"github.com/autom8ter/gokvkit/internal/util"
)

func (q QueryJson) IsAggregate() bool {
	for _, a := range q.Select {
		if !util.IsNil(a.Aggregate) {
			return true
		}
	}
	return false
}

// Validate validates the query and returns a validation error if one exists
func (q QueryJson) Validate(ctx context.Context) error {
	vlid := QueryJSONSchema.Validate(ctx, q)
	if !vlid.IsValid() {
		return fmt.Errorf("%s", util.JSONString(&vlid.Errs))

	}
	return nil
}

func defaultAs(function QueryJsonSelectElemAggregate, field string) string {
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
