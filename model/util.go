package model

import (
	"fmt"

	"github.com/autom8ter/gokvkit/internal/util"
)

func (q Query) IsAggregate() bool {
	for _, a := range q.Select {
		if !util.IsNil(a.Aggregate) {
			return true
		}
	}
	return false
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
