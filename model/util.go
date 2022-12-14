package model

import (
	"fmt"
	"sort"
	"strings"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

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

// OrderBy orders the documents by the OrderBy clause
func OrderByDocs(d Documents, orderBys []OrderBy) Documents {
	if len(orderBys) == 0 {
		return d
	}
	// TODO: support more than one order by
	orderBy := orderBys[0]

	if orderBy.Direction == OrderByDirectionDesc {
		sort.Slice(d, func(i, j int) bool {
			index := 1
			if d[i].Get(orderBy.Field) != d[j].Get(orderBy.Field) {
				return compareField(orderBy.Field, d[i], d[j])
			}
			for index < len(orderBys) {
				order := orderBys[index]
				if d[i].Get(order.Field) != d[j].Get(order.Field) {
					return compareField(order.Field, d[i], d[j])
				}
				if d[i].Get(order.Field) != d[j].Get(order.Field) {
					if order.Direction == OrderByDirectionDesc {
						if d[i].Get(orderBy.Field) != d[j].Get(orderBy.Field) {
							return compareField(orderBy.Field, d[i], d[j])
						}
					} else {
						if d[i].Get(orderBy.Field) != d[j].Get(orderBy.Field) {
							return !compareField(orderBy.Field, d[i], d[j])
						}
					}
				}
				index++
			}
			return false
		})
	} else {
		sort.Slice(d, func(i, j int) bool {
			index := 1
			if d[i].Get(orderBy.Field) != d[j].Get(orderBy.Field) {
				return !compareField(orderBy.Field, d[i], d[j])
			}
			for index < len(orderBys) {
				order := orderBys[index]
				if d[i].Get(order.Field) != d[j].Get(order.Field) {
					return !compareField(order.Field, d[i], d[j])
				}
				if d[i].Get(order.Field) != d[j].Get(order.Field) {
					if order.Direction == OrderByDirectionDesc {
						if d[i].Get(orderBy.Field) != d[j].Get(orderBy.Field) {
							return compareField(orderBy.Field, d[i], d[j])
						}
					} else {
						if d[i].Get(orderBy.Field) != d[j].Get(orderBy.Field) {
							return !compareField(orderBy.Field, d[i], d[j])
						}
					}
				}
				index++
			}
			return false

		})
	}
	return d
}

// GroupByDocs groups the documents by the given fields
func GroupByDocs(documents Documents, fields []string) map[string]Documents {
	var grouped = map[string]Documents{}
	for _, d := range documents {
		var values []string
		for _, g := range fields {
			values = append(values, cast.ToString(d.Get(g)))
		}
		group := strings.Join(values, ".")
		grouped[group] = append(grouped[group], d)
	}
	return grouped
}

// AggregateDocs reduces the documents with the input aggregates
func AggregateDocs(d Documents, selects []Select) (*Document, error) {
	var (
		aggregated *Document
	)
	var aggregates = lo.Filter[Select](selects, func(s Select, i int) bool {
		return s.Aggregate != nil
	})
	var nonAggregates = lo.Filter[Select](selects, func(s Select, i int) bool {
		return s.Aggregate == nil
	})
	for _, next := range d {
		if aggregated == nil || !aggregated.Valid() {
			aggregated = NewDocument()
			for _, nagg := range nonAggregates {
				if err := applyNonAggregates(nagg, aggregated, next); err != nil {
					return nil, err
				}
			}
		}
		for _, agg := range aggregates {
			if util.IsNil(agg.As) {
				agg.As = util.ToPtr(defaultAs(*agg.Aggregate, agg.Field))
			}
			if err := applyAggregates(agg, aggregated, next); err != nil {
				return nil, err
			}
		}
	}
	return aggregated, nil
}

func applyNonAggregates(selct Select, aggregated, next *Document) error {
	value := next.Get(selct.Field)
	if util.IsNil(selct.As) {
		if err := aggregated.Set(selct.Field, value); err != nil {
			return err
		}
	} else {
		if err := aggregated.Set(*selct.As, value); err != nil {
			return err
		}
	}
	return nil
}

func applyAggregates(agg Select, aggregated, next *Document) error {
	current := aggregated.GetFloat(*agg.As)
	switch *agg.Aggregate {
	case SelectAggregateCount:
		current++
	case SelectAggregateMax:
		if value := next.GetFloat(agg.Field); value > current {
			current = value
		}
	case SelectAggregateMin:
		if value := next.GetFloat(agg.Field); value < current {
			current = value
		}
	case SelectAggregateSum:
		current += next.GetFloat(agg.Field)
	default:
		return errors.New(errors.Validation, "unsupported aggregate function: %s/%s", agg.Field, *agg.Aggregate)
	}
	if err := aggregated.Set(*agg.As, current); err != nil {
		return err
	}
	return nil
}
