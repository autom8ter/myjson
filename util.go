package gokvkit

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/autom8ter/gokvkit/model"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

type indexDiff struct {
	toRemove []model.Index
	toAdd    []model.Index
	toUpdate []model.Index
}

func getIndexDiff(after, before map[string]model.Index) (indexDiff, error) {
	var (
		toRemove []model.Index
		toAdd    []model.Index
		toUpdate []model.Index
	)
	for _, index := range after {
		if _, ok := before[index.Name]; !ok {
			toAdd = append(toAdd, index)
		}
	}

	for _, current := range before {
		if _, ok := after[current.Name]; !ok {
			toRemove = append(toRemove, current)
		} else {
			if !reflect.DeepEqual(current.Fields, current.Fields) {
				toUpdate = append(toUpdate, current)
			}
		}
	}
	return indexDiff{
		toRemove: toRemove,
		toAdd:    toAdd,
		toUpdate: toUpdate,
	}, nil
}

func defaultAs(function model.SelectAggregate, field string) string {
	if function != "" {
		return fmt.Sprintf("%s_%s", function, field)
	}
	return field
}

func compareField(field string, i, j *model.Document) bool {
	iFieldVal := i.Get(field)
	jFieldVal := j.Get(field)
	switch val := i.Get(field).(type) {
	case bool:
		return val && !cast.ToBool(jFieldVal)
	case float64:
		return val > cast.ToFloat64(jFieldVal)
	case string:
		return val > cast.ToString(jFieldVal)
	default:
		return util.JSONString(iFieldVal) > util.JSONString(jFieldVal)
	}
}

func orderByDocs(d model.Documents, orderBys []model.OrderBy) model.Documents {
	if len(orderBys) == 0 {
		return d
	}
	// TODO: support more than one order by
	orderBy := orderBys[0]

	if orderBy.Direction == model.OrderByDirectionDesc {
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
					if order.Direction == model.OrderByDirectionDesc {
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
					if order.Direction == model.OrderByDirectionDesc {
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

func groupByDocs(documents model.Documents, fields []string) map[string]model.Documents {
	var grouped = map[string]model.Documents{}
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

func aggregateDocs(d model.Documents, selects []model.Select) (*model.Document, error) {
	var (
		aggregated *model.Document
	)
	var aggregates = lo.Filter[model.Select](selects, func(s model.Select, i int) bool {
		return s.Aggregate != ""
	})
	var nonAggregates = lo.Filter[model.Select](selects, func(s model.Select, i int) bool {
		return s.Aggregate == ""
	})
	for _, next := range d {
		if aggregated == nil || !aggregated.Valid() {
			aggregated = model.NewDocument()
			for _, nagg := range nonAggregates {
				if err := applyNonAggregates(nagg, aggregated, next); err != nil {
					return nil, err
				}
			}
		}
		for _, agg := range aggregates {
			if agg.As == "" {
				agg.As = defaultAs(agg.Aggregate, agg.Field)
			}
			if err := applyAggregates(agg, aggregated, next); err != nil {
				return nil, err
			}
		}
	}
	return aggregated, nil
}

func applyNonAggregates(selct model.Select, aggregated, next *model.Document) error {
	value := next.Get(selct.Field)
	if selct.As == "" {
		if err := aggregated.Set(selct.Field, value); err != nil {
			return err
		}
	} else {
		if err := aggregated.Set(selct.As, value); err != nil {
			return err
		}
	}
	return nil
}

func applyAggregates(agg model.Select, aggregated, next *model.Document) error {
	current := aggregated.GetFloat(agg.As)
	switch agg.Aggregate {
	case model.SelectAggregateCount:
		current++
	case model.SelectAggregateMax:
		if value := next.GetFloat(agg.Field); value > current {
			current = value
		}
	case model.SelectAggregateMin:
		if value := next.GetFloat(agg.Field); value < current {
			current = value
		}
	case model.SelectAggregateSum:
		current += next.GetFloat(agg.Field)
	default:
		return errors.New(errors.Validation, "unsupported aggregate function: %s/%s", agg.Field, agg.Aggregate)
	}
	if err := aggregated.Set(agg.As, current); err != nil {
		return err
	}
	return nil
}

func selectDocument(d *model.Document, fields []model.Select) error {
	if len(fields) == 0 || fields[0].Field == "*" {
		return nil
	}
	var (
		selected = model.NewDocument()
	)
	patch := map[string]interface{}{}
	for _, f := range fields {
		if f.As == "" {
			if f.Aggregate != "" {
				f.As = defaultAs(f.Aggregate, f.Field)
			}
		}
		if f.As == "" {
			patch[f.Field] = d.Get(f.Field)
		} else {
			patch[f.As] = d.Get(f.Field)
		}
	}
	err := selected.SetAll(patch)
	if err != nil {
		return err
	}
	*d = *selected
	return nil
}
