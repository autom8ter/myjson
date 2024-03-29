package myjson

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/util"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

type stringKey string

type indexDiff struct {
	toRemove []Index
	toAdd    []Index
	toUpdate []Index
}

func getIndexDiff(after, before map[string]Index) (indexDiff, error) {
	var (
		toRemove []Index
		toAdd    []Index
		toUpdate []Index
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

func defaultAs(function AggregateFunction, field string) string {
	if function != "" {
		return fmt.Sprintf("%s_%s", function, field)
	}
	return field
}

func compareField(field string, i, j *Document) bool {
	iFieldVal := i.Get(field)
	jFieldVal := j.Get(field)
	switch val := i.Get(field).(type) {
	case time.Time:
		return val.After(cast.ToTime(jFieldVal))
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

func orderByDocs(d Documents, orderBys []OrderBy) Documents {
	if len(orderBys) == 0 {
		return d
	}
	orderBy := orderBys[0]

	if orderBy.Direction == OrderByDirectionDesc {
		sort.Slice(d, func(i, j int) bool {
			index := 1
			if d[i].Get(orderBy.Field) != d[j].Get(orderBy.Field) {
				return compareField(orderBy.Field, d[i], d[j])
			}
			for index < len(orderBys) {
				order := orderBys[index]
				if order.Direction == OrderByDirectionDesc {
					if d[i].Get(order.Field) != d[j].Get(order.Field) {
						return compareField(order.Field, d[i], d[j])
					}
				} else {
					if d[i].Get(order.Field) != d[j].Get(order.Field) {
						return !compareField(order.Field, d[i], d[j])
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
					if order.Direction == OrderByDirectionDesc {
						if d[i].Get(order.Field) != d[j].Get(order.Field) {
							return compareField(order.Field, d[i], d[j])
						}
					} else {
						if d[i].Get(order.Field) != d[j].Get(order.Field) {
							return !compareField(order.Field, d[i], d[j])
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

func groupByDocs(documents Documents, fields []string) map[string]Documents {
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

func aggregateDocs(d Documents, selects []Select) (*Document, error) {
	var (
		aggregated *Document
	)
	var aggregates = lo.Filter[Select](selects, func(s Select, i int) bool {
		return s.Aggregate != ""
	})
	var nonAggregates = lo.Filter[Select](selects, func(s Select, i int) bool {
		return s.Aggregate == ""
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

func applyNonAggregates(selct Select, aggregated, next *Document) error {
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

func applyAggregates(agg Select, aggregated, next *Document) error {
	current := aggregated.GetFloat(agg.As)
	switch agg.Aggregate {
	case AggregateFunctionCount:
		current++
	case AggregateFunctionMax:
		if value := next.GetFloat(agg.Field); value > current {
			current = value
		}
	case AggregateFunctionMin:
		if value := next.GetFloat(agg.Field); value < current {
			current = value
		}
	case AggregateFunctionSum:
		current += next.GetFloat(agg.Field)
	default:
		return errors.New(errors.Validation, "unsupported aggregate function: %s/%s", agg.Field, agg.Aggregate)
	}
	if err := aggregated.Set(agg.As, current); err != nil {
		return err
	}
	return nil
}

func selectDocument(d *Document, fields []Select) error {
	if len(fields) == 0 || fields[0].Field == "*" {
		return nil
	}
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
	err := d.Overwrite(patch)
	if err != nil {
		return err
	}
	return nil
}

func collectionConfigKey(ctx context.Context, collection string) []byte {
	return []byte(fmt.Sprintf("cache.internal.collections.%s", collection))
}

func collectionConfigPrefix(ctx context.Context) []byte {
	return []byte("cache.internal.collections.")
}

func schemaToCtx(ctx context.Context, schema CollectionSchema) context.Context {
	if schema == nil {
		return ctx
	}
	return context.WithValue(ctx, stringKey(fmt.Sprintf("%s.schema", schema.Collection())), schema)
}

func schemaFromCtx(ctx context.Context, collection string) CollectionSchema {
	c, ok := ctx.Value(stringKey(fmt.Sprintf("%s.schema", collection))).(CollectionSchema)
	if !ok {
		return nil
	}
	return c
}

func isAggregateQuery(q Query) bool {
	for _, a := range q.Select {
		if a.Aggregate != "" {
			return true
		}
	}
	return false
}
