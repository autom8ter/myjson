package gokvkit

import (
	"github.com/autom8ter/gokvkit/errors"

	"github.com/samber/lo"
)

type defaultOptimizer struct{}

func (o defaultOptimizer) Optimize(c CollectionSchema, where []Where) (Optimization, error) {
	if len(c.PrimaryIndex().Fields) == 0 {
		return Optimization{}, errors.New(errors.Internal, "zero configured indexes")
	}
	indexes := c.Indexing()
	if len(indexes) == 0 {
		return Optimization{}, errors.New(errors.Internal, "zero configured indexes")
	}
	var defaultOptimization = Optimization{
		Index:         c.PrimaryIndex(),
		MatchedFields: []string{},
		MatchedValues: map[string]any{},
	}
	if len(where) == 0 {
		return defaultOptimization, nil
	}
	if c.PrimaryIndex().Fields[0] == where[0].Field && where[0].Op == WhereOpEq {
		return Optimization{
			Index:         c.PrimaryIndex(),
			MatchedFields: []string{c.PrimaryKey()},
			MatchedValues: getMatchedFieldValues([]string{c.PrimaryKey()}, where),
		}, nil
	}
	var (
		i = Optimization{}
	)
	for _, index := range indexes {
		if len(index.Fields) == 0 {
			continue
		}
		var matchedFields []string
		for i, field := range index.Fields {
			if len(where) > i {
				if field == where[i].Field && where[i].Op == WhereOpEq {
					matchedFields = append(matchedFields, field)
				}
			}
		}
		matchedFields = lo.Uniq(matchedFields)
		if (len(matchedFields) > len(i.MatchedFields)) ||
			(len(matchedFields) == len(i.MatchedFields)) {
			i.Index = index
			i.MatchedFields = matchedFields
		}
	}
	if len(i.MatchedFields) > 0 {
		i.MatchedValues = getMatchedFieldValues(i.MatchedFields, where)
		return i, nil
	}
	return defaultOptimization, nil
}

func getMatchedFieldValues(fields []string, where []Where) map[string]any {
	var whereFields []string
	var whereValues = map[string]any{}
	for _, f := range fields {
		for _, w := range where {
			if w.Op != WhereOpEq || w.Field != f {
				continue
			}
			whereFields = append(whereFields, w.Field)
			whereValues[w.Field] = w.Value
		}
	}
	return whereValues
}
