package myjson

import (
	"github.com/autom8ter/myjson/errors"
	"github.com/samber/lo"
)

type defaultOptimizer struct{}

func defaultExplain(c CollectionSchema) Explain {
	return Explain{
		Collection:    c.Collection(),
		Index:         c.PrimaryIndex(),
		MatchedFields: []string{},
		MatchedValues: map[string]any{},
		SeekFields:    []string{},
		SeekValues:    map[string]any{},
		Reverse:       false,
	}
}

func (o defaultOptimizer) Optimize(c CollectionSchema, where []Where) (Explain, error) {
	if len(c.PrimaryIndex().Fields) == 0 {
		return Explain{}, errors.New(errors.Internal, "zero configured indexes")
	}
	indexes := c.Indexing()
	if len(indexes) == 0 {
		return Explain{}, errors.New(errors.Internal, "zero configured indexes")
	}
	if len(where) == 0 {
		return defaultExplain(c), nil
	}
	if c.PrimaryIndex().Fields[0] == where[0].Field && where[0].Op == WhereOpEq {
		return Explain{
			Index:         c.PrimaryIndex(),
			MatchedFields: []string{c.PrimaryKey()},
			MatchedValues: getMatchedFieldValues([]string{c.PrimaryKey()}, where),
		}, nil
	}
	var (
		opt = &Explain{
			Collection: c.Collection(),
		}
	)
	for _, index := range indexes {
		if len(index.Fields) == 0 {
			continue
		}
		var (
			matchedFields []string
			seekFields    []string
			reverse       bool
		)
		for i, field := range index.Fields {
			if len(where) > i {
				if field == where[i].Field && where[i].Op == WhereOpEq {
					matchedFields = append(matchedFields, field)
				} else if field == where[i].Field && len(index.Fields)-1 == i {
					switch {
					case where[i].Op == WhereOpGt:
						seekFields = append(seekFields, field)
					case where[i].Op == WhereOpGte:
						seekFields = append(seekFields, field)
					case where[i].Op == WhereOpLt:
						seekFields = append(seekFields, field)
						reverse = true
					case where[i].Op == WhereOpLte:
						seekFields = append(seekFields, field)
						reverse = true
					}
				}
			}
		}
		matchedFields = lo.Uniq(matchedFields)
		if len(matchedFields)+len(seekFields) >= len(opt.MatchedFields)+len(opt.SeekFields) {
			opt.Index = index
			opt.MatchedFields = matchedFields
			opt.Reverse = reverse
			opt.SeekFields = seekFields
		}
	}
	if len(opt.MatchedFields)+len(opt.SeekFields) > 0 {
		opt.MatchedValues = getMatchedFieldValues(opt.MatchedFields, where)
		opt.SeekValues = getMatchedFieldValues(opt.SeekFields, where)
		return *opt, nil
	}
	if c.RequireQueryIndex() {
		return Explain{}, errors.New(errors.Forbidden, "index is required for query in collection: %s", c.Collection())
	}
	return defaultExplain(c), nil
}

func getMatchedFieldValues(fields []string, where []Where) map[string]any {
	if len(fields) == 0 {
		return map[string]any{}
	}
	var whereValues = map[string]any{}
	for _, f := range fields {
		for _, w := range where {
			if w.Field != f {
				continue
			}
			whereValues[w.Field] = w.Value
		}
	}
	return whereValues
}
