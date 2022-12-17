package gokvkit

import (
	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/model"
	"github.com/samber/lo"
)

// Optimizer selects the best index from a set of indexes based on where clauses
type Optimizer interface {
	// Optimize selects the optimal index to use based on the given where clauses
	Optimize(c CollectionSchema, where []model.Where) (model.OptimizerResult, error)
}

type defaultOptimizer struct{}

func (o defaultOptimizer) Optimize(c CollectionSchema, where []model.Where) (model.OptimizerResult, error) {
	indexes := c.Indexing()
	if len(indexes) == 0 {
		return model.OptimizerResult{}, errors.New(errors.Internal, "zero configured indexes")
	}
	values := indexableFields(where)
	var (
		i = model.OptimizerResult{
			Values: values,
		}
		primary model.Index
	)
	for _, index := range indexes {
		if len(index.Fields) == 0 {
			continue
		}
		if index.Primary {
			primary = index
		}
		var matchedFields []string
		for i, field := range index.Fields {
			if len(where) > i {
				if field == where[i].Field && where[i].Op == model.WhereOpEq {
					matchedFields = append(matchedFields, field)
				}
			}
		}
		matchedFields = lo.Uniq(matchedFields)
		if (len(matchedFields) > len(i.MatchedFields)) ||
			(len(matchedFields) == len(i.MatchedFields)) {
			i.Ref = index
			i.MatchedFields = matchedFields
			i.IsPrimaryIndex = index.Primary
		}

	}
	if len(i.MatchedFields) > 0 {
		return i, nil
	}
	return model.OptimizerResult{
		Ref:            primary,
		MatchedFields:  []string{},
		Values:         values,
		IsPrimaryIndex: true,
	}, nil
}

func indexableFields(where []model.Where) map[string]any {
	var whereFields []string
	var whereValues = map[string]any{}
	for _, w := range where {
		if w.Op != "==" && w.Op != model.WhereOpEq {
			continue
		}
		whereFields = append(whereFields, w.Field)
		whereValues[w.Field] = w.Value
	}
	return whereValues
}
