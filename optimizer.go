package gokvkit

import (
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
)

// Optimizer selects the best index from a set of indexes based on where clauses
type Optimizer interface {
	// Optimize selects the optimal index to use based on the given where clauses
	Optimize(indexes map[string]Index, where []Where) (OptimizerResult, error)
}

type defaultOptimizer struct{}

func (o defaultOptimizer) Optimize(indexes map[string]Index, where []Where) (OptimizerResult, error) {
	if len(indexes) == 0 {
		return OptimizerResult{}, stacktrace.NewErrorWithCode(ErrTODO, "zero configured indexes")
	}
	values := indexableFields(where)
	var (
		i = OptimizerResult{
			Values: values,
		}
		primary Index
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
				if field == where[i].Field && (where[i].Op == Eq || where[i].Op == "==") {
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
	return OptimizerResult{
		Ref:            primary,
		MatchedFields:  []string{},
		Values:         values,
		IsPrimaryIndex: true,
	}, nil
}

func indexableFields(where []Where) map[string]any {
	var whereFields []string
	var whereValues = map[string]any{}
	for _, w := range where {
		if w.Op != "==" && w.Op != Eq {
			continue
		}
		whereFields = append(whereFields, w.Field)
		whereValues[w.Field] = w.Value
	}
	return whereValues
}
