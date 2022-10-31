package wolverine

import (
	"github.com/palantir/stacktrace"
)

type Optimizer interface {
	// BestIndex selects the optimal index to use based on the given where clauses
	BestIndex(indexes map[string]Index, where []Where, order OrderBy) (IndexMatch, error)
}

type defaultOptimizer struct {
}

// BestIndex selects the optimal index to use given the where/orderby clause
func (o defaultOptimizer) BestIndex(indexes map[string]Index, where []Where, order OrderBy) (IndexMatch, error) {
	if len(indexes) == 0 {
		return IndexMatch{}, stacktrace.NewErrorWithCode(ErrTODO, "zero configured indexes")
	}
	values := indexableFields(where, order)
	var (
		target  Index
		primary Index
		matched []string
		ordered bool
	)
	for _, index := range indexes {
		if len(index.Fields) == 0 {
			continue
		}
		if index.Primary {
			primary = index
		}
		isOrdered := index.Fields[0] == order.Field
		var totalMatched []string
		for i, field := range index.Fields {
			if len(where) > i {
				if field == where[i].Field {
					totalMatched = append(totalMatched, field)
				}
			}
		}
		if len(totalMatched) > len(matched) || (!ordered && isOrdered) {
			target = index
			ordered = isOrdered
			matched = totalMatched
		}
	}
	if len(target.Fields) > 0 {
		return IndexMatch{
			Ref:           target,
			MatchedFields: matched,
			IsOrdered:     ordered,
			Values:        values,
		}, nil
	}
	if len(primary.Fields) == 0 {
		return IndexMatch{}, stacktrace.NewErrorWithCode(ErrTODO, "missing primary key index")
	}
	return IndexMatch{
		Ref:            primary,
		MatchedFields:  []string{primary.Fields[0]},
		IsOrdered:      order.Field == primary.Fields[0] || order.Field == "",
		Values:         values,
		IsPrimaryIndex: true,
	}, nil
}

func indexableFields(where []Where, by OrderBy) map[string]any {
	var whereFields []string
	var whereValues = map[string]any{}
	if by.Field != "" {
		whereValues[by.Field] = nil
	}
	for _, w := range where {
		if w.Op != "==" && w.Op != Eq {
			continue
		}
		whereFields = append(whereFields, w.Field)
		whereValues[w.Field] = w.Value
	}
	return whereValues
}
