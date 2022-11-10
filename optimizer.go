package brutus

import (
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
)

// Optimizer selects the best index from a set of indexes based on a query
type Optimizer interface {
	// BestIndex selects the optimal index to use based on the given where clauses
	BestIndex(indexes map[string]Index, where []Where, order OrderBy) (IndexMatch, error)
}

// IndexMatch is an index matched to a read request
type IndexMatch struct {
	// Ref is the matching index
	Ref Index `json:"ref"`
	// MatchedFields is the fields that match the index
	MatchedFields []string `json:"matchedFields"`
	// IsOrdered indicates whether the index delivers results in the order of the query.
	// If the index order does not match the query order, a full table scan is necessary to retrieve the result set.
	IsOrdered bool `json:"isOrdered"`
	// IsPrimaryIndex indicates whether the primary index was selected
	IsPrimaryIndex bool `json:"isPrimaryIndex"`
	// Values are the original values used to target the index
	Values map[string]any `json:"values"`
}

type defaultOptimizer struct{}

// BestIndex selects the optimal index to use given the where/orderby clause
func (o defaultOptimizer) BestIndex(indexes map[string]Index, where []Where, order OrderBy) (IndexMatch, error) {
	if len(indexes) == 0 {
		return IndexMatch{}, stacktrace.NewErrorWithCode(ErrTODO, "zero configured indexes")
	}

	values := indexableFields(where, order)
	var (
		i = IndexMatch{
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
		if index.Fields[0] == order.Field {
			matchedFields = append(matchedFields, order.Field)
		}
		for i, field := range index.Fields {
			if len(where) > i {
				if field == where[i].Field && (where[i].Op == "eq" || where[i].Op == "==") {
					matchedFields = append(matchedFields, field)
				}
			}
		}
		matchedFields = lo.Uniq(matchedFields)
		if (len(matchedFields) > len(i.MatchedFields)) ||
			(len(matchedFields) == len(i.MatchedFields) && index.Fields[0] == order.Field) {
			i.Ref = index
			i.IsOrdered = index.Fields[0] == order.Field || index.Primary
			i.MatchedFields = matchedFields
			i.IsPrimaryIndex = index.Primary
		}

	}
	if len(i.MatchedFields) > 0 {
		return i, nil
	}

	return IndexMatch{
		Ref:            primary,
		MatchedFields:  []string{primary.Fields[0]},
		IsOrdered:      true,
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
