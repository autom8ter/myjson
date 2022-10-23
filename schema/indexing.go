package schema

import "github.com/autom8ter/wolverine/internal/prefix"

// Indexing
type Indexing struct {
	Query     []QueryIndex     `json:"query"`
	Aggregate []AggregateIndex `json:"aggregate"`
	Search    SearchIndex      `json:"search"`
}

func (i Indexing) HasQueryIndex() bool {
	return len(i.Query) > 0
}

func (i Indexing) HasSearchIndex() bool {
	return len(i.Search.Fields) > 0
}

func (i Indexing) HasAggregateIndex() bool {
	return len(i.Aggregate) > 0
}

type QueryIndexMatch struct {
	Ref           *prefix.PrefixIndexRef
	Fields        []string
	Ordered       bool
	targetFields  []string
	targetOrderBy string
}

func (i QueryIndexMatch) FullScan() bool {
	return i.targetOrderBy != "" && !i.Ordered
}

func (i Indexing) GetQueryIndex(collection *Collection, whereFields []string, orderBy string) (QueryIndexMatch, error) {
	var (
		target  QueryIndex
		matched int
		ordered bool
	)
	indexing := collection.Indexing()
	if !indexing.HasQueryIndex() {
		return QueryIndexMatch{
			Ref:     PrimaryQueryIndex(collection.Collection()),
			Fields:  []string{"_id"},
			Ordered: orderBy == "_id" || orderBy == "",
		}, nil
	}
	for _, index := range indexing.Query {
		isOrdered := index.Fields[0] == orderBy
		var totalMatched int
		for i, f := range whereFields {
			if index.Fields[i] == f {
				totalMatched++
			}
		}
		if totalMatched > matched || (!ordered && isOrdered) {
			target = index
			ordered = isOrdered
		}
	}
	if len(target.Fields) > 0 {
		return QueryIndexMatch{
			Ref:     target.Prefix(collection.Collection()),
			Fields:  target.Fields,
			Ordered: ordered,
		}, nil
	}
	return QueryIndexMatch{
		Ref:     PrimaryQueryIndex(collection.Collection()),
		Fields:  []string{"_id"},
		Ordered: orderBy == "_id" || orderBy == "",
	}, nil
}
