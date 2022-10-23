package schema

import "github.com/autom8ter/wolverine/internal/prefix"

// QueryIndex is a database index used for quickly finding records with specific field values
type QueryIndex struct {
	Fields []string `json:"fields"`
}

func (i QueryIndex) Prefix(collection string) *prefix.PrefixIndexRef {
	return prefix.NewPrefixedIndex(collection, i.Fields)
}

func PrimaryQueryIndex(collection string) *prefix.PrefixIndexRef {
	return QueryIndex{
		Fields: []string{"_id"},
	}.Prefix(collection)
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
		target  *QueryIndex
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
	if target != nil && len(target.Fields) > 0 {
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
