package schema

import (
	"github.com/autom8ter/wolverine/internal/prefix"
)

// Indexing
type Indexing struct {
	PrimaryKey string         `json:"primaryKey"`
	Query      []*QueryIndex  `json:"query"`
	Search     []*SearchIndex `json:"search"`
}

func (i Indexing) HasQueryIndex() bool {
	return i.Query != nil && len(i.Query) > 0
}

func (i Indexing) HasSearchIndex() bool {
	return i.Search != nil && len(i.Search) > 0
}

// QueryIndex is a database index used for quickly finding records with specific field values
type QueryIndex struct {
	Fields []string `json:"fields"`
}

type QueryIndexMatch struct {
	Ref           *prefix.PrefixIndexRef `json:"-"`
	Fields        []string               `json:"fields"`
	Ordered       bool                   `json:"ordered"`
	targetFields  []string
	targetOrderBy string
}

func (i QueryIndexMatch) FullScan() bool {
	return i.targetOrderBy != "" && !i.Ordered
}

// SearchIndex
type SearchIndex struct {
	Fields []string `json:"fields"`
}

func IndexableFields(where []Where, by OrderBy) map[string]any {
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
