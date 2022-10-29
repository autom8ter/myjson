package wolverine

import (
	"github.com/autom8ter/wolverine/internal/prefix"
)

// Indexing
type Indexing struct {
	SearchEnabled bool     `json:"searchEnabled"`
	Indexes       []*Index `json:"indexes"`
}

func (i Indexing) HasIndexes() bool {
	return i.Indexes != nil && len(i.Indexes) > 0
}

// Index is a database index used for quickly finding records with specific field values
type Index struct {
	// Fields to index - order matters
	Fields []string `json:"fields"`
	// Unique indicates that it's a unique index which will enforce uniqueness
	Unique bool `json:"unique"`
}

type IndexMatch struct {
	Ref           *prefix.PrefixIndexRef `json:"-"`
	Fields        []string               `json:"fields"`
	Ordered       bool                   `json:"ordered"`
	targetFields  []string
	targetOrderBy string
}

func (i IndexMatch) FullScan() bool {
	return i.targetOrderBy != "" && !i.Ordered
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
