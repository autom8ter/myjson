package schema

import "github.com/autom8ter/wolverine/internal/prefix"

// QueryIndex is a database index used for quickly finding records with specific field values
type QueryIndex struct {
	// Fields is a list of fields that are indexed
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
