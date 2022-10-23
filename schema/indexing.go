package schema

// Indexing
type Indexing struct {
	Query     []*QueryIndex     `json:"query"`
	Aggregate []*AggregateIndex `json:"aggregate"`
	Search    *SearchIndex      `json:"search"`
}

func (i Indexing) HasQueryIndex() bool {
	return len(i.Query) > 0
}

func (i Indexing) HasSearchIndex() bool {
	return len(i.Search.fields) > 0
}

func (i Indexing) HasAggregateIndex() bool {
	return len(i.Aggregate) > 0
}
