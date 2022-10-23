package schema

// Indexing
type Indexing struct {
	Query     []*QueryIndex     `json:"query"`
	Aggregate []*AggregateIndex `json:"aggregate"`
	Search    []*SearchIndex    `json:"search"`
}

func (i Indexing) HasQueryIndex() bool {
	return i.Query != nil && len(i.Query) > 0
}

func (i Indexing) HasSearchIndex() bool {
	return i.Search != nil && len(i.Search) > 0
}

func (i Indexing) HasAggregateIndex() bool {
	return i.Aggregate != nil && len(i.Aggregate) > 0
}
