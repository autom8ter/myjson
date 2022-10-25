package schema

type Relationships struct {
	ForeignKeys map[string]ForeignKey `json:"foreignKeys"`
}

type ForeignKey struct {
	Collection string `json:"collection"`
}
