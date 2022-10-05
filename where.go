package wolverine

// WhereOp is an operator used to compare a value to a records field value in a where clause
type WhereOp string

// Valid returns true if it is a valid operation
func (o WhereOp) Valid() bool {
	switch o {
	case LteOp, LtOp, GteOp, GtOp, EqOp, NeqOp, ContainsOp, FuzzyOp, PrefixOp, TermOp:
		return true
	default:
		return false
	}
}

// IsSearch returns true if the operator requires full text search
func (o WhereOp) IsSearch() bool {
	switch o {
	case ContainsOp, FuzzyOp, PrefixOp, TermOp:
		return true
	default:
		return false
	}
}

const (
	// PrefixOp is a full text search type for finding records based on prefix matching. full text search operators can only be used
	// against collections that have full text search enabled
	PrefixOp WhereOp = "prefix"
	// ContainsOp full text search type for finding records based on contains matching. full text search operators can only be used
	// against collections that have full text search enabled
	ContainsOp WhereOp = "contains"
	// TermOp full text search type for finding records based on term matching. full text search operators can only be used
	// against collections that have full text search enabled
	TermOp WhereOp = "term"
	// FuzzyOp full text search type for finding records based on a fuzzy search. full text search operators can only be used
	// against collections that have full text search enabled
	FuzzyOp WhereOp = "fuzzy"
	// EqOp matches on equality
	EqOp WhereOp = "eq"
	// NeqOp matches on inequality
	NeqOp WhereOp = "neq"
	// GtOp matches on greater than
	GtOp WhereOp = "gt"
	// GteOp matches on greater than or equal to
	GteOp WhereOp = "gte"
	// LtOp matches on less than
	LtOp WhereOp = "lt"
	// LteOp matches on greater than or equal to
	LteOp WhereOp = "lte"
)

// Where is field-level filter for database queries
type Where struct {
	// Field is a field to compare against records field. For full text search, wrap the field in search(field1,field2,field3) and use a search operator
	Field string `json:"field"`
	// Op is an operator used to compare the field against the value.
	Op WhereOp `json:"op"`
	// Value is a value to compare against a records field value
	Value interface{} `json:"value"`
}

// OrderByDirection indicates whether results should be sorted in ascending or descending order
type OrderByDirection string

const (
	// ASC indicates ascending order
	ASC OrderByDirection = "ASC"
	// DESC indicates descending order
	DESC OrderByDirection = "DESC"
)

// OrderBy orders the result set by a given field in a given direction
type OrderBy struct {
	Field     string           `json:"field"`
	Direction OrderByDirection `json:"direction"`
}

// Query is a query against the NOSQL database
type Query struct {
	Select  []string `json:"select"`
	Where   []Where  `json:"where"`
	StartAt string   `json:"start_at"`
	Limit   int      `json:"limit"`
	OrderBy OrderBy  `json:"order_by"`
}
