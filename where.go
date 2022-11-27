package gokvkit

// WhereOp is an operator used to compare a value to a records field value in a where clause
type WhereOp string

const (
	// Eq matches on equality
	Eq WhereOp = "eq"
	// Neq matches on inequality
	Neq WhereOp = "neq"
	// Gt matches on greater than
	Gt WhereOp = "gt"
	// Gte matches on greater than or equal to
	Gte WhereOp = "gte"
	// Lt matches on less than
	Lt WhereOp = "lt"
	// Lte matches on greater than or equal to
	Lte WhereOp = "lte"
	// Contains matches on text containing a substring
	Contains WhereOp = "contains"
	// In matches on an element being contained in a list
	In WhereOp = "in"
)

// Where is field-level filter for database queries
type Where struct {
	// Field is a field to compare against records field. For full text search, wrap the field in search(field1,field2,field3) and use a search operator
	Field string `json:"field"`
	// Op is an operator used to compare the field against the value.
	Op WhereOp `json:"op"`
	// Value is a value to compare against a records field value
	Value any `json:"value"`
}
