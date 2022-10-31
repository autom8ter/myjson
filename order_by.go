package wolverine

// OrderByDirection indicates whether results should be sorted in ascending or descending order
type OrderByDirection string

const (
	// ASC indicates ascending order
	ASC OrderByDirection = "ASC"
	// DESC indicates descending order
	DESC OrderByDirection = "DESC"
)

// OrderBy orders the result set by a given field in a given direction
// OrderBy requires an index on the field that the query is sorting on.
type OrderBy struct {
	// Field is the field to sort on
	Field string `json:"field"`
	// Direction is the sort direction
	Direction OrderByDirection `json:"direction"`
}
