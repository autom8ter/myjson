package gokvkit

// Function is a function that is applied against a document field
type Function string

const (
	// SUM returns the sum of an array of values
	SUM Function = "sum"
	// MAX returns the maximum value in an array of values
	MAX Function = "max"
	// MIN returns the minimum value in an array of values
	MIN Function = "min"
	// COUNT returns the count of an array of values
	COUNT Function = "count"
)

func (f Function) IsAggregate() bool {
	switch f {
	case SUM, MAX, MIN, COUNT:
		return true
	default:
		return false
	}
}
