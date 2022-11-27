package gokvkit

// ScanFunc returns false to stop scanning and an error if one occurred
type ScanFunc func(d *Document) (bool, error)

// Scan scans the optimal index for documents passing its filters.
// results will not be ordered unless an index supporting the order by(s) was found by the optimizer
// Query should be used when order is more important than performance/resource-usage
type Scan struct {
	// From is the collection to scan
	From string `json:"from"`
	// Where filters out records that don't pass the where clause(s)
	Where []Where `json:"filter"`
	// OrderBy
	OrderBy OrderBy `json:"order_by"`
}
