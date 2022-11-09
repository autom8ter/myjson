package brutus

// ScanFunc returns false to stop scanning and an error if one occurred
type ScanFunc func(d *Document) (bool, error)

// Scan scans for documents passing its filters
type Scan struct {
	// Filter filters out records that don't pass the where clause(s)
	Filter []Where `json:"filter"`
	// Reverse reverses the order of results
	Reverse bool `json:"reverse"`
}
