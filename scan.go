package wolverine

// ScanFunc returns false to stop scanning and an error if one occurred
type ScanFunc func(d *Document) (bool, error)

// Scan scans for documents passing its filters
type Scan struct {
	Filter  []Where `json:"filter"`
	Reverse bool    `json:"reverse"`
}
