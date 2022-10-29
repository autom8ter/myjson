package wolverine

// ScanFunc returns false to stop scanning and an error if one occurred
type ScanFunc func(d *Document) (bool, error)

type Scan struct {
	Filter  []Where
	Reverse bool
}
