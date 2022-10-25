package wolverine

type DBInfo struct {
	Collections map[string]CollectionInfo `json:"collections"`
}

type CollectionInfo struct {
	Count int
}
