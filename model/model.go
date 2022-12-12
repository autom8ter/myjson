package model

import (
	_ "embed"
	"encoding/json"

	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/qri-io/jsonschema"
)

func init() {
	jsonContent, err := util.YAMLToJSON([]byte(PageSchema))
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(jsonContent, PageJSONSchema); err != nil {
		panic(err)
	}

	jsonContent, err = util.YAMLToJSON([]byte(QuerySchema))
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(jsonContent, QueryJSONSchema); err != nil {
		panic(err)
	}

}

//go:embed query.yaml
var QuerySchema string

//go:embed page.yaml
var PageSchema string

var (
	QueryJSONSchema = &jsonschema.Schema{}
	PageJSONSchema  = &jsonschema.Schema{}
)

// OptimizerResult is the output of a query optimizer
type OptimizerResult struct {
	// Ref is the matching index
	Ref Index `json:"ref"`
	// MatchedFields is the fields that match the index
	MatchedFields []string `json:"matchedFields"`
	// IsPrimaryIndex indicates whether the primary index was selected
	IsPrimaryIndex bool `json:"isPrimaryIndex"`
	// Values are the original values used to target the index
	Values map[string]any `json:"values"`
}

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
}
