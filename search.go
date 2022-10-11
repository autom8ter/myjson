package wolverine

import (
	"context"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

// SearchOp is an operator used within a where clause in a full text search query
type SearchOp string

const (
	// Prefix is a full text search type for finding records based on prefix matching. full text search operators can only be used
	// against collections that have full text search enabled
	Prefix SearchOp = "prefix"
	// Wildcard full text search type for finding records based on wildcard matching. full text search operators can only be used
	// against collections that have full text search enabled
	Wildcard SearchOp = "wildcard"
	// Fuzzy full text search type for finding records based on a fuzzy search. full text search operators can only be used
	// against collections that have full text search enabled
	Fuzzy SearchOp = "fuzzy"
	// Regex full text search type for finding records based on a regex matching. full text search operators can only be used
	// against collections that have full text search enabled
	Regex SearchOp = "regex"
	// Basic is a basic matcher that checks for an exact match.
	Basic       SearchOp = "basic"
	TermRange   SearchOp = "term_range"
	DateRange   SearchOp = "date_range"
	GeoDistance SearchOp = "geo_distance"
)

// Where is field-level filter for database queries
type SearchWhere struct {
	// Field is a field to compare against records field. For full text search, wrap the field in search(field1,field2,field3) and use a search operator
	Field string `json:"field"`
	// Op is an operator used to compare the field against the value.
	Op SearchOp `json:"op"`
	// Value is a value to compare against a records field value
	Value interface{} `json:"value"`
	// Boost boosts the score of records matching the where clause
	Boost float64 `json:"boost"`
}

// SearchQuery is a full text search query against the database
type SearchQuery struct {
	// Select is a list of fields to select from each record in the datbase(optional)
	Select []string `json:"select"`
	// Where is a list of where clauses used to filter records based on full text search (required)
	Where []SearchWhere `json:"where"`
	//
	StartAt string `json:"start_at"`
	// Limit limits the number of records returned by the query (optional)
	Limit int `json:"limit"`
}

func (d *db) Search(ctx context.Context, collection string, q SearchQuery) ([]*Document, error) {
	c, ok := d.getInmemCollection(collection)
	if !ok || !c.FullText() {
		return nil, stacktrace.NewError("unsupported full text search collection: %s must be one of: %v", collection, d.collectionNames())
	}
	var (
		fields []string
	)
	for _, w := range q.Where {
		fields = append(fields, w.Field)
	}
	if len(q.Where) == 0 {
		return nil, stacktrace.NewError("%s search: invalid search query", collection)
	}
	var queries []query.Query
	for _, where := range q.Where {
		if where.Value == nil {
			return nil, stacktrace.NewError("empty where clause value")
		}
		switch where.Op {
		case Basic:
			switch where.Value.(type) {
			case bool:
				qry := bleve.NewBoolFieldQuery(cast.ToBool(where.Value))
				if where.Boost > 0 {
					qry.SetBoost(where.Boost)
				}
				qry.SetField(where.Field)
				queries = append(queries, qry)
			case float64, int, int32, int64, float32, uint64, uint, uint8, uint16, uint32:
				qry := bleve.NewNumericRangeQuery(lo.ToPtr(cast.ToFloat64(where.Value)), nil)
				if where.Boost > 0 {
					qry.SetBoost(where.Boost)
				}
				qry.SetField(where.Field)
				queries = append(queries, qry)
			default:
				qry := bleve.NewMatchQuery(cast.ToString(where.Value))
				if where.Boost > 0 {
					qry.SetBoost(where.Boost)
				}
				qry.SetField(where.Field)
				queries = append(queries, qry)
			}
		case DateRange:
			var (
				from time.Time
				to   time.Time
			)
			split := strings.Split(cast.ToString(where.Value), ",")
			from = cast.ToTime(split[0])
			if len(split) == 2 {
				to = cast.ToTime(split[1])
			}
			qry := bleve.NewDateRangeQuery(from, to)
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case TermRange:
			var (
				from string
				to   string
			)
			split := strings.Split(cast.ToString(where.Value), ",")
			from = split[0]
			if len(split) == 2 {
				to = split[1]
			}
			qry := bleve.NewTermRangeQuery(from, to)
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case GeoDistance:
			var (
				from     float64
				to       float64
				distance string
			)
			split := strings.Split(cast.ToString(where.Value), ",")
			if len(split) < 3 {
				return nil, stacktrace.NewError("geo distance where clause requires 3 comma separated values: lat(float), lng(float), distance(string)")
			}
			from = cast.ToFloat64(split[0])
			to = cast.ToFloat64(split[1])
			distance = cast.ToString(split[2])
			qry := bleve.NewGeoDistanceQuery(from, to, distance)
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case Prefix:
			qry := bleve.NewPrefixQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case Fuzzy:
			qry := bleve.NewFuzzyQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case Regex:
			qry := bleve.NewRegexpQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case Wildcard:
			qry := bleve.NewWildcardQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		}
	}
	if len(queries) == 0 {
		return nil, stacktrace.NewError("%s search: invalid search query", collection)
	}
	var searchRequest *bleve.SearchRequest
	if len(queries) > 1 {
		searchRequest = bleve.NewSearchRequest(bleve.NewConjunctionQuery(queries...))
	} else {
		searchRequest = bleve.NewSearchRequest(queries[0])
	}
	searchRequest.Fields = []string{"*"}
	searchRequest.Size = q.Limit
	if searchRequest.Size == 0 {
		searchRequest.Size = 100
	}
	if q.StartAt != "" {
		searchRequest.SearchAfter = []string{q.StartAt}
	}
	results, err := d.getFullText(collection).Search(searchRequest)
	if err != nil {
		return nil, stacktrace.Propagate(err, "failed to search index: %s", collection)
	}

	var data []*Document
	for _, h := range results.Hits {
		if len(h.Fields) == 0 {
			continue
		}
		record, err := NewDocumentFromMap(h.Fields)
		if err != nil {
			return nil, stacktrace.Propagate(err, "failed to search index: %s", collection)
		}
		data = append(data, record)
	}
	if len(q.Select) > 0 {
		for _, r := range data {
			r.Select(q.Select)
		}
	}
	if q.Limit > 0 && len(data) > q.Limit {
		return data[:q.Limit], nil
	}
	return data, nil
}
