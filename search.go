package wolverine

import (
	"context"
	"github.com/autom8ter/wolverine/schema"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

func (d *db) Search(ctx context.Context, collection string, q schema.SearchQuery) (schema.Page, error) {
	now := time.Now()
	c, ok := d.getInmemCollection(collection)
	if !ok || !c.Indexing().HasSearchIndex() {
		return schema.Page{}, stacktrace.NewError("unsupported full text search collection: %s must be one of: %v", collection, d.schema.CollectionNames())
	}
	var (
		fields []string
		limit  = q.Limit
	)
	for _, w := range q.Where {
		fields = append(fields, w.Field)
	}
	if limit == 0 {
		limit = 1000
	}
	var queries []query.Query
	for _, where := range q.Where {
		if where.Value == nil {
			return schema.Page{}, stacktrace.NewError("empty where clause value")
		}
		switch where.Op {
		case schema.Basic:
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
		case schema.DateRange:
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
		case schema.TermRange:
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
		case schema.GeoDistance:
			var (
				from     float64
				to       float64
				distance string
			)
			split := strings.Split(cast.ToString(where.Value), ",")
			if len(split) < 3 {
				return schema.Page{}, stacktrace.NewError("geo distance where clause requires 3 comma separated values: lat(float), lng(float), distance(string)")
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
		case schema.Prefix:
			qry := bleve.NewPrefixQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case schema.Fuzzy:
			qry := bleve.NewFuzzyQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case schema.Regex:
			qry := bleve.NewRegexpQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case schema.Wildcard:
			qry := bleve.NewWildcardQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		}
	}
	if len(queries) == 0 {
		queries = []query.Query{bleve.NewMatchAllQuery()}
	}
	var searchRequest *bleve.SearchRequest
	if len(queries) > 1 {
		searchRequest = bleve.NewSearchRequestOptions(bleve.NewConjunctionQuery(queries...), q.Limit, q.Page*q.Limit, false)
	} else {
		searchRequest = bleve.NewSearchRequestOptions(bleve.NewConjunctionQuery(queries[0]), q.Limit, q.Page*q.Limit, false)
	}
	searchRequest.Fields = []string{"*"}
	results, err := d.getFullText(collection).Search(searchRequest)
	if err != nil {
		return schema.Page{}, stacktrace.Propagate(err, "failed to search index: %s", collection)
	}

	var data []*schema.Document
	for _, h := range results.Hits {
		if len(h.Fields) == 0 {
			continue
		}
		record, err := schema.NewDocumentFromMap(h.Fields)
		if err != nil {
			return schema.Page{}, stacktrace.Propagate(err, "failed to search index: %s", collection)
		}
		data = append(data, record)
	}
	if len(q.Select) > 0 {
		for _, r := range data {
			r.Select(q.Select)
		}
	}
	return schema.Page{
		Documents: data,
		NextPage:  q.Page + 1,
		Count:     len(data),
		Stats: schema.PageStats{
			ExecutionTime: time.Since(now),
		},
	}, nil
}

// SearchPaginate paginates through each page of the query until the handlePage function returns false or there are no more results
func (d *db) SearchPaginate(ctx context.Context, collection string, query schema.SearchQuery, handlePage schema.PageHandler) error {
	page := query.Page
	for {
		results, err := d.Search(ctx, collection, schema.SearchQuery{
			Select: query.Select,
			Where:  query.Where,
			Page:   page,
			Limit:  query.Limit,
		})
		if err != nil {
			return stacktrace.Propagate(err, "failed to query collection: %s", collection)
		}
		if len(results.Documents) == 0 {
			return nil
		}
		if !handlePage(results) {
			return nil
		}
		page = results.NextPage
	}
}
