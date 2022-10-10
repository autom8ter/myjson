package wolverine

import (
	"context"
	"fmt"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
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
	// Term full text search type for finding records based on term matching. full text search operators can only be used
	// against collections that have full text search enabled
	Term SearchOp = "term"
	// Fuzzy full text search type for finding records based on a fuzzy search. full text search operators can only be used
	// against collections that have full text search enabled
	Fuzzy SearchOp = "fuzzy"
	// Regex full text search type for finding records based on a regex matching. full text search operators can only be used
	// against collections that have full text search enabled
	Regex   SearchOp = "regex"
	Match   SearchOp = "match"
	Numeric SearchOp = "numeric"
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
	Where   []SearchWhere `json:"where"`
	StartAt string        `json:"start_at"`
	// Limit limits the number of records returned by the query (optional)
	Limit int `json:"limit"`
}

func (d *db) Search(ctx context.Context, collection string, q SearchQuery) ([]*Document, error) {
	c, ok := d.getInmemCollection(collection)
	if !ok {
		return nil, fmt.Errorf("unsupported full text search collection: %s", collection)
	}
	if c.fullText == nil {
		return nil, fmt.Errorf("unsupported full text search collection: %s", collection)
	}
	var (
		fields []string
	)
	for _, w := range q.Where {
		fields = append(fields, w.Field)
	}

	if len(q.Where) == 0 {
		return nil, fmt.Errorf("%s search: invalid search query", collection)
	}
	var queries []query.Query
	for _, where := range q.Where {
		switch where.Op {
		case Numeric:
			qry := bleve.NewNumericRangeQuery(lo.ToPtr(cast.ToFloat64(where.Value)), nil)
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case Match:
			qry := bleve.NewMatchQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case Term:
			qry := bleve.NewTermQuery(cast.ToString(where.Value))
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
		return nil, fmt.Errorf("%s search: invalid search query", collection)
	}
	var searchRequest *bleve.SearchRequest
	if len(queries) > 1 {
		searchRequest = bleve.NewSearchRequest(bleve.NewConjunctionQuery(queries...))
	} else {
		searchRequest = bleve.NewSearchRequest(queries[0])
	}
	searchRequest.Fields = fields
	searchRequest.Size = q.Limit
	if searchRequest.Size == 0 {
		searchRequest.Size = 100
	}
	if q.StartAt != "" {
		searchRequest.SearchAfter = []string{q.StartAt}
	}
	results, err := c.fullText.Search(searchRequest)
	if err != nil {
		return nil, d.wrapErr(err, "")
	}

	var data []*Document
	for _, h := range results.Hits {
		if len(h.Fields) == 0 {
			continue
		}
		record, err := NewDocumentFromMap(h.Fields)
		if err != nil {
			return nil, err
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
