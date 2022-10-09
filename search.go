package wolverine

import (
	"context"
	"fmt"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/spf13/cast"
)

func (d *db) isSearchQuery(collection string, query Query) bool {
	if c, ok := d.getInmemCollection(collection); !ok || c.fullText == nil {
		return false
	}
	for _, w := range query.Where {
		switch {
		case w.Op.IsSearch():
			return true
		}
	}
	return false
}

func (d *db) search(ctx context.Context, collection string, q Query) ([]*Document, error) {
	c, ok := d.getInmemCollection(collection)
	if !ok {
		return nil, fmt.Errorf("unsupported full text search collection: %s", collection)
	}
	if c.fullText == nil {
		return nil, fmt.Errorf("unsupported full text search collection: %s", collection)
	}
	var (
		wheres []Where
		fields []string
	)
	for _, w := range q.Where {
		switch {
		case w.Op.IsSearch():
			wheres = append(wheres, w)
			fields = append(fields, w.Field)
		}
	}

	if len(wheres) == 0 {
		return nil, fmt.Errorf("%s search: invalid search query", collection)
	}
	var queries []query.Query
	for _, where := range wheres {
		switch where.Op {
		case Term:
			queries = append(queries, bleve.NewTermQuery(cast.ToString(where.Value)))
		case Prefix:
			queries = append(queries, bleve.NewPrefixQuery(cast.ToString(where.Value)))
		case Fuzzy:
			queries = append(queries, bleve.NewFuzzyQuery(cast.ToString(where.Value)))
		case Regex:
			queries = append(queries, bleve.NewRegexpQuery(cast.ToString(where.Value)))
		case Contains:
			queries = append(queries, bleve.NewQueryStringQuery(cast.ToString(where.Value)))
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
	if searchRequest != nil {
		searchRequest.Fields = fields
		searchRequest.Size = q.Limit
		if searchRequest.Size == 0 {
			searchRequest.Size = 100
		}
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
		ok, err := record.Where(q.Where)
		if err != nil {
			return nil, err
		}
		if ok {
			data = append(data, record)
		}
	}
	data = orderBy(q.OrderBy, q.Limit, data)
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
