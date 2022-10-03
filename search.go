package wolverine

import (
	"context"
	"fmt"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/spf13/cast"
)

func (d *db) isSearchQuery(collection string, query Query) bool {
	if _, ok := d.fullText[collection]; !ok {
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

func (d *db) search(ctx context.Context, collection string, query Query) ([]*Document, error) {
	if _, ok := d.fullText[collection]; !ok {
		return nil, fmt.Errorf("unsupported full text search collection: %s", collection)
	}
	var (
		wheres []Where
		fields []string
	)
	for _, w := range query.Where {
		switch {
		case w.Op.IsSearch():
			wheres = append(wheres, w)
			fields = append(fields, w.Field)
		}
	}

	if len(wheres) == 0 {
		return nil, fmt.Errorf("%s search: invalid search query", collection)
	}
	var searchRequest *bleve.SearchRequest
	for _, where := range wheres {
		switch where.Op {
		case TermOp:
			searchRequest = bleve.NewSearchRequest(bleve.NewTermQuery(cast.ToString(where.Value)))
		case PrefixOp:
			searchRequest = bleve.NewSearchRequest(bleve.NewPrefixQuery(cast.ToString(where.Value)))
		case FuzzyOp:
			searchRequest = bleve.NewSearchRequest(bleve.NewFuzzyQuery(cast.ToString(where.Value)))
		default:
			searchRequest = bleve.NewSearchRequest(bleve.NewQueryStringQuery(cast.ToString(where.Value)))
		}
		searchRequest.Fields = strings.Split(where.Field, ",")
	}
	//searchRequest.Fields = []string{"*"}
	searchRequest.Fields = fields
	searchRequest.Size = query.Limit
	if searchRequest.Size == 0 {
		searchRequest.Size = 100
	}
	results, err := d.fullText[collection].Search(searchRequest)
	if err != nil {
		return nil, err
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
		if d.config.OnRead != nil {
			if d.config.OnRead != nil {
				if err := d.config.OnRead(d, ctx, record); err != nil {
					return data, d.wrapErr(err, "")
				}
			}
		}
		data = append(data, record)
	}
	if len(query.Select) > 0 {
		for _, r := range data {
			r.Select(query.Select)
		}
	}
	return data, nil
}
