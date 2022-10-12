package wolverine

import (
	"context"

	"github.com/palantir/stacktrace"
)

type PageHandler func(document []*Document) bool

// PaginateQuery paginates through each page of the query until the handlePage function returns false or there are no more results
func PaginateQuery(collection string, query Query, handlePage PageHandler) Script {
	return func(ctx context.Context, db DB) error {
		page := query.Page
		for {
			results, err := db.Query(ctx, collection, Query{
				Select:  query.Select,
				Where:   query.Where,
				Page:    page,
				Limit:   query.Limit,
				OrderBy: query.OrderBy,
			})
			if err != nil {
				return stacktrace.Propagate(err, "failed to query collection: %s", collection)
			}
			if len(results.Documents) == 0 {
				return nil
			}
			if !handlePage(results.Documents) {
				return nil
			}
			page = results.NextPage
		}
	}
}

// PaginateSearch paginates through each page of the query until the handlePage function returns false or there are no more results
func PaginateSearch(collection string, query SearchQuery, handlePage PageHandler) Script {
	return func(ctx context.Context, db DB) error {
		page := query.Page
		for {
			results, err := db.Search(ctx, collection, SearchQuery{
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
			if !handlePage(results.Documents) {
				return nil
			}
			page = results.NextPage
		}
	}
}
