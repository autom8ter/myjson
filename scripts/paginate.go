package scripts

import (
	"context"

	"github.com/palantir/stacktrace"

	"github.com/autom8ter/wolverine"
)

// QueryPaginate paginates through each page of the query until the handlePage function returns false or there are no more results
func QueryPaginate(collection string, query wolverine.Query, handlePage func(document []*wolverine.Document) bool) wolverine.Script {
	return func(ctx context.Context, db wolverine.DB) error {
		startAt := query.StartAt
		for {
			results, err := db.Query(ctx, collection, wolverine.Query{
				Select:  query.Select,
				Where:   query.Where,
				StartAt: startAt,
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
			startAt = results.NextPage
		}
	}
}

// PaginateSearch paginates through each page of the query until the handlePage function returns false or there are no more results
func PaginateSearch(collection string, query wolverine.SearchQuery, handlePage func(document []*wolverine.Document) bool) wolverine.Script {
	return func(ctx context.Context, db wolverine.DB) error {
		startAt := query.StartAt
		for {
			results, err := db.Search(ctx, collection, wolverine.SearchQuery{
				Select:  query.Select,
				Where:   query.Where,
				StartAt: startAt,
				Limit:   query.Limit,
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
			startAt = results.NextPage
		}
	}
}
