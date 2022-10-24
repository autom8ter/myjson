package wolverine_test

import (
	"context"
	"github.com/autom8ter/wolverine"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/autom8ter/wolverine/schema"
	"github.com/palantir/stacktrace"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(testutil.AllCollections, func(ctx context.Context, db *wolverine.DB) {
			assert.Nil(t, db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
				for i := 0; i < 10; i++ {
					assert.Nil(t, collection.Set(ctx, testutil.NewUserDoc()))
				}
				return nil
			}))
		}))
	})
	t.Run("batch set", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(testutil.AllCollections, func(ctx context.Context, db *wolverine.DB) {
			assert.Nil(t, db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
				var usrs []*schema.Document
				var ids []string
				for i := 0; i < 1000; i++ {
					usr := testutil.NewUserDoc()
					ids = append(ids, usr.GetID())
					usrs = append(usrs, usr)
				}
				assert.Nil(t, collection.BatchSet(ctx, usrs))
				{
					assert.Nil(t, collection.Reindex(ctx))
				}
				{
					allUsrs, err := collection.GetAll(ctx, ids)
					assert.Nil(t, err)
					assert.Equal(t, 1000, len(allUsrs))
				}
				{
					for _, u := range usrs {
						usr, err := collection.Get(ctx, u.GetID())
						if err != nil {
							return stacktrace.Propagate(err, "")
						}
						assert.Equal(t, u.String(), usr.String())
					}
				}
				{
					results, err := collection.Query(ctx, schema.Query{
						Select: []string{"account_id"},
						Where: []schema.Where{
							{
								Field: "account_id",
								Op:    ">",
								Value: 50,
							},
						},
						Page:    0,
						Limit:   0,
						OrderBy: schema.OrderBy{},
					})
					assert.Nil(t, err)
					assert.Greater(t, len(results.Documents), 1)
					for _, result := range results.Documents {
						assert.Greater(t, result.GetFloat("account_id"), float64(50))
					}
					t.Logf("found %v documents in %s", results.Count, results.Stats.ExecutionTime)
				}
				{
					results, err := collection.Query(ctx, schema.Query{
						Select:  nil,
						Page:    0,
						Limit:   0,
						OrderBy: schema.OrderBy{},
					})
					assert.Nil(t, err)
					assert.Equal(t, 1000, len(results.Documents))
					t.Logf("found %v documents in %s", results.Count, results.Stats.ExecutionTime)
				}

				{
					pageCount := 0
					err := collection.QueryPaginate(ctx, schema.Query{
						Page:    0,
						Limit:   10,
						OrderBy: schema.OrderBy{},
					}, func(page schema.Page) bool {
						pageCount++
						return true
					})
					assert.Nil(t, err)
					assert.Equal(t, 100, pageCount)
				}
				{
					results, err := collection.Aggregate(ctx, schema.AggregateQuery{
						GroupBy: []string{"account_id", "gender"},
						Where: []schema.Where{
							{
								Field: "account_id",
								Op:    ">",
								Value: 90,
							},
						},
						Aggregates: []schema.Aggregate{
							{
								Field:    "gender",
								Function: schema.COUNT,
								Alias:    "gender_count",
							},
						},
						Page:  0,
						Limit: 0,
						OrderBy: schema.OrderBy{
							Field:     "account_id",
							Direction: schema.DESC,
						},
					})
					assert.Nil(t, err)
					assert.Greater(t, results.Count, 1)
					for _, doc := range results.Documents {
						t.Logf("aggregate: %s", doc.String())
					}
					t.Logf("found %v aggregates in %s", results.Count, results.Stats.ExecutionTime)
				}
				{

				}
				{
					for _, id := range ids {
						assert.Nil(t, collection.Delete(ctx, id))
					}
					for _, id := range ids {
						_, err := collection.Get(ctx, id)
						assert.NotNil(t, err)
					}
				}
				return nil
			}))
		}))
	})
}
