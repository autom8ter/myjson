package wolverine_test

import (
	"context"
	"github.com/autom8ter/wolverine"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/autom8ter/wolverine/schema"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"runtime"
	"strings"
	"testing"
	"time"
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
	assert.Nil(t, testutil.TestDB(testutil.AllCollections, func(ctx context.Context, db *wolverine.DB) {
		assert.Nil(t, db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
			var usrs []*schema.Document
			var ids []string
			t.Run("batch set", func(t *testing.T) {
				for i := 0; i < 1000; i++ {
					usr := testutil.NewUserDoc()
					ids = append(ids, collection.Schema().GetDocumentID(usr))
					usrs = append(usrs, usr)
				}
				assert.Nil(t, collection.BatchSet(ctx, usrs))
			})
			t.Run("reindex", func(t *testing.T) {
				assert.Nil(t, collection.Reindex(ctx))
			})
			t.Run("get all", func(t *testing.T) {
				allUsrs, err := collection.GetAll(ctx, ids)
				assert.Nil(t, err)
				assert.Equal(t, 1000, len(allUsrs))
			})
			t.Run("get each", func(t *testing.T) {
				for _, u := range usrs {
					usr, err := collection.Get(ctx, collection.Schema().GetDocumentID(u))
					if err != nil {
						t.Fatal(err)
					}
					assert.Equal(t, u.String(), usr.String())
				}
			})
			t.Run("query users account_id > 50", func(t *testing.T) {
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
			})
			t.Run("query all", func(t *testing.T) {
				results, err := collection.Query(ctx, schema.Query{
					Select:  nil,
					Page:    0,
					Limit:   0,
					OrderBy: schema.OrderBy{},
				})
				assert.Nil(t, err)
				assert.Equal(t, 1000, len(results.Documents))
				t.Logf("found %v documents in %s", results.Count, results.Stats.ExecutionTime)
			})
			t.Run("paginate all", func(t *testing.T) {
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
			})
			t.Run("aggregate account_id, gender, count", func(t *testing.T) {
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
				assert.EqualValues(t, []string{"account_id", "gender"}, results.Stats.IndexMatch.Fields)
				t.Logf("found %v aggregates in %s", results.Count, results.Stats.ExecutionTime)
			})
			t.Run("search wildcard name", func(t *testing.T) {
				results, err := collection.Search(ctx, schema.SearchQuery{
					Select: []string{"*"},
					Where: []schema.SearchWhere{
						{
							Field: "name",
							Op:    schema.Wildcard,
							Value: "*",
						},
						{
							Field: "account_id",
							Op:    schema.Basic,
							Value: 50,
						},
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				assert.GreaterOrEqual(t, results.Count, 1)
				t.Logf("found %v wildcard search results in %s", results.Count, results.Stats.ExecutionTime)
			})
			t.Run("search basic contact.email ", func(t *testing.T) {
				results, err := collection.Search(ctx, schema.SearchQuery{
					Select: []string{"*"},
					Where: []schema.SearchWhere{
						{
							Field: "contact.email",
							Op:    schema.Basic,
							Value: usrs[0].GetString("contact.email"),
						},
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				assert.GreaterOrEqual(t, results.Count, 1)
				t.Logf("found %v basic search results in %s", results.Count, results.Stats.ExecutionTime)
			})
			t.Run("search prefix contact.email", func(t *testing.T) {
				var prefix = strings.Split(usrs[0].GetString("contact.email"), "@")[0]
				results, err := collection.Search(ctx, schema.SearchQuery{
					Select: []string{"*"},
					Where: []schema.SearchWhere{
						{
							Field: "contact.email",
							Op:    schema.Prefix,
							Value: prefix,
						},
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				assert.GreaterOrEqual(t, results.Count, 1)
				t.Logf("found %v prefix search results in %s", results.Count, results.Stats.ExecutionTime)
			})
			t.Run("search fuzzy contact.email", func(t *testing.T) {
				var prefix = strings.Split(usrs[0].GetString("contact.email"), "@")[0]
				results, err := collection.Search(ctx, schema.SearchQuery{
					Select: []string{"*"},
					Where: []schema.SearchWhere{
						{
							Field: "contact.email",
							Op:    schema.Fuzzy,
							Value: prefix,
						},
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				assert.GreaterOrEqual(t, results.Count, 1)
				t.Logf("found %v fuzzy search results in %s", results.Count, results.Stats.ExecutionTime)
			})
			t.Run("search regex contact.email", func(t *testing.T) {
				var prefix = strings.Split(usrs[0].GetString("contact.email"), "@")[0]
				results, err := collection.Search(ctx, schema.SearchQuery{
					Select: []string{"*"},
					Where: []schema.SearchWhere{
						{
							Field: "contact.email",
							Op:    schema.Regex,
							Value: "^" + prefix,
						},
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				assert.GreaterOrEqual(t, results.Count, 1)
				t.Logf("found %v regex search results in %s", results.Count, results.Stats.ExecutionTime)
			})
			t.Run("add tasks", func(t *testing.T) {
				var taskDocs []*schema.Document
				if err := db.Collection(ctx, "task", func(tasks *wolverine.Collection) error {
					for _, u := range usrs {
						task := testutil.NewTaskDoc(collection.Schema().GetDocumentID(u))
						taskDocs = append(taskDocs, task)
						assert.Nil(t, tasks.Set(ctx, task))
						ur, err := tasks.GetRelationship(ctx, "user", task)
						assert.Nil(t, err)
						assert.Equal(t, u.Get("_id"), ur.Get("_id"))
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			})
			t.Run("update contact.email", func(t *testing.T) {
				for _, u := range usrs {
					edit := u.Clone()
					email := gofakeit.Email()
					edit.Set("contact.email", email)
					assert.Nil(t, collection.Update(ctx, edit))
					doc, err := collection.Get(ctx, collection.Schema().GetDocumentID(edit))
					assert.Nil(t, err)
					assert.Equal(t, email, doc.GetString("contact.email"))
					assert.Equal(t, u.GetString("name"), doc.GetString("name"))
				}
			})

			t.Run("delete first 50", func(t *testing.T) {
				for _, id := range ids[:50] {
					assert.Nil(t, collection.Delete(ctx, id))
				}
				for _, id := range ids[:50] {
					_, err := collection.Get(ctx, id)
					assert.NotNil(t, err)
				}
			})
			t.Run("query delete all", func(t *testing.T) {
				assert.Nil(t, collection.QueryDelete(ctx, schema.Query{
					Select:  nil,
					Page:    0,
					Limit:   0,
					OrderBy: schema.OrderBy{},
				}))
				for _, id := range ids[50:] {
					_, err := collection.Get(ctx, id)
					assert.NotNil(t, err)
				}
			})
			return nil
		}))
	}))
	time.Sleep(1 * time.Second)
	t.Log(runtime.NumGoroutine())
}
