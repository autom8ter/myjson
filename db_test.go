package wolverine_test

import (
	"bytes"
	"context"
	"github.com/autom8ter/wolverine"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/autom8ter/wolverine/schema"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func timer() func(t *testing.T) {
	now := time.Now()
	return func(t *testing.T) {
		t.Logf("duration: %s", time.Since(now))
	}
}

func Test(t *testing.T) {
	t.Run("basic collection checks", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			assert.True(t, db.HasCollection("user"))
			assert.True(t, db.HasCollection("task"))
			assert.False(t, db.HasCollection("zebras"))
			hasUser := false
			assert.Nil(t, db.Collections(ctx, func(collection *wolverine.Collection) error {
				if collection.Schema().Collection() == "user" {
					hasUser = true
				}
				return nil
			}))
			assert.True(t, hasUser)
		}))
	})
	t.Run("create", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			assert.Nil(t, db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
				id, err := collection.Create(ctx, testutil.NewUserDoc())
				assert.Nil(t, err)
				u, err := collection.Get(ctx, id)
				assert.Nil(t, err)
				assert.Equal(t, id, collection.Schema().GetDocumentID(u))
				return nil
			}))
		}))
	})
	t.Run("set", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			timer := timer()
			defer timer(t)
			assert.Nil(t, db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
				for i := 0; i < 10; i++ {
					assert.Nil(t, collection.Set(ctx, testutil.NewUserDoc()))
				}
				return nil
			}))
		}))
	})
	t.Run("change stream", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			assert.Nil(t, db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
				wg := sync.WaitGroup{}
				changes := 0
				wg.Add(1)
				go func() {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
					defer cancel()
					assert.Nil(t, collection.ChangeStream(ctx, func(ctx context.Context, change schema.StateChange) error {
						changes++
						return nil
					}))
				}()
				for i := 0; i < 3; i++ {
					assert.Nil(t, collection.Set(ctx, testutil.NewUserDoc()))
				}
				wg.Wait()
				assert.Equal(t, 3, changes)
				return nil
			}))
		}))
	})
	assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
		assert.Nil(t, db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
			var usrs []*schema.Document
			var ids []string
			t.Run("batch set", func(t *testing.T) {
				timer := timer()
				defer timer(t)
				for i := 0; i < 100; i++ {
					usr := testutil.NewUserDoc()
					ids = append(ids, collection.Schema().GetDocumentID(usr))
					usrs = append(usrs, usr)
				}
				assert.Nil(t, collection.BatchSet(ctx, usrs))
			})
			t.Run("reindex", func(t *testing.T) {
				timer := timer()
				defer timer(t)
				assert.Nil(t, collection.Reindex(ctx))
			})
			t.Run("get all", func(t *testing.T) {
				timer := timer()
				defer timer(t)
				allUsrs, err := collection.GetAll(ctx, ids)
				assert.Nil(t, err)
				assert.Equal(t, 100, len(allUsrs))
			})
			t.Run("get each", func(t *testing.T) {
				timer := timer()
				defer timer(t)
				for _, u := range usrs {
					usr, err := collection.Get(ctx, collection.Schema().GetDocumentID(u))
					if err != nil {
						t.Fatal(err)
					}
					assert.Equal(t, u.String(), usr.String())
				}
			})
			t.Run("query users account_id > 50", func(t *testing.T) {
				timer := timer()
				defer timer(t)
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
			t.Run("query users account_id in 51-55", func(t *testing.T) {
				timer := timer()
				defer timer(t)
				results, err := collection.Query(ctx, schema.Query{
					Select: []string{"account_id"},
					Where: []schema.Where{
						{
							Field: "account_id",
							Op:    schema.In,
							Value: []float64{51, 52, 53, 54, 55},
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
				timer := timer()
				defer timer(t)
				results, err := collection.Query(ctx, schema.Query{
					Select:  nil,
					Page:    0,
					Limit:   0,
					OrderBy: schema.OrderBy{},
				})
				assert.Nil(t, err)
				assert.Equal(t, 100, len(results.Documents))
				t.Logf("found %v documents in %s", results.Count, results.Stats.ExecutionTime)
			})
			t.Run("paginate all", func(t *testing.T) {
				timer := timer()
				defer timer(t)
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
				assert.Equal(t, 10, pageCount)
			})
			t.Run("aggregate account_id, gender, count", func(t *testing.T) {
				results, err := collection.Aggregate(ctx, schema.AggregateQuery{
					GroupBy: []string{"account_id"},
					Where: []schema.Where{
						{
							Field: "account_id",
							Op:    ">",
							Value: 1,
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
				assert.EqualValues(t, []string{"account_id"}, results.Stats.IndexMatch.Fields)
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
			t.Run("update contact.email", func(t *testing.T) {
				for _, u := range usrs {
					id := collection.Schema().GetDocumentID(u)
					email := gofakeit.Email()
					assert.Nil(t, collection.Update(ctx, id, map[string]any{
						"contact.email": email,
					}))
					doc, err := collection.Get(ctx, id)
					assert.Nil(t, err)
					assert.Equal(t, email, doc.GetString("contact.email"))
					assert.Equal(t, u.GetString("name"), doc.GetString("name"))
				}
			})
			backup := bytes.NewBuffer(nil)
			t.Run("backup & restore", func(t *testing.T) {
				assert.Nil(t, db.Backup(ctx, backup))
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
			t.Run("restore", func(t *testing.T) {
				assert.Nil(t, testutil.TestDB(func(ctx context.Context, db2 *wolverine.DB) {
					assert.Nil(t, db2.Restore(ctx, backup))
					assert.Nil(t, db2.Collection(ctx, "user", func(collection *wolverine.Collection) error {
						for _, id := range ids {
							_, err := collection.Get(ctx, id)
							assert.Nil(t, err)
						}
						return nil
					}))
				}))
			})
			return nil
		}))
	}))
	time.Sleep(1 * time.Second)
	t.Log(runtime.NumGoroutine())
}

func Benchmark(b *testing.B) {
	// Benchmark/set-12         	      22	  47669475 ns/op	  702120 B/op	    4481 allocs/op
	b.Run("set", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			assert.Nil(b, db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					assert.Nil(b, collection.Set(ctx, doc))
				}
				return nil
			}))
		}))
	})
	// Benchmark/get-12         	  207938	      5389 ns/op	    4228 B/op	      31 allocs/op
	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			assert.Nil(b, db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
				assert.Nil(b, collection.Set(ctx, doc))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := collection.Get(ctx, collection.Schema().GetDocumentID(doc))
					assert.Nil(b, err)
				}
				return nil
			}))
		}))
	})
	// Benchmark/query-12         	   55064	     20678 ns/op	   15849 B/op	     100 allocs/op
	b.Run("query", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			assert.Nil(b, db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
				assert.Nil(b, collection.Set(ctx, doc))
				var docs []*schema.Document
				for i := 0; i < 100; i++ {
					docs = append(docs, testutil.NewUserDoc())
				}
				assert.Nil(b, collection.BatchSet(ctx, docs))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					results, err := collection.Query(ctx, schema.Query{
						Select: nil,
						Where: []schema.Where{
							{
								Field: "contact.email",
								Op:    "==",
								Value: doc.GetString("contact.email"),
							},
						},
						Page:    0,
						Limit:   10,
						OrderBy: schema.OrderBy{},
					})
					assert.Nil(b, err)
					assert.Equal(b, 1, len(results.Documents))
					assert.Equal(b, "contact.email", results.Stats.IndexMatch.Fields[0])
				}
				return nil
			}))
		}))
	})
}

func TestAggregate(t *testing.T) {
	t.Run("sum basic", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			assert.Nil(t, db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
				var usrs []*schema.Document
				ageSum := float64(0)
				for i := 0; i < 10; i++ {
					u := testutil.NewUserDoc()
					ageSum += u.GetFloat("age")
					usrs = append(usrs, u)
				}
				query := schema.AggregateQuery{
					GroupBy: []string{"account_id"},
					//Where:      []schema.Where{
					//	{
					//
					//	},
					//},
					Aggregates: []schema.Aggregate{
						{
							Field:    "age",
							Function: schema.SUM,
							Alias:    "age_sum",
						},
					},
					Page:    0,
					Limit:   0,
					OrderBy: schema.OrderBy{},
				}
				result, err := schema.ApplyReducers(ctx, query, usrs)
				assert.Nil(t, err)
				assert.Equal(t, ageSum, result.GetFloat("age_sum"))
				return nil
			}))
		}))
	})
	t.Run("sum advanced", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			assert.Nil(t, db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
				var usrs []*schema.Document

				for i := 0; i < 10; i++ {
					u := testutil.NewUserDoc()
					usrs = append(usrs, u)
				}
				assert.Nil(t, collection.BatchSet(ctx, usrs))
				query := schema.AggregateQuery{
					GroupBy: []string{"account_id"},
					//Where:      []schema.Where{
					//	{
					//
					//	},
					//},
					Aggregates: []schema.Aggregate{
						{
							Field:    "age",
							Function: schema.SUM,
							Alias:    "age_sum",
						},
					},
					Page:    0,
					Limit:   0,
					OrderBy: schema.OrderBy{},
				}
				groups := lo.GroupBy[*schema.Document](usrs, func(t *schema.Document) string {
					return t.GetString("account_id")
				})

				ageSum := map[string]float64{}
				for grup, value := range groups {
					result, err := schema.ApplyReducers(ctx, query, value)
					assert.Nil(t, err)
					ageSum[grup] += result.GetFloat("age")
				}
				results, err := collection.Aggregate(ctx, query)
				if err != nil {
					t.Fatal(err)
				}
				for _, result := range results.Documents {
					assert.Equal(t, ageSum[result.GetString("account_id")], result.GetFloat("age_sum"))
				}
				return nil
			}))
		}))
	})
}
