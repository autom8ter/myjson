package wolverine_test

import (
	"context"
	"github.com/autom8ter/wolverine"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"runtime"
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
		}))
	})
	t.Run("create", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			collection := db.Collection("user")
			id, err := collection.Create(ctx, testutil.NewUserDoc())
			assert.Nil(t, err)
			u, err := collection.Get(ctx, id)
			assert.Nil(t, err)
			assert.Equal(t, id, collection.Schema().GetPrimaryKey(u))
		}))
	})
	t.Run("set", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			collection := db.Collection("user")
			timer := timer()
			defer timer(t)
			for i := 0; i < 10; i++ {
				assert.Nil(t, collection.Set(ctx, testutil.NewUserDoc()))
			}
		}))
	})
	t.Run("change stream", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			collection := db.Collection("user")
			wg := sync.WaitGroup{}
			changes := 0
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				defer cancel()
				assert.Nil(t, collection.ChangeStream(ctx, func(ctx context.Context, change wolverine.StateChange) error {
					changes++
					return nil
				}))
			}()
			for i := 0; i < 3; i++ {
				assert.Nil(t, collection.Set(ctx, testutil.NewUserDoc()))
			}
			wg.Wait()
			assert.Equal(t, 3, changes)
		}))
	})
	assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
		collection := db.Collection("user")
		var usrs []*wolverine.Document
		var ids []string
		t.Run("batch set", func(t *testing.T) {
			timer := timer()
			defer timer(t)
			for i := 0; i < 100; i++ {
				usr := testutil.NewUserDoc()
				ids = append(ids, collection.Schema().GetPrimaryKey(usr))
				usrs = append(usrs, usr)
			}
			assert.Nil(t, collection.BatchSet(ctx, usrs))
		})
		t.Run("reindex", func(t *testing.T) {
			timer := timer()
			defer timer(t)
			assert.Nil(t, collection.Reindex(ctx))
		})
		t.Run("get each", func(t *testing.T) {
			timer := timer()
			defer timer(t)
			for _, u := range usrs {
				usr, err := collection.Get(ctx, collection.Schema().GetPrimaryKey(u))
				if err != nil {
					t.Fatal(err)
				}
				assert.Equal(t, u.String(), usr.String())
			}
		})
		t.Run("query users account_id > 50", func(t *testing.T) {
			timer := timer()
			defer timer(t)
			results, err := collection.Query(ctx, wolverine.Query{
				Select: []string{"account_id"},
				Where: []wolverine.Where{
					{
						Field: "account_id",
						Op:    ">",
						Value: 50,
					},
				},
				Page:    0,
				Limit:   0,
				OrderBy: wolverine.OrderBy{},
			})
			assert.Nil(t, err)
			assert.Greater(t, len(results.Documents), 1)
			for _, result := range results.Documents {
				assert.Greater(t, result.GetFloat("account_id"), float64(50))
			}
			t.Logf("found %v documents in %s", results.Count, results.Stats.ExecutionTime)
		})
		t.Run("query users account_id in 51-60", func(t *testing.T) {
			timer := timer()
			defer timer(t)
			results, err := collection.Query(ctx, wolverine.Query{
				Select: []string{"account_id"},
				Where: []wolverine.Where{
					{
						Field: "account_id",
						Op:    wolverine.In,
						Value: []float64{51, 52, 53, 54, 55, 56, 57, 58, 59, 60},
					},
				},
				Page:    0,
				Limit:   0,
				OrderBy: wolverine.OrderBy{},
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
			results, err := collection.Query(ctx, wolverine.Query{
				Select:  nil,
				Page:    0,
				Limit:   0,
				OrderBy: wolverine.OrderBy{},
			})
			assert.Nil(t, err)
			assert.Equal(t, 100, len(results.Documents))
			t.Logf("found %v documents in %s", results.Count, results.Stats.ExecutionTime)
		})
		t.Run("paginate all", func(t *testing.T) {
			timer := timer()
			defer timer(t)
			pageCount := 0
			err := collection.QueryPaginate(ctx, wolverine.Query{
				Page:    0,
				Limit:   10,
				OrderBy: wolverine.OrderBy{},
			}, func(page wolverine.Page) bool {
				pageCount++
				return true
			})
			assert.Nil(t, err)
			assert.Equal(t, 10, pageCount)
		})
		t.Run("update contact.email", func(t *testing.T) {
			for _, u := range usrs {
				id := collection.Schema().GetPrimaryKey(u)
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
			assert.Nil(t, collection.QueryDelete(ctx, wolverine.Query{
				Select:  nil,
				Page:    0,
				Limit:   0,
				OrderBy: wolverine.OrderBy{},
			}))
			for _, id := range ids[50:] {
				_, err := collection.Get(ctx, id)
				assert.NotNil(t, err)
			}
		})
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
			collection := db.Collection("user")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				assert.Nil(b, collection.Set(ctx, doc))
			}
		}))
	})
	// Benchmark/get-12         	  207938	      5389 ns/op	    4228 B/op	      31 allocs/op
	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			collection := db.Collection("user")
			assert.Nil(b, collection.Set(ctx, doc))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := collection.Get(ctx, collection.Schema().GetPrimaryKey(doc))
				assert.Nil(b, err)
			}
		}))
	})
	// Benchmark/query-12         	   55064	     20678 ns/op	   15849 B/op	     100 allocs/op
	b.Run("query", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			collection := db.Collection("user")
			assert.Nil(b, collection.Set(ctx, doc))
			var docs []*wolverine.Document
			for i := 0; i < 100; i++ {
				docs = append(docs, testutil.NewUserDoc())
			}
			assert.Nil(b, collection.BatchSet(ctx, docs))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				results, err := collection.Query(ctx, wolverine.Query{
					Select: nil,
					Where: []wolverine.Where{
						{
							Field: "contact.email",
							Op:    "==",
							Value: doc.GetString("contact.email"),
						},
					},
					Page:    0,
					Limit:   10,
					OrderBy: wolverine.OrderBy{},
				})
				assert.Nil(b, err)
				assert.Equal(b, 1, len(results.Documents))
				assert.Equal(b, "contact.email", results.Stats.IndexMatch.Fields[0])
			}
		}))
	})
}

func TestAggregate(t *testing.T) {
	t.Run("sum basic", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			var usrs []*wolverine.Document
			ageSum := float64(0)
			for i := 0; i < 10; i++ {
				u := testutil.NewUserDoc()
				ageSum += u.GetFloat("age")
				usrs = append(usrs, u)
			}
			query := wolverine.AggregateQuery{
				GroupBy: []string{"account_id"},
				//Where:      []schema.Where{
				//	{
				//
				//	},
				//},
				Aggregates: []wolverine.Aggregate{
					{
						Field:    "age",
						Function: wolverine.SUM,
						Alias:    "age_sum",
					},
				},
				Page:    0,
				Limit:   0,
				OrderBy: wolverine.OrderBy{},
			}
			result, err := wolverine.ApplyReducers(ctx, query, usrs)
			assert.Nil(t, err)
			assert.Equal(t, ageSum, result.GetFloat("age_sum"))
		}))
	})
	t.Run("sum advanced", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
			collection := db.Collection("user")
			var usrs []*wolverine.Document

			for i := 0; i < 10; i++ {
				u := testutil.NewUserDoc()
				usrs = append(usrs, u)
			}
			assert.Nil(t, collection.BatchSet(ctx, usrs))
			query := wolverine.AggregateQuery{
				GroupBy: []string{"account_id"},
				//Where:      []schema.Where{
				//	{
				//
				//	},
				//},
				Aggregates: []wolverine.Aggregate{
					{
						Field:    "age",
						Function: wolverine.SUM,
						Alias:    "age_sum",
					},
				},
				Page:  0,
				Limit: 0,
				OrderBy: wolverine.OrderBy{
					Field:     "account_id",
					Direction: wolverine.ASC,
				},
			}
			groups := lo.GroupBy[*wolverine.Document](usrs, func(t *wolverine.Document) string {
				return t.GetString("account_id")
			})

			ageSum := map[string]float64{}
			for grup, value := range groups {
				result, err := wolverine.ApplyReducers(ctx, query, value)
				assert.Nil(t, err)
				ageSum[grup] += result.GetFloat("age")
			}
			results, err := collection.Aggregate(ctx, query)
			if err != nil {
				t.Fatal(err)
			}
			assert.NotEqual(t, 0, results.Count)
			var accounts []string
			for _, result := range results.Documents {
				accounts = append(accounts, result.GetString("account_id"))
				assert.Equal(t, ageSum[result.GetString("account_id")], result.GetFloat("age_sum"))
			}
		}))
	})
}

func TestDBCollection(t *testing.T) {
	assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
		collection := db.Collection("user")
		t.Run("schema", func(t *testing.T) {
			assert.NotNil(t, collection.Schema())
		})
		t.Run("db", func(t *testing.T) {
			assert.NotNil(t, collection.DB())
		})
		t.Run("schema primary query index", func(t *testing.T) {
			assert.NotNil(t, collection.Schema().PrimaryIndex())
		})
		t.Run("schema not empty", func(t *testing.T) {
			assert.NotEmpty(t, collection.Schema())
		})
		t.Run("schema name not empty", func(t *testing.T) {
			assert.NotEmpty(t, collection.Schema().Collection())
		})
	}))
}
