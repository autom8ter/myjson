package brutus_test

import (
	"context"
	"github.com/autom8ter/brutus"
	"github.com/autom8ter/brutus/testutil"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"runtime"
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
	t.Run("create", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *brutus.DB) {
			id, err := db.Create(ctx, "user", testutil.NewUserDoc())
			assert.Nil(t, err)
			u, err := db.Get(ctx, "user", id)
			assert.Nil(t, err)
			assert.Equal(t, id, u.GetString("_id"))
		}))
	})
	t.Run("set", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *brutus.DB) {
			timer := timer()
			defer timer(t)
			for i := 0; i < 10; i++ {
				assert.Nil(t, db.Set(ctx, "user", testutil.NewUserDoc()))
			}
		}))
	})
	assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *brutus.DB) {
		var usrs []*brutus.Document
		var ids []string
		t.Run("batch set", func(t *testing.T) {
			timer := timer()
			defer timer(t)
			for i := 0; i < 100; i++ {
				usr := testutil.NewUserDoc()
				ids = append(ids, usr.GetString("_id"))
				usrs = append(usrs, usr)
			}
			assert.Nil(t, db.BatchSet(ctx, "user", usrs))
		})
		t.Run("get each", func(t *testing.T) {
			timer := timer()
			defer timer(t)
			for _, u := range usrs {
				usr, err := db.Get(ctx, "user", u.GetString("_id"))
				if err != nil {
					t.Fatal(err)
				}
				assert.Equal(t, u.String(), usr.String())
			}
		})
		t.Run("query users account_id > 50", func(t *testing.T) {
			timer := timer()
			defer timer(t)
			results, err := db.Query(ctx, brutus.Query{
				From:   "user",
				Select: []string{"account_id"},
				Where: []brutus.Where{
					{
						Field: "account_id",
						Op:    ">",
						Value: 50,
					},
				},
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
			results, err := db.Query(ctx, brutus.Query{
				From:   "user",
				Select: []string{"account_id"},
				Where: []brutus.Where{
					{
						Field: "account_id",
						Op:    brutus.In,
						Value: []float64{51, 52, 53, 54, 55, 56, 57, 58, 59, 60},
					},
				},
				Page:    0,
				Limit:   10,
				OrderBy: brutus.OrderBy{},
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
			results, err := db.Query(ctx, brutus.Query{
				From:    "user",
				Select:  nil,
				Page:    0,
				Limit:   0,
				OrderBy: brutus.OrderBy{},
			})
			assert.Nil(t, err)
			assert.Equal(t, 100, len(results.Documents))
			t.Logf("found %v documents in %s", results.Count, results.Stats.ExecutionTime)
		})
		t.Run("paginate all", func(t *testing.T) {
			//timer := timer()
			//defer timer(t)
			//pageCount := 0
			//err := collection.QueryPaginate(ctx, brutus.Query{
			//	Page:    0,
			//	Limit:   10,
			//	OrderBy: brutus.OrderBy{},
			//}, func(page brutus.Page) bool {
			//	pageCount++
			//	return true
			//})
			//assert.Nil(t, err)
			//assert.Equal(t, 10, pageCount)
		})
		t.Run("update contact.email", func(t *testing.T) {
			for _, u := range usrs {
				id := u.GetString("_id")
				email := gofakeit.Email()
				assert.Nil(t, db.Update(ctx, "user", id, map[string]any{
					"contact.email": email,
				}))
				doc, err := db.Get(ctx, "user", id)
				assert.Nil(t, err)
				assert.Equal(t, email, doc.GetString("contact.email"))
				assert.Equal(t, u.GetString("name"), doc.GetString("name"))
			}
		})
		t.Run("delete first 50", func(t *testing.T) {
			for _, id := range ids[:50] {
				assert.Nil(t, db.Delete(ctx, "user", id))
			}
			for _, id := range ids[:50] {
				_, err := db.Get(ctx, "user", id)
				assert.NotNil(t, err)
			}
		})
		t.Run("query delete all", func(t *testing.T) {
			assert.Nil(t, db.QueryDelete(ctx, brutus.Query{
				From:    "user",
				Select:  nil,
				Page:    0,
				Limit:   0,
				OrderBy: brutus.OrderBy{},
			}))
			for _, id := range ids[50:] {
				d, err := db.Get(ctx, "user", id)
				assert.NotNil(t, err, d)
			}
		})
	}))
	time.Sleep(1 * time.Second)
	t.Log(runtime.NumGoroutine())
}

func Benchmark(b *testing.B) {
	// Benchmark/set-12         	    5662	    330875 ns/op	  288072 B/op	    2191 allocs/op
	b.Run("set", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *brutus.DB) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				assert.Nil(b, db.Set(ctx, "user", doc))
			}
		}))
	})
	// Benchmark/get-12         	   52730	     19125 ns/op	   13022 B/op	      98 allocs/op
	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *brutus.DB) {
			assert.Nil(b, db.Set(ctx, "user", doc))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := db.Get(ctx, "user", doc.GetString("_id"))
				assert.Nil(b, err)
			}
		}))
	})
	// Benchmark/query-12       	   44590	     25061 ns/op	   18920 B/op	     131 allocs/op
	b.Run("query with index", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *brutus.DB) {
			assert.Nil(b, db.Set(ctx, "user", doc))
			var docs []*brutus.Document
			for i := 0; i < 100000; i++ {
				docs = append(docs, testutil.NewUserDoc())
			}
			assert.Nil(b, db.BatchSet(ctx, "user", docs))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				results, err := db.Query(ctx, brutus.Query{
					From:   "user",
					Select: nil,
					Where: []brutus.Where{
						{
							Field: "contact.email",
							Op:    "==",
							Value: doc.GetString("contact.email"),
						},
					},
					Page:    0,
					Limit:   10,
					OrderBy: brutus.OrderBy{},
				})
				assert.Nil(b, err)
				assert.Equal(b, 1, len(results.Documents))
				assert.Equal(b, "contact.email", results.Stats.IndexMatch.MatchedFields[0])
			}
		}))
	})
	// Benchmark/query_without_index-12         	   10780	     98709 ns/op	   49977 B/op	     216 allocs/op
	b.Run("query without index", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *brutus.DB) {
			assert.Nil(b, db.Set(ctx, "user", doc))
			var docs []*brutus.Document
			for i := 0; i < 100000; i++ {
				docs = append(docs, testutil.NewUserDoc())
			}
			assert.Nil(b, db.BatchSet(ctx, "user", docs))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := db.Query(ctx, brutus.Query{
					From:   "user",
					Select: nil,
					Where: []brutus.Where{
						{
							Field: "name",
							Op:    brutus.Contains,
							Value: doc.GetString("John"),
						},
					},
					Page:    0,
					Limit:   10,
					OrderBy: brutus.OrderBy{},
				})
				assert.Nil(b, err)
			}
		}))
	})
}

func TestAggregate(t *testing.T) {
	t.Run("sum age", func(t *testing.T) {
		var expected = float64(0)
		var docs brutus.Documents
		for i := 0; i < 5; i++ {
			u := testutil.NewUserDoc()
			expected += u.GetFloat("age")
			docs = append(docs, u)
		}
		reduced, err := docs.Aggregate(context.Background(), []brutus.Aggregate{
			{
				Field:    "age",
				Function: "sum",
				Alias:    "age_sum",
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, expected, reduced.GetFloat("age_sum"))
	})
	t.Run("sum advanced", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *brutus.DB) {
			var usrs brutus.Documents
			ageSum := map[string]float64{}
			for i := 0; i < 10; i++ {
				u := testutil.NewUserDoc()
				ageSum[u.GetString("account_id")] += u.GetFloat("age")
				usrs = append(usrs, u)
			}
			assert.Nil(t, db.BatchSet(ctx, "user", usrs))
			query := brutus.AggregateQuery{
				From:    "user",
				GroupBy: []string{"account_id"},
				//Where:      []schema.Where{
				//	{
				//
				//	},
				//},
				Aggregates: []brutus.Aggregate{
					{
						Field:    "age",
						Function: brutus.SUM,
						Alias:    "age_sum",
					},
				},
				Page:  0,
				Limit: 0,
				OrderBy: brutus.OrderBy{
					Field:     "account_id",
					Direction: brutus.ASC,
				},
			}
			results, err := db.Aggregate(ctx, query)
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
