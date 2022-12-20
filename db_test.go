package gokvkit_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/autom8ter/gokvkit"

	"github.com/autom8ter/gokvkit/testutil"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
)

func timer() func(t *testing.T) {
	now := time.Now()
	return func(t *testing.T) {
		t.Logf("duration: %s", time.Since(now))
	}
}

func Test(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			var (
				id  string
				err error
			)
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				id, err = tx.Create(ctx, "user", testutil.NewUserDoc())
				_, err := tx.Get(ctx, "user", id)
				return err
			}))
			u, err := db.Get(ctx, "user", id)
			assert.Nil(t, err)
			assert.NotNil(t, u)
			assert.Equal(t, id, u.GetString("_id"))
		}))
	})
	t.Run("set", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			timer := timer()
			defer timer(t)
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				for i := 0; i < 10; i++ {
					assert.Nil(t, tx.Set(ctx, "user", testutil.NewUserDoc()))
				}
				return nil
			}))
		}))
	})
	assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
		var usrs []*gokvkit.Document
		var ids []string
		t.Run("set all", func(t *testing.T) {
			timer := timer()
			defer timer(t)

			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				for i := 0; i < 100; i++ {
					usr := testutil.NewUserDoc()
					ids = append(ids, usr.GetString("_id"))
					usrs = append(usrs, usr)
					assert.Nil(t, tx.Set(ctx, "user", usr))
				}
				return nil
			}))
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
			results, err := db.Query(ctx, "user", gokvkit.Query{
				Select: []gokvkit.Select{{Field: "account_id"}},
				Where: []gokvkit.Where{
					{
						Field: "account_id",
						Op:    gokvkit.WhereOpGt,
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
			results, err := db.Query(ctx, "user", gokvkit.Query{
				Select: []gokvkit.Select{{Field: "account_id"}},
				Where: []gokvkit.Where{
					{
						Field: "account_id",
						Op:    gokvkit.WhereOpIn,
						Value: []float64{51, 52, 53, 54, 55, 56, 57, 58, 59, 60},
					},
				},
				Limit: 10,
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
			results, err := db.Query(ctx, "user", gokvkit.Query{
				Select: []gokvkit.Select{{Field: "*"}},
			})
			assert.Nil(t, err)
			assert.Equal(t, 100, len(results.Documents))
			t.Logf("found %v documents in %s", results.Count, results.Stats.ExecutionTime)
		})
		t.Run("update contact.email", func(t *testing.T) {
			for _, u := range usrs {
				id := u.GetString("_id")
				email := gofakeit.Email()
				assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
					assert.Nil(t, tx.Update(ctx, "user", id, map[string]any{
						"contact.email": email,
					}))
					return nil
				}))
				doc, err := db.Get(ctx, "user", id)
				assert.Nil(t, err)
				assert.Equal(t, email, doc.GetString("contact.email"))
				assert.Equal(t, u.GetString("name"), doc.GetString("name"))
			}
		})
		t.Run("delete first 50", func(t *testing.T) {
			for _, id := range ids[:50] {
				assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
					assert.Nil(t, tx.Delete(ctx, "user", id))
					return nil
				}))

			}
			for _, id := range ids[:50] {
				_, err := db.Get(ctx, "user", id)
				assert.NotNil(t, err)
			}
		})
		t.Run("query delete all", func(t *testing.T) {
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				res, err := db.Query(ctx, "user", gokvkit.Query{
					Select: []gokvkit.Select{{Field: "*"}},
				})
				if err != nil {
					return err
				}
				for _, res := range res.Documents {
					if err := tx.Delete(ctx, "user", res.GetString("_id")); err != nil {
						return err
					}
				}
				return nil
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
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				assert.Nil(b, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
					return tx.Set(ctx, "user", doc)
				}))
			}
		}))
	})
	// Benchmark/get-12         	   52730	     19125 ns/op	   13022 B/op	      98 allocs/op
	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			assert.Nil(b, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				return tx.Set(ctx, "user", doc)
			}))
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
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			assert.Nil(b, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				return tx.Set(ctx, "user", doc)
			}))
			var docs []*gokvkit.Document
			assert.Nil(b, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				for i := 0; i < 100000; i++ {
					usr := testutil.NewUserDoc()
					docs = append(docs, usr)
					if err := tx.Set(ctx, "user", usr); err != nil {
						return err
					}
				}
				return nil
			}))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				results, err := db.Query(ctx, "user", gokvkit.Query{
					Select: []gokvkit.Select{{Field: "*"}},
					Where: []gokvkit.Where{
						{
							Field: "contact.email",
							Op:    gokvkit.WhereOpEq,
							Value: doc.GetString("contact.email"),
						},
					},
					Limit: 10,
				})
				assert.Nil(b, err)
				assert.Equal(b, 1, len(results.Documents))
				assert.Equal(b, "contact.email", results.Stats.Optimization.MatchedFields[0])
			}
		}))
	})
	// Benchmark/query_without_index-12         	   10780	     98709 ns/op	   49977 B/op	     216 allocs/op
	b.Run("query without index", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			assert.Nil(b, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				return tx.Set(ctx, "user", doc)
			}))
			var docs []*gokvkit.Document
			assert.Nil(b, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				for i := 0; i < 100000; i++ {
					usr := testutil.NewUserDoc()
					docs = append(docs, usr)
					if err := tx.Set(ctx, "user", usr); err != nil {
						return err
					}
				}
				return nil
			}))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := db.Query(ctx, "user", gokvkit.Query{
					Select: []gokvkit.Select{{Field: "*"}},
					Where: []gokvkit.Where{
						{
							Field: "name",
							Op:    gokvkit.WhereOpContains,
							Value: doc.GetString("John"),
						},
					},
					Limit: 10,
				})
				assert.Nil(b, err)
			}
		}))
	})
}

func TestIndexing1(t *testing.T) {
	t.Run("matching unique index (contact.email)", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			var docs gokvkit.Documents
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				for i := 0; i < 5; i++ {
					usr := testutil.NewUserDoc()
					docs = append(docs, usr)
					if err := tx.Set(ctx, "user", usr); err != nil {
						return err
					}
				}
				return nil
			}))
			page, err := db.Query(ctx, "user", gokvkit.Query{
				Select: []gokvkit.Select{
					{
						Field: "contact.email",
					},
				},
				Where: []gokvkit.Where{
					{
						Field: "contact.email",
						Op:    gokvkit.WhereOpEq,
						Value: docs[0].Get("contact.email"),
					},
				},
			})
			assert.Nil(t, err)
			assert.Equal(t, 1, page.Count)
			assert.Equal(t, page.Documents[0].Get("contact.email"), docs[0].Get("contact.email"))
			assert.Equal(t, "contact.email", page.Stats.Optimization.MatchedFields[0])
			assert.Equal(t, false, page.Stats.Optimization.Index.Primary)
		}))
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			var docs gokvkit.Documents
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				for i := 0; i < 5; i++ {
					usr := testutil.NewUserDoc()
					docs = append(docs, usr)
					if err := tx.Set(ctx, "user", usr); err != nil {
						return err
					}
				}
				return nil
			}))
			page, err := db.Query(ctx, "user", gokvkit.Query{

				Select: []gokvkit.Select{
					{
						Field: "name",
					},
				},
				Where: []gokvkit.Where{
					{
						Field: "contact.email",
						Op:    gokvkit.WhereOpEq,
						Value: docs[0].Get("contact.email"),
					},
				},
			})
			assert.Nil(t, err)
			assert.Equal(t, 1, page.Count)
			assert.Equal(t, "contact.email", page.Stats.Optimization.MatchedFields[0])

			assert.Equal(t, false, page.Stats.Optimization.Index.Primary)
		}))
	})
	t.Run("non-matching (name)", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			var docs gokvkit.Documents
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				for i := 0; i < 5; i++ {
					usr := testutil.NewUserDoc()
					docs = append(docs, usr)
					if err := tx.Set(ctx, "user", usr); err != nil {
						return err
					}
				}
				return nil
			}))
			page, err := db.Query(ctx, "user", gokvkit.Query{

				Select: []gokvkit.Select{
					{
						Field: "name",
					},
				},
				Where: []gokvkit.Where{
					{
						Field: "name",
						Op:    gokvkit.WhereOpContains,
						Value: docs[0].Get("name"),
					},
				},
			})
			assert.Nil(t, err)
			assert.Equal(t, 1, page.Count)
			assert.Equal(t, page.Documents[0].Get("name"), docs[0].Get("name"))
			assert.Equal(t, []string{}, page.Stats.Optimization.MatchedFields)

			assert.Equal(t, true, page.Stats.Optimization.Index.Primary)
		}))
	})
	t.Run("matching primary (_id)", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			var docs gokvkit.Documents
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				for i := 0; i < 5; i++ {
					usr := testutil.NewUserDoc()
					docs = append(docs, usr)
					if err := tx.Set(ctx, "user", usr); err != nil {
						return err
					}
				}
				return nil
			}))
			page, err := db.Query(ctx, "user", gokvkit.Query{

				Select: []gokvkit.Select{
					{
						Field: "_id",
					},
				},
				Where: []gokvkit.Where{
					{
						Field: "_id",
						Op:    gokvkit.WhereOpEq,
						Value: docs[0].Get("_id"),
					},
				},
			})
			assert.Nil(t, err)
			assert.Equal(t, 1, page.Count)
			assert.Equal(t, page.Documents[0].Get("_id"), docs[0].Get("_id"))
			assert.Equal(t, []string{"_id"}, page.Stats.Optimization.MatchedFields)

			assert.Equal(t, true, page.Stats.Optimization.Index.Primary)
		}))
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			var docs gokvkit.Documents
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				for i := 0; i < 5; i++ {
					usr := testutil.NewUserDoc()
					docs = append(docs, usr)
					if err := tx.Set(ctx, "user", usr); err != nil {
						return err
					}
				}
				return nil
			}))
			page, err := db.Query(ctx, "user", gokvkit.Query{

				Select: []gokvkit.Select{
					{
						Field: "_id",
					},
				},
				Where: []gokvkit.Where{
					{
						Field: "_id",
						Op:    gokvkit.WhereOpContains,
						Value: docs[0].Get("_id"),
					},
				},
			})
			assert.Nil(t, err)
			assert.Equal(t, 1, page.Count)
			assert.Equal(t, page.Documents[0].Get("_id"), docs[0].Get("_id"))
			assert.Equal(t, []string{}, page.Stats.Optimization.MatchedFields)
			assert.Equal(t, true, page.Stats.Optimization.Index.Primary)
		}))
	})
}

func TestAggregate(t *testing.T) {

	t.Run("sum advanced", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			var usrs gokvkit.Documents
			ageSum := map[string]float64{}
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				for i := 0; i < 10; i++ {
					u := testutil.NewUserDoc()
					ageSum[u.GetString("account_id")] += u.GetFloat("age")
					usrs = append(usrs, u)
					assert.Nil(t, tx.Set(ctx, "user", u))
				}
				return nil
			}))

			query := gokvkit.Query{
				GroupBy: []string{"account_id"},
				//Where:      []schema.Where{
				//	{
				//
				//	},
				//},
				Select: []gokvkit.Select{
					{
						Field: "account_id",
					},
					{
						Field:     "age",
						Aggregate: gokvkit.SelectAggregateSum,
						As:        "age_sum",
					},
				},
				OrderBy: []gokvkit.OrderBy{
					{
						Field:     "account_id",
						Direction: gokvkit.OrderByDirectionAsc,
					},
				},
			}
			results, err := db.Query(ctx, "user", query)
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
