package myjson_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/testutil"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/sjson"
)

func timer() func(t *testing.T) {
	now := time.Now()
	return func(t *testing.T) {
		t.Logf("duration: %s", time.Since(now))
	}
}

func Test(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var (
				id  string
				err error
			)
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				id, err = tx.Create(ctx, "user", testutil.NewUserDoc())
				assert.NoError(t, err)
				_, err := tx.Get(ctx, "user", id)
				return err
			}))
			u, err := db.Get(ctx, "user", id)
			assert.NoError(t, err)

			assert.NotNil(t, u)
			assert.Less(t, time.Now().UTC().Sub(u.GetTime("timestamp")), 1*time.Second)
			assert.Equal(t, id, u.GetString("_id"))
		}))
	})
	t.Run("create then set", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var (
				id  string
				err error
			)
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				id, err = tx.Create(ctx, "account", myjson.D().Set(map[string]any{
					"name": gofakeit.Company(),
				}).Doc())
				assert.NoError(t, err)
				_, err := tx.Get(ctx, "account", id)
				return err
			}))
			u, err := db.Get(ctx, "account", id)
			assert.NoError(t, err)
			assert.NotNil(t, u)
			assert.Equal(t, id, u.GetString("_id"))
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				err = tx.Set(ctx, "account", u)
				return err
			}))
		}))
	})
	t.Run("batch create", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var (
				id  string
				err error
			)
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false, IsBatch: true}, func(ctx context.Context, tx myjson.Tx) error {
				id, err = tx.Create(ctx, "user", testutil.NewUserDoc())
				assert.NoError(t, err)
				return nil
			}))
			u, err := db.Get(ctx, "user", id)
			assert.NoError(t, err)

			assert.NotNil(t, u)
			assert.Equal(t, id, u.GetString("_id"))
		}))
	})
	t.Run("create & stream", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			wg := sync.WaitGroup{}
			wg.Add(1)
			var received = make(chan struct{}, 1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				err := db.ChangeStream(ctx, "user", nil, func(ctx context.Context, cdc myjson.CDC) (bool, error) {
					received <- struct{}{}
					return true, nil
				})
				assert.NoError(t, err)
			}()
			var (
				id  string
				err error
			)
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				id, err = tx.Create(ctx, "user", testutil.NewUserDoc())
				assert.NoError(t, err)
				_, err := tx.Get(ctx, "user", id)
				assert.NoError(t, err)
				return err
			}))
			u, err := db.Get(ctx, "user", id)
			assert.NoError(t, err)
			assert.NotNil(t, u)
			assert.Equal(t, id, u.GetString("_id"))
			<-received
		}))
	})
	t.Run("set", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			timer := timer()
			defer timer(t)
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 10; i++ {
					assert.Nil(t, tx.Set(ctx, "user", testutil.NewUserDoc()))
				}
				return nil
			}))
		}))
	})
	assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
		var usrs []*myjson.Document
		var ids []string
		t.Run("set all", func(t *testing.T) {
			timer := timer()
			defer timer(t)

			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
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
			results, err := db.Query(ctx, "user", myjson.Query{
				Select: []myjson.Select{{Field: "account_id"}},
				Where: []myjson.Where{
					{
						Field: "account_id",
						Op:    myjson.WhereOpGt,
						Value: 50,
					},
				},
			})
			assert.NoError(t, err)
			assert.Greater(t, len(results.Documents), 1)
			for _, result := range results.Documents {
				assert.Greater(t, result.GetFloat("account_id"), float64(50))
			}
			t.Logf("found %v documents in %s", results.Count, results.Stats.ExecutionTime)
		})
		t.Run("query users account_id in 51-60", func(t *testing.T) {
			timer := timer()
			defer timer(t)
			results, err := db.Query(ctx, "user", myjson.Query{
				Select: []myjson.Select{{Field: "account_id"}},
				Where: []myjson.Where{
					{
						Field: "account_id",
						Op:    myjson.WhereOpIn,
						Value: []string{"51", "52", "53", "54", "55", "56", "57", "58", "59", "60"},
					},
				},
				Limit: 10,
			})
			assert.NoError(t, err)
			assert.Greater(t, len(results.Documents), 1)
			for _, result := range results.Documents {
				assert.Greater(t, result.GetFloat("account_id"), float64(50))
			}
			t.Logf("found %v documents in %s", results.Count, results.Stats.ExecutionTime)
		})
		t.Run("query all", func(t *testing.T) {
			timer := timer()
			defer timer(t)
			results, err := db.Query(ctx, "user", myjson.Query{
				Select: []myjson.Select{{Field: "*"}},
			})
			assert.NoError(t, err)
			assert.Equal(t, 100, len(results.Documents))
			t.Logf("found %v documents in %s", results.Count, results.Stats.ExecutionTime)
		})
		t.Run("update contact.email", func(t *testing.T) {
			for _, u := range usrs {
				id := u.GetString("_id")
				email := gofakeit.Email()
				assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
					assert.Nil(t, tx.Update(ctx, "user", id, map[string]any{
						"contact.email": email,
					}))
					return nil
				}))
				doc, err := db.Get(ctx, "user", id)
				assert.NoError(t, err)
				assert.Equal(t, email, doc.GetString("contact.email"))
				assert.Equal(t, u.GetString("name"), doc.GetString("name"))
			}
		})
		t.Run("delete first 50", func(t *testing.T) {
			for _, id := range ids[:50] {
				assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
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
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				res, err := db.Query(ctx, "user", myjson.Query{
					Select: []myjson.Select{{Field: "*"}},
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
}

func Benchmark(b *testing.B) {
	// Benchmark/set-12         	    5662	    330875 ns/op	  288072 B/op	    2191 allocs/op
	b.Run("set", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				assert.Nil(b, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
					return tx.Set(ctx, "user", doc)
				}))
			}
		}))
	})
	// Benchmark/get-12         	   52730	     19125 ns/op	   13022 B/op	      98 allocs/op
	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(b, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
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
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(b, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				return tx.Set(ctx, "user", doc)
			}))
			var docs []*myjson.Document
			assert.Nil(b, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
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
				results, err := db.Query(ctx, "user", myjson.Query{
					Select: []myjson.Select{{Field: "*"}},
					Where: []myjson.Where{
						{
							Field: "contact.email",
							Op:    myjson.WhereOpEq,
							Value: doc.GetString("contact.email"),
						},
					},
					Limit: 10,
				})
				assert.Nil(b, err)
				assert.Equal(b, 1, len(results.Documents))
				assert.Equal(b, "contact.email", results.Stats.Explain.MatchedFields[0])
			}
		}))
	})
	// Benchmark/query_without_index-12         	   10780	     98709 ns/op	   49977 B/op	     216 allocs/op
	b.Run("query without index", func(b *testing.B) {
		b.ReportAllocs()
		doc := testutil.NewUserDoc()
		assert.Nil(b, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(b, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				return tx.Set(ctx, "user", doc)
			}))
			var docs []*myjson.Document
			assert.Nil(b, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
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
				_, err := db.Query(ctx, "user", myjson.Query{
					Select: []myjson.Select{{Field: "*"}},
					Where: []myjson.Where{
						{
							Field: "name",
							Op:    myjson.WhereOpContains,
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

func TestImmutable(t *testing.T) {
	t.Run("update immutable property", func(t *testing.T) {
		var (
			currentName string
		)
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				current, err := tx.Get(ctx, "account", "1")
				if err != nil {
					return err
				}
				currentName = current.GetString("name")
				return tx.Update(ctx, "account", "1", map[string]any{
					"name": gofakeit.Company(),
				})
			}))
			now, err := db.Get(ctx, "account", "1")
			assert.NoError(t, err)
			assert.Equal(t, currentName, now.GetString("name"))
		}))
	})
	t.Run("set immutable property", func(t *testing.T) {
		var (
			currentName string
		)
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				current, err := tx.Get(ctx, "account", "1")
				if err != nil {
					return err
				}
				currentName = current.GetString("name")
				assert.NoError(t, current.Set("name", gofakeit.Company()))
				return tx.Set(ctx, "account", current)
			}))
			now, err := db.Get(ctx, "account", "1")
			assert.NoError(t, err)
			assert.Equal(t, currentName, now.GetString("name"))
		}))
	})
}

func TestComputedProperty(t *testing.T) {
	start := time.Now().UnixMilli()
	t.Run("check computed property", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				current, err := tx.Get(ctx, "account", "1")
				if err != nil {
					return err
				}
				assert.Greater(t, current.GetFloat("created_at"), float64(start))
				assert.NoError(t, tx.Set(ctx, "account", current.Clone()))
				updated, err := tx.Get(ctx, "account", "1")
				if err != nil {
					return err
				}
				assert.Equal(t, current.GetFloat("created_at"), updated.GetFloat("created_at"))
				return nil
			}))
		}))
	})
}

func TestDefaultProperty(t *testing.T) {
	t.Run("check default property", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				current, err := tx.Get(ctx, "account", "1")
				if err != nil {
					return err
				}
				assert.Equal(t, "inactive", current.Get("status"))
				assert.NoError(t, current.Del("status"))
				assert.NoError(t, tx.Set(ctx, "account", current))
				updated, err := tx.Get(ctx, "account", "1")
				if err != nil {
					return err
				}
				assert.Equal(t, "inactive", updated.Get("status"))
				return nil
			}))
		}))
	})
}

func TestIndexing1(t *testing.T) {
	t.Run("matching unique index (contact.email)", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var docs myjson.Documents
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 5; i++ {
					usr := testutil.NewUserDoc()
					docs = append(docs, usr)
					if err := tx.Set(ctx, "user", usr); err != nil {
						return err
					}
				}
				return nil
			}))
			page, err := db.Query(ctx, "user", myjson.Query{
				Select: []myjson.Select{
					{
						Field: "contact.email",
					},
				},
				Where: []myjson.Where{
					{
						Field: "contact.email",
						Op:    myjson.WhereOpEq,
						Value: docs[0].Get("contact.email"),
					},
				},
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, page.Count)
			assert.Equal(t, page.Documents[0].Get("contact.email"), docs[0].Get("contact.email"))
			assert.Equal(t, "contact.email", page.Stats.Explain.MatchedFields[0])
			assert.Equal(t, false, page.Stats.Explain.Index.Primary)
		}))
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var docs myjson.Documents
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 5; i++ {
					usr := testutil.NewUserDoc()
					docs = append(docs, usr)
					if err := tx.Set(ctx, "user", usr); err != nil {
						return err
					}
				}
				return nil
			}))
			page, err := db.Query(ctx, "user", myjson.Query{

				Select: []myjson.Select{
					{
						Field: "name",
					},
				},
				Where: []myjson.Where{
					{
						Field: "contact.email",
						Op:    myjson.WhereOpEq,
						Value: docs[0].Get("contact.email"),
					},
				},
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, page.Count)
			assert.Equal(t, "contact.email", page.Stats.Explain.MatchedFields[0])

			assert.Equal(t, false, page.Stats.Explain.Index.Primary)
		}))
	})
	t.Run("non-matching (name)", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var docs myjson.Documents
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 5; i++ {
					usr := testutil.NewUserDoc()
					docs = append(docs, usr)
					if err := tx.Set(ctx, "user", usr); err != nil {
						return err
					}
				}
				return nil
			}))
			page, err := db.Query(ctx, "user", myjson.Query{

				Select: []myjson.Select{
					{
						Field: "name",
					},
				},
				Where: []myjson.Where{
					{
						Field: "name",
						Op:    myjson.WhereOpContains,
						Value: docs[0].Get("name"),
					},
				},
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, page.Count)
			assert.Equal(t, page.Documents[0].Get("name"), docs[0].Get("name"))
			assert.Equal(t, []string{}, page.Stats.Explain.MatchedFields)

			assert.Equal(t, true, page.Stats.Explain.Index.Primary)
		}))
	})
	t.Run("matching primary (_id)", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var docs myjson.Documents
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 5; i++ {
					usr := testutil.NewUserDoc()
					docs = append(docs, usr)
					if err := tx.Set(ctx, "user", usr); err != nil {
						return err
					}
				}
				return nil
			}))
			page, err := db.Query(ctx, "user", myjson.Query{

				Select: []myjson.Select{
					{
						Field: "_id",
					},
				},
				Where: []myjson.Where{
					{
						Field: "_id",
						Op:    myjson.WhereOpEq,
						Value: docs[0].Get("_id"),
					},
				},
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, page.Count)
			assert.Equal(t, page.Documents[0].Get("_id"), docs[0].Get("_id"))
			assert.Equal(t, []string{"_id"}, page.Stats.Explain.MatchedFields)

			assert.Equal(t, true, page.Stats.Explain.Index.Primary)
		}))
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var docs myjson.Documents
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 5; i++ {
					usr := testutil.NewUserDoc()
					docs = append(docs, usr)
					if err := tx.Set(ctx, "user", usr); err != nil {
						return err
					}
				}
				return nil
			}))
			page, err := db.Query(ctx, "user", myjson.Query{

				Select: []myjson.Select{
					{
						Field: "_id",
					},
				},
				Where: []myjson.Where{
					{
						Field: "_id",
						Op:    myjson.WhereOpContains,
						Value: docs[0].Get("_id"),
					},
				},
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, page.Count)
			assert.Equal(t, page.Documents[0].Get("_id"), docs[0].Get("_id"))
			assert.Equal(t, []string{}, page.Stats.Explain.MatchedFields)
			assert.Equal(t, true, page.Stats.Explain.Index.Primary)
		}))
	})
	t.Run("cdc queries", func(t *testing.T) {
		t.Run("no results (>)", func(t *testing.T) {
			assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
				var docs myjson.Documents
				assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
					for i := 0; i < 5; i++ {
						usr := testutil.NewUserDoc()
						docs = append(docs, usr)
						if err := tx.Set(ctx, "user", usr); err != nil {
							return err
						}
					}
					return nil
				}))
				count := 0
				now := time.Now().UnixNano()
				o, err := db.ForEach(ctx, "system_cdc", myjson.ForEachOpts{
					Where: []myjson.Where{{
						Field: "timestamp",
						Op:    myjson.WhereOpGt,
						Value: now,
					}},
				}, func(d *myjson.Document) (bool, error) {
					assert.Greater(t, d.GetFloat("timestamp"), float64(now))
					count++
					return true, nil
				})
				assert.NoError(t, err)
				assert.Equal(t, false, o.Index.Primary)
				assert.EqualValues(t, now, o.SeekValues["timestamp"])
				assert.False(t, o.Reverse)
				assert.Equal(t, "timestamp", o.SeekFields[0])
				assert.Equal(t, 0, count)
			}))
		})
		t.Run("all results (>)", func(t *testing.T) {
			assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
				var docs myjson.Documents
				assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
					for i := 0; i < 5; i++ {
						usr := testutil.NewUserDoc()
						docs = append(docs, usr)
						if err := tx.Set(ctx, "user", usr); err != nil {
							return err
						}
					}
					return nil
				}))
				count := 0
				now := time.Now().Truncate(5 * time.Minute).UnixNano()
				o, err := db.ForEach(ctx, "system_cdc", myjson.ForEachOpts{
					Where: []myjson.Where{{
						Field: "timestamp",
						Op:    myjson.WhereOpGt,
						Value: now,
					}},
				}, func(d *myjson.Document) (bool, error) {
					assert.Greater(t, d.GetFloat("timestamp"), float64(now))
					count++
					return true, nil
				})
				assert.NoError(t, err)
				assert.Equal(t, false, o.Index.Primary)
				assert.EqualValues(t, now, o.SeekValues["timestamp"])
				assert.False(t, o.Reverse)
				assert.Equal(t, "timestamp", o.SeekFields[0])
				assert.NotEqual(t, 0, count)
			}))
		})
		t.Run("all results (<)", func(t *testing.T) {
			assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
				var docs myjson.Documents
				assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
					for i := 0; i < 5; i++ {
						usr := testutil.NewUserDoc()
						docs = append(docs, usr)
						if err := tx.Set(ctx, "user", usr); err != nil {
							return err
						}
					}
					return nil
				}))
				count := 0
				now := time.Now().Add(5 * time.Minute).UnixNano()
				o, err := db.ForEach(ctx, "system_cdc", myjson.ForEachOpts{
					Where: []myjson.Where{{
						Field: "timestamp",
						Op:    myjson.WhereOpLt,
						Value: now,
					}},
				}, func(d *myjson.Document) (bool, error) {
					assert.Less(t, d.GetFloat("timestamp"), float64(now))
					count++
					return true, nil
				})
				assert.NoError(t, err)
				assert.Equal(t, false, o.Index.Primary)
				assert.EqualValues(t, now, o.SeekValues["timestamp"])
				assert.True(t, o.Reverse)
				assert.Equal(t, "timestamp", o.SeekFields[0])
				assert.NotEqual(t, 0, count)
			}))
		})

		t.Run("no results (>)", func(t *testing.T) {
			assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
				var docs myjson.Documents
				assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
					for i := 0; i < 5; i++ {
						usr := testutil.NewUserDoc()
						docs = append(docs, usr)
						if err := tx.Set(ctx, "user", usr); err != nil {
							return err
						}
					}
					return nil
				}))
				count := 0
				now := time.Now().UnixNano()
				o, err := db.ForEach(ctx, "system_cdc", myjson.ForEachOpts{
					Where: []myjson.Where{{
						Field: "timestamp",
						Op:    myjson.WhereOpGt,
						Value: now,
					}},
				}, func(d *myjson.Document) (bool, error) {
					assert.Greater(t, d.GetFloat("timestamp"), float64(now))
					count++
					return true, nil
				})
				assert.NoError(t, err)
				assert.Equal(t, false, o.Index.Primary)
				assert.EqualValues(t, now, o.SeekValues["timestamp"])
				assert.False(t, o.Reverse)
				assert.Equal(t, "timestamp", o.SeekFields[0])
				assert.Equal(t, 0, count)
			}))
		})
		t.Run("no results (<)", func(t *testing.T) {
			assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
				var docs myjson.Documents
				assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
					for i := 0; i < 5; i++ {
						usr := testutil.NewUserDoc()
						docs = append(docs, usr)
						if err := tx.Set(ctx, "user", usr); err != nil {
							return err
						}
					}
					return nil
				}))
				count := 0
				now := time.Now().Truncate(15 * time.Minute).UnixNano()
				o, err := db.ForEach(ctx, "system_cdc", myjson.ForEachOpts{
					Where: []myjson.Where{{
						Field: "timestamp",
						Op:    myjson.WhereOpLt,
						Value: now,
					}},
				}, func(d *myjson.Document) (bool, error) {
					assert.Less(t, d.GetFloat("timestamp"), float64(now))
					count++
					return true, nil
				})
				assert.NoError(t, err)
				assert.Equal(t, false, o.Index.Primary)
				assert.EqualValues(t, now, o.SeekValues["timestamp"])
				assert.True(t, o.Reverse)
				assert.Equal(t, "timestamp", o.SeekFields[0])
				assert.Equal(t, 0, count)
			}))
		})
	})

}

func TestOrderBy(t *testing.T) {
	t.Run("basic asc/desc", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var usrs []*myjson.Document
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 10; i++ {
					u := testutil.NewUserDoc()
					assert.NoError(t, u.Set("age", i))
					usrs = append(usrs, u)
					assert.NoError(t, tx.Set(ctx, "user", u))
				}
				return nil
			}))
			{
				results, err := db.Query(ctx, "user", myjson.Q().
					Select(myjson.Select{Field: "*"}).
					OrderBy(myjson.OrderBy{Field: "age", Direction: myjson.OrderByDirectionAsc}).
					Query())
				assert.NoError(t, err)
				for i, d := range results.Documents {
					assert.Equal(t, usrs[i].Get("age"), d.Get("age"))
				}
			}
			{
				results, err := db.Query(ctx, "user", myjson.Q().
					Select(myjson.Select{Field: "*"}).
					OrderBy(myjson.OrderBy{Field: "age", Direction: myjson.OrderByDirectionDesc}).
					Query())
				assert.NoError(t, err)
				for i, d := range results.Documents {
					assert.Equal(t, usrs[len(usrs)-i-1].Get("age"), d.Get("age"))
				}
			}
		}))
	})
}

func TestPagination(t *testing.T) {
	t.Run("order by asc + pagination", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var usrs []*myjson.Document
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 10; i++ {
					u := testutil.NewUserDoc()
					assert.NoError(t, u.Set("age", i))
					usrs = append(usrs, u)
					assert.NoError(t, tx.Set(ctx, "user", u))
				}
				return nil
			}))
			for i := 0; i < 10; i++ {
				results, err := db.Query(ctx, "user", myjson.Q().
					OrderBy(myjson.OrderBy{Field: "age", Direction: myjson.OrderByDirectionAsc}).
					Page(i).
					Limit(1).
					Query())
				assert.NoError(t, err)
				assert.Equal(t, 1, results.Count)
				assert.Equal(t, usrs[i].Get("age"), results.Documents[0].Get("age"))
			}
		}))
	})
	t.Run("order by desc + pagination", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var usrs []*myjson.Document
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 10; i++ {
					u := testutil.NewUserDoc()
					assert.NoError(t, u.Set("age", i))
					usrs = append(usrs, u)
					assert.NoError(t, tx.Set(ctx, "user", u))
				}
				return nil
			}))
			for i := 0; i < 10; i++ {
				results, err := db.Query(ctx, "user", myjson.Q().
					OrderBy(myjson.OrderBy{Field: "age", Direction: myjson.OrderByDirectionDesc}).
					Page(i).
					Limit(1).
					Query())
				assert.NoError(t, err)
				assert.Equal(t, 1, results.Count)
				assert.Equal(t, usrs[len(usrs)-i-1].Get("age"), results.Documents[0].Get("age"))
			}
		}))
	})
	t.Run("order by desc + where + pagination", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var usrs []*myjson.Document
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 10; i++ {
					u := testutil.NewUserDoc()
					assert.NoError(t, u.Set("age", i))
					usrs = append(usrs, u)
					assert.NoError(t, tx.Set(ctx, "user", u))
				}
				return nil
			}))
			for i := 0; i < 10; i++ {
				results, err := db.Query(ctx, "user", myjson.Q().
					Where(myjson.Where{Field: "age", Op: myjson.WhereOpGte, Value: 5}).
					OrderBy(myjson.OrderBy{Field: "age", Direction: myjson.OrderByDirectionDesc}).
					Page(i).
					Limit(1).
					Query())
				assert.NoError(t, err)
				if i < 5 {
					assert.Equal(t, 1, results.Count)
					assert.Equal(t, usrs[len(usrs)-i-1].Get("age"), results.Documents[0].Get("age"))
				}
			}
		}))
	})
}

func TestAggregate(t *testing.T) {
	t.Run("sum advanced", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var usrs myjson.Documents
			ageSum := map[string]float64{}
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 10; i++ {
					u := testutil.NewUserDoc()
					ageSum[u.GetString("account_id")] += u.GetFloat("age")
					usrs = append(usrs, u)
					assert.Nil(t, tx.Set(ctx, "user", u))
				}
				return nil
			}))
			query := myjson.Query{
				Select: []myjson.Select{
					{
						Field: "account_id",
					},
					{
						Field:     "age",
						Aggregate: myjson.AggregateFunctionSum,
						As:        "age_sum",
					},
				},
				GroupBy: []string{"account_id"},
				OrderBy: []myjson.OrderBy{
					{
						Field:     "account_id",
						Direction: myjson.OrderByDirectionAsc,
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

func TestScript(t *testing.T) {
	t.Run("getAccount", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			getAccountScript := `
let res = db.Get(ctx, 'account', params.id)
res.Get('_id')
 `
			results, err := db.RunScript(ctx, getAccountScript, map[string]any{
				"id": "1",
			})
			assert.NoError(t, err)
			assert.Equal(t, "1", results)
		}))
	})
	t.Run("getAccounts", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			getAccountScript := `
let res = db.Query(ctx, 'account', {Select: [{Field: '*'}]})
res.documents
 `
			results, err := db.RunScript(ctx, getAccountScript, map[string]any{})
			assert.NoError(t, err)
			assert.Equal(t, 101, len(results.(myjson.Documents)))
		}))
	})
	t.Run("setAccount", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			getAccountScript := `
db.Tx(ctx, {IsReadOnly: false}, (ctx, tx) => {
	tx.Set(ctx, "account", params.doc)
})
 `
			id := ksuid.New().String()
			doc, err := myjson.NewDocumentFrom(map[string]any{
				"_id":  id,
				"name": gofakeit.Company(),
			})
			_, err = db.RunScript(ctx, getAccountScript, map[string]any{
				"doc": doc,
			})
			assert.NoError(t, err)
			val, err := db.Get(ctx, "account", id)
			assert.NoError(t, err)
			assert.NotEmpty(t, val)
		}))
	})
	t.Run("forEachAccount", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			getAccountScript := `
db.ForEach(ctx, 'account', undefined, params.fn)
 `
			count := 0
			_, err := db.RunScript(ctx, getAccountScript, map[string]any{
				"fn": myjson.ForEachFunc(func(d *myjson.Document) (bool, error) {
					count++
					return true, nil
				}),
			})
			assert.NoError(t, err)
			assert.Equal(t, 101, count)
		}))
	})
}

func TestJoin(t *testing.T) {
	t.Run("join user to account", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var usrs = map[string]*myjson.Document{}
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 100; i++ {
					u := testutil.NewUserDoc()
					usrs[u.GetString("_id")] = u
					assert.NoError(t, tx.Set(ctx, "user", u))
				}
				return nil
			}))
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				assert.NoError(t, tx.Set(ctx, "user", testutil.NewUserDoc()))
				return nil
			}))
			{
				results, err := db.Query(ctx, "user", myjson.Q().
					Select(
						myjson.Select{Field: "acc._id", As: "account_id"},
						myjson.Select{Field: "acc.name", As: "account_name"},
						myjson.Select{Field: "_id", As: "user_id"},
					).
					Join(myjson.Join{
						Collection: "account",
						On: []myjson.Where{
							{
								Field: "_id",
								Op:    myjson.WhereOpEq,
								Value: "$account_id",
							},
						},
						As: "acc",
					}).
					Query())
				assert.NoError(t, err)

				for _, r := range results.Documents {
					assert.True(t, r.Exists("account_name"))
					assert.True(t, r.Exists("account_id"))
					assert.True(t, r.Exists("user_id"))
					if usrs[r.GetString("user_id")] != nil {
						assert.NotEmpty(t, usrs[r.GetString("user_id")])
						assert.Equal(t, usrs[r.GetString("user_id")].Get("account_id"), r.GetString("account_id"))
					}
				}
			}
			{
				results, err := db.Query(ctx, "user", myjson.Q().
					Select(
						myjson.Select{Field: "acc._id", As: "account_id"},
						myjson.Select{Field: "acc.name", As: "account_name"},
						myjson.Select{Field: "_id", As: "user_id"},
					).
					Join(myjson.Join{
						Collection: "account",
						On: []myjson.Where{
							{
								Field: "_id",
								Op:    myjson.WhereOpNeq,
								Value: "$account_id",
							},
						},
						As: "acc",
					}).
					Query())
				assert.NoError(t, err)

				for _, r := range results.Documents {
					assert.True(t, r.Exists("account_name"))
					assert.True(t, r.Exists("account_id"))
					assert.True(t, r.Exists("user_id"))
					if usrs[r.GetString("user_id")] != nil {
						assert.NotEmpty(t, usrs[r.GetString("user_id")])
						assert.NotEqual(t, usrs[r.GetString("user_id")].Get("account_id"), r.GetString("account_id"))
					}
				}
			}
		}))
	})
	t.Run("join account to user", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			accID := ""
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				doc := testutil.NewUserDoc()
				accID = doc.GetString("account_id")
				doc2 := testutil.NewUserDoc()
				assert.Nil(t, doc2.Set("account_id", accID))
				assert.NoError(t, tx.Set(ctx, "user", doc))
				assert.NoError(t, tx.Set(ctx, "user", doc2))
				return nil
			}))
			results, err := db.Query(ctx, "account", myjson.Q().
				Select(
					myjson.Select{Field: "_id", As: "account_id"},
					myjson.Select{Field: "name", As: "account_name"},
					myjson.Select{Field: "usr.name"},
				).
				Where(
					myjson.Where{
						Field: "_id",
						Op:    myjson.WhereOpEq,
						Value: accID,
					},
				).
				Join(myjson.Join{
					Collection: "user",
					On: []myjson.Where{
						{
							Field: "account_id",
							Op:    myjson.WhereOpEq,
							Value: "$_id",
						},
					},
					As: "usr",
				}).
				OrderBy(myjson.OrderBy{Field: "account_name", Direction: myjson.OrderByDirectionAsc}).
				Query())
			assert.NoError(t, err)

			for _, r := range results.Documents {
				assert.True(t, r.Exists("account_name"))
				assert.True(t, r.Exists("account_id"))
				assert.True(t, r.Exists("usr"))
			}
			assert.Equal(t, 2, results.Count)
		}))
	})
	t.Run("join task to user to account", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			var usrs = map[string]*myjson.Document{}
			var tasks = map[string]*myjson.Document{}
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i < 100; i++ {
					u := testutil.NewUserDoc()
					tsk := testutil.NewTaskDoc(u.GetString("_id"))
					usrs[u.GetString("_id")] = u
					tasks[tsk.GetString("_id")] = tsk
					assert.NoError(t, tx.Set(ctx, "user", u))
					assert.NoError(t, tx.Set(ctx, "task", tsk))
				}
				return nil
			}))
			//assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
			//	assert.NoError(t, tx.Set(ctx, "user", testutil.NewUserDoc()))
			//	return nil
			//}))
			{
				results, err := db.Query(ctx, "task", myjson.Q().
					Select(
						myjson.Select{Field: "acc._id", As: "account_id"},
						myjson.Select{Field: "acc.name", As: "account_name"},
						myjson.Select{Field: "usr._id", As: "user_id"},
						myjson.Select{Field: "_id", As: "task_id"},
						myjson.Select{Field: "content", As: "task_content"},
					).
					Join(myjson.Join{
						Collection: "user",
						On: []myjson.Where{
							{
								Field: "_id",
								Op:    myjson.WhereOpEq,
								Value: "$user",
							},
						},
						As: "usr",
					}).
					Join(myjson.Join{
						Collection: "account",
						On: []myjson.Where{
							{
								Field: "_id",
								Op:    myjson.WhereOpEq,
								Value: "$usr.account_id",
							},
						},
						As: "acc",
					}).
					Query())
				assert.NoError(t, err)
				assert.NotEqual(t, 0, results.Count)
				for _, r := range results.Documents {
					assert.True(t, r.Exists("account_name"))
					assert.True(t, r.Exists("account_id"))
					assert.True(t, r.Exists("user_id"))
					assert.NotEmpty(t, usrs[r.GetString("user_id")], r.String())
					assert.Equal(t, usrs[r.GetString("user_id")].Get("account_id"), r.GetString("account_id"))
				}
			}
		}))
	})
	t.Run("cascade delete", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i <= 100; i++ {
					u := testutil.NewUserDoc()
					if err := tx.Set(ctx, "user", u); err != nil {
						return err
					}
					tsk := testutil.NewTaskDoc(u.GetString("_id"))
					if err := tx.Set(ctx, "task", tsk); err != nil {
						return err
					}
				}
				return nil
			}))
			assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				for i := 0; i <= 100; i++ {
					if err := tx.Delete(ctx, "account", fmt.Sprint(i)); err != nil {
						return err
					}
				}
				return nil
			}))
			results, err := db.Query(ctx, "account", myjson.Query{Select: []myjson.Select{{Field: "*"}}})
			assert.NoError(t, err)
			assert.Equal(t, 0, results.Count, "failed to delete accounts")
			results, err = db.Query(ctx, "user", myjson.Query{Select: []myjson.Select{{Field: "*"}}})
			assert.NoError(t, err)
			assert.Equal(t, 0, results.Count, "failed to cascade delete users")
			results, err = db.Query(ctx, "task", myjson.Query{Select: []myjson.Select{{Field: "*"}}})
			assert.NoError(t, err)
			assert.Equal(t, 0, results.Count, "failed to cascade delete tasks")
		}))
	})
}

func TestTriggers(t *testing.T) {
	t.Run("test set_timestamp trigger", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				id, err := tx.Create(ctx, "user", testutil.NewUserDoc())
				assert.NoError(t, err)
				u, err := tx.Get(ctx, "user", id)
				assert.NoError(t, err)
				assert.True(t, time.Now().Truncate(1*time.Minute).Before(u.GetTime("timestamp")))
				return err
			}))
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				u := testutil.NewUserDoc()
				err := tx.Set(ctx, "user", u)
				assert.NoError(t, err)
				gu, err := tx.Get(ctx, "user", u.GetString("_id"))
				assert.NoError(t, err)
				assert.True(t, time.Now().Truncate(1*time.Minute).Before(gu.GetTime("timestamp")))
				return err
			}))
		}))
	})
}

func TestConfigure(t *testing.T) {
	t.Run("test configure", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.NoError(t, testutil.SeedUsers(ctx, db, 10, 3))
			assert.NoError(t, db.Configure(ctx, []string{testutil.AccountSchema, testutil.UserSchema}))
			assert.False(t, db.HasCollection(ctx, "task"))
			assert.NoError(t, db.Configure(ctx, []string{testutil.AccountSchema, testutil.UserSchema, testutil.TaskSchema}))
			assert.True(t, db.HasCollection(ctx, "task"))
			assert.NoError(t, db.Configure(ctx, []string{testutil.AccountSchema}))
			assert.False(t, db.HasCollection(ctx, "task"))
			assert.False(t, db.HasCollection(ctx, "user"))
			assert.True(t, db.HasCollection(ctx, "account"))
		}))
	})
	t.Run("test bad configure", testutil.Test(t, testutil.TestConfig{
		Opts: []myjson.DBOpt{
			myjson.WithGlobalJavascriptFunctions([]string{testutil.GlobalScript}),
		},
		Persist:     false,
		Collections: testutil.AllCollections,
		Roles:       []string{"super_user"},
	}, func(ctx context.Context, t *testing.T, db myjson.Database) {
		assert.NoError(t, testutil.Seed(ctx, db, 100, 10, 3))
		var badTaskSchema string
		{
			schema := db.GetSchema(ctx, "task")
			bits, err := schema.MarshalJSON()
			assert.NoError(t, err)
			bits, err = sjson.SetBytes(bits, "properties.user.x-foreign.collection", "usr")
			assert.NoError(t, err)
			badTaskSchema = string(bits)
		}
		assert.Error(t, db.Configure(ctx, []string{testutil.AccountSchema, testutil.UserSchema, badTaskSchema}))
		assert.NoError(t, db.Configure(ctx, []string{testutil.AccountSchema, testutil.UserSchema, testutil.TaskSchema}))
		assert.NoError(t, db.Configure(ctx, []string{testutil.AccountSchema}))
		assert.False(t, db.HasCollection(ctx, "task"))
		assert.False(t, db.HasCollection(ctx, "user"))
		assert.True(t, db.HasCollection(ctx, "account"))
	}))
	t.Run("test configure while seeding concurrently", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				testutil.SeedUsers(ctx, db, 10, 3)
			}()
			assert.NoError(t, db.Configure(ctx, []string{testutil.AccountSchema, testutil.UserSchema}))
			assert.False(t, db.HasCollection(ctx, "task"))
			assert.NoError(t, db.Configure(ctx, []string{testutil.AccountSchema, testutil.UserSchema, testutil.TaskSchema}))
			assert.True(t, db.HasCollection(ctx, "task"))
			assert.NoError(t, db.Configure(ctx, []string{testutil.AccountSchema}))
			assert.False(t, db.HasCollection(ctx, "task"))
			assert.False(t, db.HasCollection(ctx, "user"))
			assert.True(t, db.HasCollection(ctx, "account"))
			wg.Wait()
		}))
	})
}
