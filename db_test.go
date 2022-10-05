package wolverine_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/autom8ter/wolverine"
)

var defaultCollections = []wolverine.Collection{
	{
		Name: "user",
		Indexes: []wolverine.Index{
			{
				Fields: []string{
					"contact.email",
				},
			},
			{
				Fields: []string{
					"age",
				},
				FullText: true,
			},
		},
	},
	{
		Name: "task",
		Indexes: []wolverine.Index{
			{
				Fields: []string{
					"user",
				},
			},
		},
	},
}

func newUserDoc() *wolverine.Document {
	doc, err := wolverine.NewDocumentFromMap(map[string]interface{}{
		"_collection": "user",
		"_id":         gofakeit.UUID(),
		"name":        gofakeit.Name(),
		"contact": map[string]interface{}{
			"email": gofakeit.Email(),
		},
		"account_id":      gofakeit.IntRange(0, 100),
		"language":        gofakeit.Language(),
		"birthday_month":  gofakeit.Month(),
		"favorite_number": gofakeit.Second(),
		"gender":          gofakeit.Gender(),
		"age":             gofakeit.IntRange(0, 100),
		"annotations":     gofakeit.Map(),
	})
	if err != nil {
		panic(err)
	}
	return doc
}

func newTaskDoc(usrID string) *wolverine.Document {
	doc, err := wolverine.NewDocumentFromMap(map[string]interface{}{
		"_collection": "task",
		"_id":         gofakeit.UUID(),
		"user":        usrID,
		"content":     gofakeit.LoremIpsumSentence(5),
	})
	if err != nil {
		panic(err)
	}
	return doc
}

func Test(t *testing.T) {
	const myEmail = "colemanword@gmail.com"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := wolverine.New(ctx, wolverine.Config{
		Path:  "inmem",
		Debug: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range defaultCollections {
		assert.Nil(t, db.SetCollection(ctx, &c))
	}
	defer db.Close(ctx)
	t.Run("seed_users_tasks", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			usr := newUserDoc()
			assert.Nil(t, db.Set(ctx, "user", usr))
			assert.Nil(t, db.Set(ctx, "task", newTaskDoc(usr.GetID())))
			assert.Nil(t, db.Set(ctx, "task", newTaskDoc(usr.GetID())))
			result, err := db.Get(ctx, "user", usr.GetID())
			assert.Nil(t, err)
			assert.Equal(t, "user", result.GetCollection())
			assert.Equal(t, usr.GetID(), result.GetID())
			assert.Nil(t, err)
			assert.Equal(t, usr.Get("name"), result.Get("name"))
			assert.Equal(t, usr.Get("language"), result.Get("language"))
		}
	})
	t.Run("set-get-query", func(t *testing.T) {
		usr := newUserDoc()
		usr.Set("contact.email", myEmail)
		assert.Nil(t, db.Set(ctx, "user", usr))
		result, err := db.Get(ctx, "user", usr.GetID())
		assert.Nil(t, err)
		assert.Equal(t, myEmail, result.Get("contact.email"))
		query := &wolverine.Query{
			//Fields:  []string{"email"},
			Where: []wolverine.Where{
				{
					Field: "contact.email",
					Op:    "==",
					Value: myEmail,
				},
			},
			Limit:   1,
			OrderBy: wolverine.OrderBy{},
		}
		results, err := db.Query(ctx, "user", *query)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, myEmail, result.Get("contact.email"))
		update := wolverine.NewDocument()
		newEmail := gofakeit.Email()
		update.Set("contact.email", newEmail)
		assert.Equal(t, newEmail, update.Get("contact.email"))
		assert.Nil(t, db.QueryUpdate(ctx, update, "user", *query))
		result, err = db.Get(ctx, "user", usr.GetID())
		assert.Nil(t, err)
		assert.NotEqual(t, myEmail, result.Get("contact.email"))
		query.Where[0].Value = newEmail
		assert.Nil(t, db.QueryDelete(ctx, "user", *query))
		result, err = db.Get(ctx, "user", usr.GetID())
		assert.NotNil(t, err)
		assert.Empty(t, result)
	})
	t.Run("get all", func(t *testing.T) {
		assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			var ids []string
			for i := 0; i < 3; i++ {
				u := newUserDoc()
				ids = append(ids, u.GetID())
				assert.Nil(t, db.Set(ctx, "user", u))
			}
			results, err := db.GetAll(ctx, "user", ids)
			assert.Nil(t, err)
			assert.Equal(t, 3, len(results))
			for _, id := range ids {
				assert.Nil(t, db.Delete(ctx, "user", id))
			}
			for _, id := range ids {
				result, err := db.Get(ctx, "user", id)
				assert.NotNil(t, err)
				assert.Nil(t, result)
			}
		}))
	})
	t.Run("search", func(t *testing.T) {
		assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			record := newUserDoc()
			record.Set("contact.email", myEmail)
			assert.Nil(t, db.Set(ctx, "user", record))
			for i := 0; i < 3; i++ {
				assert.Nil(t, db.Set(ctx, "user", newUserDoc()))
			}
			{
				results, err := db.Query(ctx, "user", wolverine.Query{
					Select: []string{"name", "contact.email"},
					Where: []wolverine.Where{
						{
							Field: "contact.email",
							Op:    wolverine.PrefixOp,
							Value: "colemanword",
						},
					},
					Limit: 100,
				})
				assert.Nil(t, err)
				assert.Equal(t, 1, len(results))
				//assert.EqualValues(t, myEmail, results[0].Get("contact.email"))
			}

			{
				results, err := db.Query(ctx, "user", wolverine.Query{
					Select: []string{"name", "contact.email"},
					Where: []wolverine.Where{
						{
							Field: "contact.email",
							Op:    wolverine.ContainsOp,
							Value: "colemanword",
						},
						{
							Field: "name",
							Op:    wolverine.ContainsOp,
							Value: "colemanword",
						},
					},
					Limit: 100,
				})
				assert.Nil(t, err)
				assert.Equal(t, 1, len(results))
				assert.EqualValues(t, myEmail, results[0].Get("contact.email"))
			}
			{
				results, err := db.Query(ctx, "user", wolverine.Query{
					Select: []string{"name", "contact.email"},
					Where: []wolverine.Where{
						{
							Field: "search(age)",
							Op:    wolverine.ContainsOp,
							Value: "colemanword",
						},
					},
					Limit: 100,
				})
				assert.Nil(t, err)
				assert.Equal(t, 0, len(results))
			}
			{
				results, err := db.Query(ctx, "user", wolverine.Query{
					Select: []string{"name", "contact.email"},
					Where: []wolverine.Where{
						{
							Field: "contact.email",
							Op:    wolverine.FuzzyOp,
							Value: "colemanword",
						},
					},
					Limit: 100,
				})
				assert.Nil(t, err)
				assert.Equal(t, 1, len(results))
				assert.EqualValues(t, myEmail, results[0].Get("contact.email"))
			}
			{
				results, err := db.Query(ctx, "user", wolverine.Query{
					Select: []string{"name", "contact.email"},
					Where: []wolverine.Where{
						{
							Field: "contact.email",
							Op:    wolverine.TermOp,
							Value: "colemanword",
						},
					},
					Limit: 100,
				})
				assert.Nil(t, err)
				assert.Equal(t, 1, len(results))
				assert.EqualValues(t, myEmail, results[0].Get("contact.email"))
			}
		}))
	})
	t.Run("stream", func(t *testing.T) {
		assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			found := 0
			mu := sync.RWMutex{}
			go func() {
				assert.Nil(t, db.ChangeStream(ctx, []string{"user"}, func(ctx context.Context, records []*wolverine.Document) error {
					mu.Lock()
					found += len(records)
					mu.Unlock()
					return nil
				}))
			}()
			for i := 0; i < 3; i++ {
				assert.Nil(t, db.Set(ctx, "user", newUserDoc()))
			}
			time.Sleep(1 * time.Second)
			mu.RLock()
			assert.Equal(t, 3, found)
			mu.RUnlock()
		}))
	})
	t.Run("batch set/delete/update", func(t *testing.T) {
		assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			var records []*wolverine.Document
			var ids []string
			for i := 0; i < 5; i++ {
				doc := newUserDoc()
				records = append(records, doc)
				ids = append(ids, doc.GetID())
			}
			assert.Nil(t, db.BatchSet(ctx, "user", records))
			for _, record := range records {
				record.Set("name", gofakeit.Name())
			}
			assert.Nil(t, db.BatchUpdate(ctx, "user", records))
			assert.Nil(t, db.BatchDelete(ctx, "user", ids))
			for _, id := range ids {
				result, err := db.Get(ctx, "user", id)
				assert.NotNil(t, err)
				assert.Nil(t, result)
			}
		}))
	})
	t.Run("order by", func(t *testing.T) {
		users, err := db.Query(ctx, "user", wolverine.Query{
			Select:  nil,
			Where:   nil,
			StartAt: "",
			Limit:   10,
			OrderBy: wolverine.OrderBy{
				Field:     "language",
				Direction: wolverine.ASC,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, 10, len(users))
		var previous string
		for _, usr := range users {
			lang := usr.Get("language")
			name := usr.Get("name")
			fmt.Println(lang, name)
			if previous != "" {
				assert.LessOrEqual(t, bytes.Compare([]byte(previous), []byte(cast.ToString(lang))), 0)
			}
			previous = cast.ToString(lang)
		}
	})
	t.Run("mapreduce sum", func(t *testing.T) {
		results, err := db.Aggregate(ctx, "user", wolverine.AggregateQuery{
			GroupBy: []string{"account_id"},
			Aggregate: []wolverine.Aggregate{
				{
					Function: wolverine.AggregateCount,
					Field:    "gender",
				},
				{
					Function: wolverine.AggregateCount,
					Field:    "language",
				},
				{
					Function: wolverine.AggregateAvg,
					Field:    "age",
				},
			},
			OrderBy: wolverine.OrderBy{
				Field:     "account_id",
				Direction: wolverine.ASC,
			},
		})
		assert.Nil(t, err)
		assert.Greater(t, len(results), 1)
		for _, result := range results {
			fmt.Println(result.String())
		}
	})
	//t.Run("drop collections", func(t *testing.T) {
	//	assert.Nil(t, db.DropAll(ctx, []string{"user", "task"}))
	//})
}

// go test -bench=. -benchmem -run=^#

/*
goos: darwin
goarch: amd64
pkg: github.com/autom8ter/wolverine
cpu: Intel(R) Core(TM) i9-9880H CPU @ 2.30GHz
Benchmark/set-16                     565           2077823 ns/op         1332976 B/op      25417 allocs/op
Benchmark/get-16                  278175              4103 ns/op            3355 B/op         34 allocs/op
Benchmark/setget-16                  596           2142664 ns/op         1349257 B/op      25691 allocs/op
Benchmark/query.1000-16            52450             20755 ns/op           14992 B/op        102 allocs/op
Benchmark/search.1000-16            8380            120230 ns/op          219707 B/op        673 allocs/op
Benchmark/batch.10-16                163           6564877 ns/op         6803312 B/op     112352 allocs/op
*/
func Benchmark(b *testing.B) {
	// Benchmark/set-16         	      68	  16504072 ns/op
	b.Run("set", func(b *testing.B) {
		assert.Nil(b, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Set(ctx, "user", newUserDoc()); err != nil {
					b.Fatal(err)
				}
			}
		}))
	})
	b.Run("get", func(b *testing.B) {
		assert.Nil(b, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			b.ResetTimer()
			u := newUserDoc()
			if err := db.Set(ctx, "user", u); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := db.Get(ctx, "user", u.GetString("_id"))
				assert.Nil(b, err)
			}
		}))
	})
	b.Run("setget", func(b *testing.B) {
		assert.Nil(b, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				doc := newUserDoc()
				if err := db.Set(ctx, "user", doc); err != nil {
					b.Fatal(err)
				}
				if _, err := db.Get(ctx, doc.GetCollection(), doc.GetID()); err != nil {
					b.Fatal(err)
				}
			}
		}))
	})
	b.Run("query.1000", func(b *testing.B) {
		assert.Nil(b, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			doc := newUserDoc()
			doc.Set("contact.email", "colemanword@gmail.com")
			if err := db.Set(ctx, "user", doc); err != nil {
				b.Fatal(err)
			}
			var docs []*wolverine.Document
			for i := 0; i < 999; i++ {
				docs = append(docs, newUserDoc())
			}
			if err := db.BatchSet(ctx, "user", docs); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				results, err := db.Query(ctx, "user", wolverine.Query{
					Select: nil,
					Where: []wolverine.Where{
						{
							Field: "contact.email",
							Op:    "==",
							Value: "colemanword@gmail.com",
						},
					},
					StartAt: "",
					Limit:   1000,
					OrderBy: wolverine.OrderBy{},
				})
				if err != nil {
					b.Fatal(err)
				}
				if len(results) != 1 {
					b.Fatal("failed to query email", len(results))
				}
			}
		}))
	})
	b.Run("search.1000", func(b *testing.B) {
		assert.Nil(b, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			doc := newUserDoc()
			doc.Set("contact.email", "colemanword@gmail.com")
			if err := db.Set(ctx, "user", doc); err != nil {
				b.Fatal(err)
			}
			var docs []*wolverine.Document
			for i := 0; i < 999; i++ {
				docs = append(docs, newUserDoc())
			}
			if err := db.BatchSet(ctx, "user", docs); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				results, err := db.Query(ctx, "user", wolverine.Query{
					Select: nil,
					Where: []wolverine.Where{
						{
							Field: "contact.email",
							Op:    wolverine.PrefixOp,
							Value: "colemanword",
						},
					},
					StartAt: "",
					Limit:   1000,
					OrderBy: wolverine.OrderBy{},
				})
				if err != nil {
					b.Fatal(err)
				}
				if len(results) != 1 {
					b.Fatal("failed to search email", len(results))
				}
			}
		}))
	})
	b.Run("batch.10000", func(b *testing.B) {
		assert.Nil(b, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			var docs []*wolverine.Document
			for i := 0; i < 10000; i++ {
				docs = append(docs, newUserDoc())
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.BatchSet(ctx, "user", docs); err != nil {
					b.Fatal(err)
				}
			}
		}))
	})
}

func testDB(collections []wolverine.Collection, fn func(ctx context.Context, db wolverine.DB)) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := wolverine.New(ctx, wolverine.Config{
		Path:  "inmem",
		Debug: true,
	})
	if err != nil {
		return err
	}
	for _, c := range defaultCollections {
		if err := db.SetCollection(ctx, &c); err != nil {
			return err
		}
	}
	defer db.Close(ctx)
	fn(ctx, db)
	return nil
}
