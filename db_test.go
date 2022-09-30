package wolverine_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"wolverine"
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

func Test(t *testing.T) {
	const myEmail = "colemanword@gmail.com"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := wolverine.New(ctx, wolverine.Config{
		Path:        "inmem",
		Collections: defaultCollections,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	t.Run("seed_users_tasks", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			id := fmt.Sprintf("%v", i)
			usr := wolverine.Record{
				"_collection": "user",
				"_id":         id,
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
			}
			assert.Nil(t, db.Set(ctx, usr))
			assert.Nil(t, db.Set(ctx, wolverine.Record{
				"_collection": "task",
				"_id":         gofakeit.UUID(),
				"user":        id,
				"content":     gofakeit.LoremIpsumSentence(25),
			}))
			if i%2 == 0 {
				assert.Nil(t, db.Set(ctx, wolverine.Record{
					"_collection": "task",
					"_id":         gofakeit.UUID(),
					"user":        id,
					"content":     gofakeit.LoremIpsumSentence(5),
				}))
			}
			result, err := db.Get(ctx, "user", id)
			assert.Nil(t, err)
			assert.Equal(t, "user", result.GetCollection())
			assert.Equal(t, id, result.GetID())
			assert.Nil(t, err)
			assert.Equal(t, usr["name"], result["name"])
			assert.Equal(t, usr["language"], result["language"])
		}
	})
	t.Run("set-get-query", func(t *testing.T) {
		id := gofakeit.UUID()
		usr := wolverine.Record{
			"_collection": "user",
			"_id":         id,
			"name":        gofakeit.Name(),
			"contact": map[string]interface{}{
				"email": myEmail,
			},
			"account_id":      gofakeit.IntRange(0, 100),
			"language":        gofakeit.Language(),
			"favorite_number": gofakeit.Second(),
			"gender":          gofakeit.Gender(),
		}
		assert.Nil(t, db.Set(ctx, usr))
		result, err := db.Get(ctx, "user", id)
		assert.Nil(t, err)
		assert.Equal(t, myEmail, result["contact.email"])
		results, err := db.Query(ctx, "user", wolverine.Query{
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
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, myEmail, results[0]["contact.email"])
	})
	t.Run("search", func(t *testing.T) {
		assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			record := wolverine.Record{
				"_collection": "user",
				"_id":         gofakeit.UUID(),
				"name":        gofakeit.Name(),
				"language":    gofakeit.Language(),
				"contact": map[string]interface{}{
					"email": myEmail,
				},
				"account_id":      gofakeit.IntRange(0, 100),
				"favorite_number": gofakeit.Second(),
				"gender":          gofakeit.Gender(),
			}
			assert.Nil(t, db.Set(ctx, record))
			for i := 0; i < 3; i++ {
				record := wolverine.Record{
					"_collection": "user",
					"_id":         gofakeit.UUID(),
					"name":        gofakeit.Name(),
					"language":    gofakeit.Language(),
					"contact": map[string]interface{}{
						"email": gofakeit.Email(),
					},
					"account_id":      gofakeit.IntRange(0, 100),
					"favorite_number": gofakeit.Second(),
					"gender":          gofakeit.Gender(),
				}
				assert.Nil(t, db.Set(ctx, record))
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
				assert.EqualValues(t, myEmail, results[0]["contact.email"])
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
				assert.EqualValues(t, myEmail, results[0]["contact.email"])
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
				assert.EqualValues(t, myEmail, results[0]["contact.email"])
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
				assert.EqualValues(t, myEmail, results[0]["contact.email"])
			}
		}))
	})
	t.Run("ttl", func(t *testing.T) {
		now := time.Now().Add(3 * time.Second)
		id := gofakeit.UUID()
		assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			assert.Nil(t, db.Set(ctx, wolverine.Record{
				"_collection": "user",
				"_id":         id,
				"_expires_at": now,
				"name":        gofakeit.Name(),
				"contact": map[string]interface{}{
					"email": gofakeit.Email(),
				},
				"account_id":      gofakeit.IntRange(0, 100),
				"language":        gofakeit.Language(),
				"favorite_number": gofakeit.Second(),
				"gender":          gofakeit.Gender(),
			}))
			time.Sleep(3 * time.Second)
			result, err := db.Get(ctx, "user", id)
			assert.NotNil(t, err)
			assert.Nil(t, result)
		}))
	})
	t.Run("stream", func(t *testing.T) {
		assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			found := 0
			go func() {
				assert.Nil(t, db.Stream(ctx, []string{"user"}, func(ctx context.Context, records []wolverine.Record) error {
					found += len(records)
					return nil
				}))
			}()
			for i := 0; i < 3; i++ {
				assert.Nil(t, db.Set(ctx, wolverine.Record{
					"_collection": "user",
					"_id":         gofakeit.UUID(),
					"name":        gofakeit.Name(),
					"contact": map[string]interface{}{
						"email": gofakeit.Email(),
					},
					"account_id": gofakeit.IntRange(0, 100),
					"language":   gofakeit.Language(),
				}))
			}
			time.Sleep(1 * time.Second)
			assert.Equal(t, 3, found)
		}))
	})
	t.Run("batch set", func(t *testing.T) {
		assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			var records []wolverine.Record
			for i := 0; i < 5; i++ {
				records = append(records, wolverine.Record{
					"_collection": "user",
					"_id":         gofakeit.UUID(),
					"name":        gofakeit.Name(),
					"contact": map[string]interface{}{
						"email": gofakeit.Email(),
					},
					"account_id":      gofakeit.IntRange(0, 100),
					"language":        gofakeit.Language(),
					"favorite_number": gofakeit.Second(),
				})
			}
			assert.Nil(t, db.BatchSet(ctx, records))
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
			lang, _ := usr.Get("language")
			name, _ := usr.Get("name")
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
	t.Run("drop collections", func(t *testing.T) {
		assert.Nil(t, db.DropAll(ctx, []string{"user", "task"}))
	})
}

func Benchmark(b *testing.B) {
	// Benchmark/set-16         	      68	  16504072 ns/op
	b.Run("set", func(b *testing.B) {
		assert.Nil(b, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := fmt.Sprintf("%v", i)
				if err := db.Set(ctx, wolverine.Record{
					"_collection": "user",
					"_id":         id,
					"name":        gofakeit.Name(),
					"email":       gofakeit.Email(),
					"language":    gofakeit.Language(),
				}); err != nil {
					b.Fatal(err)
				}
			}
		}))
	})
	/*
		goos: darwin
		goarch: amd64
		pkg: wolverine
		cpu: Intel(R) Core(TM) i9-9880H CPU @ 2.30GHz
		Benchmark
		Benchmark/get
		Benchmark/get-16         	  212913	      3865 ns/op
	*/
	b.Run("get", func(b *testing.B) {
		assert.Nil(b, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			b.ResetTimer()
			if err := db.Set(ctx, wolverine.Record{
				"_collection": "user",
				"_id":         "1",
				"name":        gofakeit.Name(),
				"email":       gofakeit.Email(),
				"language":    gofakeit.Language(),
			}); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := db.Get(ctx, "user", "1")
				assert.Nil(b, err)
			}
		}))
	})
}

func testDB(collections []wolverine.Collection, fn func(ctx context.Context, db wolverine.DB)) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := wolverine.New(ctx, wolverine.Config{
		Path:        "inmem",
		Collections: collections,
	})
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	fn(ctx, db)
	return nil
}
