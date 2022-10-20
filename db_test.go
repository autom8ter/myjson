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
	"github.com/autom8ter/wolverine/internal/testutil"
)

func Test(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := wolverine.New(ctx, wolverine.Config{
		Path:  "inmem",
		Debug: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Nil(t, db.SetCollection(ctx, testutil.UserCollection))
	assert.Nil(t, db.SetCollection(ctx, testutil.TaskCollection))
	defer db.Close(ctx)
	t.Run("seed_users_tasks", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			usr := testutil.NewUserDoc()
			assert.Nil(t, db.Set(ctx, "user", usr))
			assert.Nil(t, db.Set(ctx, "task", testutil.NewTaskDoc(usr.GetID())))
			assert.Nil(t, db.Set(ctx, "task", testutil.NewTaskDoc(usr.GetID())))
			result, err := db.Get(ctx, "user", usr.GetID())
			assert.Nil(t, err)
			assert.Equal(t, usr.GetID(), result.GetID())
			assert.Nil(t, err)
			assert.Equal(t, usr.Get("name"), result.Get("name"))
			assert.Equal(t, usr.Get("language"), result.Get("language"))
		}
	})
	assert.Nil(t, db.SetCollections(ctx, []*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}))
	t.Run("set-get-query", func(t *testing.T) {
		usr := testutil.NewUserDoc()
		usr.Set("contact.email", testutil.MyEmail)
		assert.Nil(t, db.Set(ctx, "user", usr))
		result, err := db.Get(ctx, "user", usr.GetID())
		assert.Nil(t, err)
		assert.Equal(t, testutil.MyEmail, result.Get("contact.email"))
		query := &wolverine.Query{
			//Fields:  []string{"email"},
			Where: []wolverine.Where{
				{
					Field: "contact.email",
					Op:    wolverine.Eq,
					Value: testutil.MyEmail,
				},
			},
			Limit:   1,
			OrderBy: wolverine.OrderBy{},
		}
		results, err := db.Query(ctx, "user", *query)
		assert.Nil(t, err)
		assert.GreaterOrEqual(t, 1, len(results.Stats.IndexedFields))
		assert.Equal(t, 1, len(results.Documents))
		assert.Equal(t, testutil.MyEmail, result.Get("contact.email"))
		update := wolverine.NewDocument()
		newEmail := gofakeit.Email()
		update.Set("contact.email", newEmail)
		assert.Equal(t, newEmail, update.Get("contact.email"))
		assert.Nil(t, db.QueryUpdate(ctx, update, "user", *query))
		result, err = db.Get(ctx, "user", usr.GetID())
		assert.Nil(t, err)
		assert.NotEqual(t, testutil.MyEmail, result.Get("contact.email"))
		query.Where[0].Value = newEmail
		assert.Nil(t, db.QueryDelete(ctx, "user", *query))
		result, err = db.Get(ctx, "user", usr.GetID())
		assert.NotNil(t, err)
		assert.Empty(t, result)
	})
	t.Run("get all then delete", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB([]*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
			var ids []string
			for i := 0; i < 3; i++ {
				u := testutil.NewUserDoc()
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
	t.Run("update", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB([]*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
			var documents []*wolverine.Document
			for i := 0; i < 3; i++ {
				u := testutil.NewUserDoc()
				documents = append(documents, u)
				assert.Nil(t, db.Set(ctx, "user", u))
			}
			for _, doc := range documents {
				email := gofakeit.Email()
				newDoc, err := wolverine.NewDocumentFromMap(map[string]interface{}{
					"_id":           doc.GetID(),
					"contact.email": email,
				})
				assert.Nil(t, err)
				assert.Equal(t, email, newDoc.Get("contact.email"))
				assert.Nil(t, db.Update(ctx, "user", newDoc))
				fetched, err := db.Get(ctx, "user", doc.GetID())
				assert.Nil(t, err)
				assert.Equal(t, email, fetched.Get("contact.email"))
			}
		}))
	})
	t.Run("search", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB([]*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
			record := testutil.NewUserDoc()
			record.Set("contact.email", testutil.MyEmail)
			record.Set("account_id", 1)
			record.Set("language", "english")
			assert.Nil(t, db.Set(ctx, "user", record))
			for i := 0; i < 3; i++ {
				assert.Nil(t, db.Set(ctx, "user", testutil.NewUserDoc()))
			}
			{
				results, err := db.Search(ctx, "user", wolverine.SearchQuery{
					Select: []string{"name", "contact.email"},
					Where: []wolverine.SearchWhere{
						{
							Field: "account_id",
							Op:    wolverine.Basic,
							Value: 1,
						},
						{
							Field: "language",
							Op:    wolverine.Basic,
							Value: "english",
						},
						{
							Field: "contact.email",
							Op:    wolverine.Prefix,
							Value: "colemanword",
						},
					},
					Limit: 100,
				})
				assert.Nil(t, err)
				assert.Equal(t, 1, len(results.Documents))
				//assert.EqualValues(t, testutil.MyEmail, results.Documents[0].Get("contact.email"))
			}

			{
				results, err := db.Search(ctx, "user", wolverine.SearchQuery{
					Select: []string{"name", "contact.email"},
					Where: []wolverine.SearchWhere{
						{
							Field: "contact.email",
							Op:    wolverine.Wildcard,
							Value: "colemanword*",
						},
						{
							Field: "name",
							Op:    wolverine.Wildcard,
							Value: "colemanword*",
						},
					},
					Limit: 100,
				})
				assert.Nil(t, err)
				assert.Equal(t, 0, len(results.Documents))
			}
			{
				results, err := db.Search(ctx, "user", wolverine.SearchQuery{
					Select: []string{"name", "contact.email"},
					Where: []wolverine.SearchWhere{
						{
							Field: "search(age)",
							Op:    wolverine.Wildcard,
							Value: "colemanword*",
						},
					},
					Limit: 100,
				})
				assert.Nil(t, err)
				assert.Equal(t, 0, len(results.Documents))
			}
			{
				results, err := db.Search(ctx, "user", wolverine.SearchQuery{
					Select: []string{"name", "contact.email"},
					Where: []wolverine.SearchWhere{
						{
							Field: "contact.email",
							Op:    wolverine.Fuzzy,
							Value: "colemanword",
						},
					},
					Limit: 100,
				})
				assert.Nil(t, err)
				assert.Equal(t, 1, len(results.Documents))
				assert.EqualValues(t, testutil.MyEmail, results.Documents[0].Get("contact.email"))
			}
			{
				results, err := db.Search(ctx, "user", wolverine.SearchQuery{
					Select: []string{"name", "contact.email"},
					Where: []wolverine.SearchWhere{
						{
							Field: "contact.email",
							Op:    wolverine.Basic,
							Value: "colemanword",
						},
					},
					Limit: 100,
				})
				assert.Nil(t, err)
				assert.Equal(t, 1, len(results.Documents))
				assert.EqualValues(t, testutil.MyEmail, results.Documents[0].Get("contact.email"))
			}
			{
				results, err := db.Search(ctx, "user", wolverine.SearchQuery{
					Select: []string{"name", "contact.email"},
					Where: []wolverine.SearchWhere{
						{
							Field: "contact.email",
							Op:    wolverine.Regex,
							Value: "colemanword*",
						},
					},
					Limit: 100,
				})
				assert.Nil(t, err)
				assert.Equal(t, 1, len(results.Documents))
				assert.EqualValues(t, testutil.MyEmail, results.Documents[0].Get("contact.email"))
			}
		}))
	})
	t.Run("stream", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB([]*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
			found := 0
			mu := sync.RWMutex{}
			go func() {
				assert.Nil(t, db.ChangeStream(ctx, []string{"user"}, func(ctx context.Context, event *wolverine.Event) error {
					mu.Lock()
					found += len(event.Documents)
					mu.Unlock()
					return nil
				}))
			}()
			for i := 0; i < 3; i++ {
				assert.Nil(t, db.Set(ctx, "user", testutil.NewUserDoc()))
			}
			time.Sleep(1 * time.Second)
			mu.RLock()
			assert.Equal(t, 3, found)
			mu.RUnlock()
		}))
	})
	t.Run("batch set/delete/update", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB([]*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
			var records []*wolverine.Document
			var ids []string
			for i := 0; i < 5; i++ {
				doc := testutil.NewUserDoc()
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
	t.Run("expect errors", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB([]*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
			var records []*wolverine.Document
			var ids []string
			for i := 0; i < 5; i++ {
				doc := testutil.NewUserDoc()
				records = append(records, doc)
				ids = append(ids, doc.GetID())
			}
			assert.NotNil(t, db.BatchSet(ctx, "use", records))
			_, err := db.Get(ctx, "use", "isdf")
			assert.NotNil(t, err)
			err = db.Delete(ctx, "use", "isdf")
			assert.NotNil(t, err)
			results, err := db.Query(ctx, "test", wolverine.Query{})
			assert.NotNil(t, err)
			assert.Nil(t, results.Documents)
		}))
	})
	t.Run("order by - no index asc", func(t *testing.T) {

		users, err := db.Query(ctx, "user", wolverine.Query{
			Select: nil,
			Where:  nil,
			Limit:  10,
			OrderBy: wolverine.OrderBy{
				Field:     "name",
				Direction: wolverine.ASC,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, 10, len(users.Documents))
		assert.False(t, users.Stats.OrderedIndex)
		var before []byte
		for _, usr := range users.Documents {
			name := usr.GetString("name")
			fmt.Println(name)
			if before != nil {
				assert.LessOrEqual(t, bytes.Compare(before, []byte(name)), 0)
			}
			before = []byte(name)
		}
	})
	t.Run("order by - no index desc", func(t *testing.T) {

		users, err := db.Query(ctx, "user", wolverine.Query{
			Select: nil,
			Where:  nil,
			Limit:  10,
			OrderBy: wolverine.OrderBy{
				Field:     "name",
				Direction: wolverine.DESC,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, 10, len(users.Documents))
		assert.False(t, users.Stats.OrderedIndex)
		var before []byte
		for _, usr := range users.Documents {
			name := usr.GetString("name")
			fmt.Println(name)
			if before != nil {
				assert.Equal(t, bytes.Compare(before, []byte(name)), 1)
			}
			before = []byte(name)
		}
	})
	t.Run("order by - indexed asc", func(t *testing.T) {

		users, err := db.Query(ctx, "user", wolverine.Query{
			Select: nil,
			Where:  nil,
			Limit:  10,
			OrderBy: wolverine.OrderBy{
				Field:     "language",
				Direction: wolverine.ASC,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, 10, len(users.Documents))
		assert.Equal(t, "language", users.Stats.IndexedFields[0])
		assert.True(t, users.Stats.OrderedIndex)
		var previous string
		for _, usr := range users.Documents {
			lang := usr.Get("language")
			name := usr.Get("name")
			fmt.Println(lang, name)
			if previous != "" {
				assert.LessOrEqual(t, bytes.Compare([]byte(previous), []byte(cast.ToString(lang))), 0)
			}
			previous = cast.ToString(lang)
		}
	})
	t.Run("order by - indexed desc", func(t *testing.T) {

		users, err := db.Query(ctx, "user", wolverine.Query{
			Select: nil,
			Where:  nil,
			Limit:  10,
			OrderBy: wolverine.OrderBy{
				Field:     "language",
				Direction: wolverine.DESC,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, 10, len(users.Documents))
		assert.Equal(t, "language", users.Stats.IndexedFields[0])
		assert.True(t, users.Stats.OrderedIndex)
		var previous string
		for _, usr := range users.Documents {
			lang := usr.Get("language")
			name := usr.Get("name")
			fmt.Println(lang, name)
			if previous != "" {
				assert.Equal(t, bytes.Compare([]byte(previous), []byte(cast.ToString(lang))), 1)
			}
			previous = cast.ToString(lang)
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
		assert.Nil(b, testutil.TestDB([]*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Set(ctx, "user", testutil.NewUserDoc()); err != nil {
					b.Fatal(err)
				}
			}
		}))
	})
	b.Run("get", func(b *testing.B) {
		assert.Nil(b, testutil.TestDB([]*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
			b.ResetTimer()
			u := testutil.NewUserDoc()
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
		assert.Nil(b, testutil.TestDB([]*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				doc := testutil.NewUserDoc()
				if err := db.Set(ctx, "user", doc); err != nil {
					b.Fatal(err)
				}
				if _, err := db.Get(ctx, "user", doc.GetID()); err != nil {
					b.Fatal(err)
				}
			}
		}))
	})
	b.Run("query.1000", func(b *testing.B) {
		assert.Nil(b, testutil.TestDB([]*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
			doc := testutil.NewUserDoc()
			doc.Set("contact.email", testutil.MyEmail)
			if err := db.Set(ctx, "user", doc); err != nil {
				b.Fatal(err)
			}
			var docs []*wolverine.Document
			for i := 0; i < 999; i++ {
				docs = append(docs, testutil.NewUserDoc())
			}
			if err := db.BatchSet(ctx, "user", docs); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.Log(i)
				results, err := db.Query(ctx, "user", wolverine.Query{
					Select: nil,
					Where: []wolverine.Where{
						{
							Field: "contact.email",
							Op:    wolverine.Eq,
							Value: testutil.MyEmail,
						},
					},
					Limit:   1000,
					OrderBy: wolverine.OrderBy{},
				})
				if err != nil {
					b.Fatal(err)
				}
				if len(results.Documents) != 1 {
					b.Fatal("failed to query email", len(results.Documents))
				}
			}
		}))
	})
	b.Run("search.1000", func(b *testing.B) {
		assert.Nil(b, testutil.TestDB([]*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
			doc := testutil.NewUserDoc()
			doc.Set("contact.email", "colemanword@gmail.com")
			if err := db.Set(ctx, "user", doc); err != nil {
				b.Fatal(err)
			}
			var docs []*wolverine.Document
			for i := 0; i < 999; i++ {
				docs = append(docs, testutil.NewUserDoc())
			}
			if err := db.BatchSet(ctx, "user", docs); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				results, err := db.Search(ctx, "user", wolverine.SearchQuery{
					Select: nil,
					Where: []wolverine.SearchWhere{
						{
							Field: "contact.email",
							Op:    wolverine.Basic,
							Value: testutil.MyEmail,
						},
					},
					Limit: 1000,
				})
				if err != nil {
					b.Fatal(err)
				}
				if len(results.Documents) != 1 {
					b.Fatal("failed to search email", len(results.Documents))
				}
			}
		}))
	})
	b.Run("batch.10000", func(b *testing.B) {
		assert.Nil(b, testutil.TestDB([]*wolverine.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
			var docs []*wolverine.Document
			for i := 0; i < 10000; i++ {
				docs = append(docs, testutil.NewUserDoc())
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
