package wolverine_test

import (
	"context"
	"fmt"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/autom8ter/wolverine/schema"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/autom8ter/wolverine"
)

func TestSearch(t *testing.T) {
	assert.Nil(t, testutil.TestDB([]*schema.Collection{testutil.UserCollection, testutil.TaskCollection}, func(ctx context.Context, db wolverine.DB) {
		record := testutil.NewUserDoc()
		record.Set("contact.email", testutil.MyEmail)
		record.Set("account_id", 1)
		record.Set("language", "english")
		assert.Nil(t, db.Set(ctx, "user", record))
		for i := 0; i < 1000; i++ {
			assert.Nil(t, db.Set(ctx, "user", testutil.NewUserDoc()))
		}
		t.Run("basic", func(t *testing.T) {
			results, err := db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "account_id",
						Op:    schema.Basic,
						Value: 1,
					},
					{
						Field: "language",
						Op:    schema.Basic,
						Value: "english",
					},
					{
						Field: "contact.email",
						Op:    schema.Basic,
						Value: testutil.MyEmail,
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 1, len(results.Documents))
			assert.EqualValues(t, testutil.MyEmail, results.Documents[0].Get("contact.email"))

			results, err = db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "account_id",
						Op:    schema.Basic,
						Value: 2,
					},
					{
						Field: "language",
						Op:    schema.Basic,
						Value: "englis",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 0, len(results.Documents))

			results, err = db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "account_id",
						Op:    schema.Basic,
						Value: 1,
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Greater(t, len(results.Documents), 0)
		})

		t.Run("prefix", func(t *testing.T) {
			results, err := db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "account_id",
						Op:    schema.Basic,
						Value: 1,
					},
					{
						Field: "language",
						Op:    schema.Basic,
						Value: "english",
					},
					{
						Field: "contact.email",
						Op:    schema.Prefix,
						Value: "colemanword",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 1, len(results.Documents))
			assert.EqualValues(t, testutil.MyEmail, results.Documents[0].Get("contact.email"))

			results, err = db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "contact.email",
						Op:    schema.Prefix,
						Value: "colemanworz",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 0, len(results.Documents))

			results, err = db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "contact.email",
						Op:    schema.Prefix,
						Value: "c",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Greater(t, len(results.Documents), 0)
		})

		t.Run("wildcard", func(t *testing.T) {
			results, err := db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "account_id",
						Op:    schema.Basic,
						Value: 1,
					},
					{
						Field: "language",
						Op:    schema.Basic,
						Value: "english",
					},
					{
						Field: "contact.email",
						Op:    schema.Wildcard,
						Value: "colemanword*",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 1, len(results.Documents))
			assert.EqualValues(t, testutil.MyEmail, results.Documents[0].Get("contact.email"))

			results, err = db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "contact.email",
						Op:    schema.Wildcard,
						Value: "colemanworz*",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 0, len(results.Documents))

			results, err = db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "contact.email",
						Op:    schema.Wildcard,
						Value: "c*",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Greater(t, len(results.Documents), 0)
		})

		t.Run("date range", func(t *testing.T) {
			results, err := db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "timestamp",
						Op:    schema.DateRange,
						Value: fmt.Sprintf("%s,%s", time.Time{}, time.Now()),
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Greater(t, len(results.Documents), 0)

		})

		t.Run("term range", func(t *testing.T) {
			results, err := db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "account_id",
						Op:    schema.Basic,
						Value: 1,
					},
					{
						Field: "language",
						Op:    schema.Basic,
						Value: "english",
					},
					{
						Field: "contact.email",
						Op:    schema.TermRange,
						Value: "colemanword,colemanword@gmail.comz",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 1, len(results.Documents))
			assert.EqualValues(t, testutil.MyEmail, results.Documents[0].Get("contact.email"))

			results, err = db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "contact.email",
						Op:    schema.TermRange,
						Value: "zzzzz,zzzzzzz",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 0, len(results.Documents))

			results, err = db.Search(ctx, "user", schema.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []schema.SearchWhere{
					{
						Field: "contact.email",
						Op:    schema.TermRange,
						Value: "colemanword",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Greater(t, len(results.Documents), 0)
		})
		t.Run("search paginate", func(t *testing.T) {
			assert.Nil(t, testutil.TestDB(testutil.AllCollections, func(ctx context.Context, db wolverine.DB) {
				var usrs []*schema.Document
				for i := 0; i < 10; i++ {
					u := testutil.NewUserDoc()
					usrs = append(usrs, u)
				}
				assert.Nil(t, db.BatchSet(ctx, "user", usrs))
				seen := map[string]struct{}{}
				handler := func(page schema.Page) bool {
					for _, doc := range page.Documents {
						if _, ok := seen[doc.GetID()]; ok {
							t.Fatal("duplicate doc", doc.GetID())
						}
						seen[doc.GetID()] = struct{}{}
					}
					return true
				}

				assert.Nil(t, db.SearchPaginate(ctx, "user", schema.SearchQuery{
					Select: nil,
					Page:   0,
					Limit:  1,
				}, handler))

				assert.Equal(t, len(usrs), len(seen))
			}))
		})

	}))
}
