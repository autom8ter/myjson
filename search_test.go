package wolverine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/autom8ter/wolverine"
)

func TestSearch(t *testing.T) {
	assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
		record := newUserDoc()
		record.Set("contact.email", myEmail)
		record.Set("account_id", 1)
		record.Set("language", "english")
		assert.Nil(t, db.Set(ctx, "user", record))
		for i := 0; i < 1000; i++ {
			assert.Nil(t, db.Set(ctx, "user", newUserDoc()))
		}
		t.Run("basic", func(t *testing.T) {
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
						Op:    wolverine.Basic,
						Value: myEmail,
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 1, len(results))
			assert.EqualValues(t, myEmail, results[0].Get("contact.email"))

			results, err = db.Search(ctx, "user", wolverine.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []wolverine.SearchWhere{
					{
						Field: "account_id",
						Op:    wolverine.Basic,
						Value: 2,
					},
					{
						Field: "language",
						Op:    wolverine.Basic,
						Value: "englis",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 0, len(results))

			results, err = db.Search(ctx, "user", wolverine.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []wolverine.SearchWhere{
					{
						Field: "account_id",
						Op:    wolverine.Basic,
						Value: 1,
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Greater(t, len(results), 0)
		})

		t.Run("prefix", func(t *testing.T) {
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
			assert.Equal(t, 1, len(results))
			assert.EqualValues(t, myEmail, results[0].Get("contact.email"))

			results, err = db.Search(ctx, "user", wolverine.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []wolverine.SearchWhere{
					{
						Field: "contact.email",
						Op:    wolverine.Prefix,
						Value: "colemanworz",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 0, len(results))

			results, err = db.Search(ctx, "user", wolverine.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []wolverine.SearchWhere{
					{
						Field: "contact.email",
						Op:    wolverine.Prefix,
						Value: "c",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Greater(t, len(results), 0)
		})

		t.Run("wildcard", func(t *testing.T) {
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
						Op:    wolverine.Wildcard,
						Value: "colemanword*",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 1, len(results))
			assert.EqualValues(t, myEmail, results[0].Get("contact.email"))

			results, err = db.Search(ctx, "user", wolverine.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []wolverine.SearchWhere{
					{
						Field: "contact.email",
						Op:    wolverine.Wildcard,
						Value: "colemanworz*",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 0, len(results))

			results, err = db.Search(ctx, "user", wolverine.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []wolverine.SearchWhere{
					{
						Field: "contact.email",
						Op:    wolverine.Wildcard,
						Value: "c*",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Greater(t, len(results), 0)
		})

		t.Run("date range", func(t *testing.T) {
			results, err := db.Search(ctx, "user", wolverine.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []wolverine.SearchWhere{
					{
						Field: "timestamp",
						Op:    wolverine.DateRange,
						Value: fmt.Sprintf("%s,%s", time.Time{}, time.Now()),
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Greater(t, len(results), 0)

		})

		t.Run("term range", func(t *testing.T) {
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
						Op:    wolverine.TermRange,
						Value: "colemanword,colemanword@gmail.comz",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 1, len(results))
			assert.EqualValues(t, myEmail, results[0].Get("contact.email"))

			results, err = db.Search(ctx, "user", wolverine.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []wolverine.SearchWhere{
					{
						Field: "contact.email",
						Op:    wolverine.TermRange,
						Value: "zzzzz,zzzzzzz",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Equal(t, 0, len(results))

			results, err = db.Search(ctx, "user", wolverine.SearchQuery{
				Select: []string{"name", "contact.email"},
				Where: []wolverine.SearchWhere{
					{
						Field: "contact.email",
						Op:    wolverine.TermRange,
						Value: "colemanword",
					},
				},
				Limit: 100,
			})
			assert.Nil(t, err)
			assert.Greater(t, len(results), 0)
		})
	}))
}
