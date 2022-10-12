package wolverine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/autom8ter/wolverine"
	"github.com/autom8ter/wolverine/internal/testutil"
)

func TestPaginate(t *testing.T) {
	t.Run("query paginate", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(testutil.AllCollections, func(ctx context.Context, db wolverine.DB) {
			var usrs []*wolverine.Document
			for i := 0; i < 10; i++ {
				u := testutil.NewUserDoc()
				usrs = append(usrs, u)
			}
			assert.Nil(t, db.BatchSet(ctx, "user", usrs))
			seen := map[string]struct{}{}
			handler := func(documents []*wolverine.Document) bool {
				for _, doc := range documents {
					if _, ok := seen[doc.GetID()]; ok {
						t.Fatal("duplicate doc", doc.GetID())
					}
					seen[doc.GetID()] = struct{}{}
				}
				return true
			}
			assert.Nil(t, db.RunScript(ctx, wolverine.PaginateQuery("user", wolverine.Query{
				Select:  nil,
				Page:    0,
				Limit:   1,
				OrderBy: wolverine.OrderBy{},
			}, handler)))

			assert.Equal(t, len(usrs), len(seen))
		}))
	})
	t.Run("search paginate", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(testutil.AllCollections, func(ctx context.Context, db wolverine.DB) {
			var usrs []*wolverine.Document
			for i := 0; i < 10; i++ {
				u := testutil.NewUserDoc()
				usrs = append(usrs, u)
			}
			assert.Nil(t, db.BatchSet(ctx, "user", usrs))
			seen := map[string]struct{}{}
			handler := func(documents []*wolverine.Document) bool {
				for _, doc := range documents {
					if _, ok := seen[doc.GetID()]; ok {
						t.Fatal("duplicate doc", doc.GetID())
					}
					seen[doc.GetID()] = struct{}{}
				}
				return true
			}
			assert.Nil(t, db.RunScript(ctx, wolverine.PaginateSearch("user", wolverine.SearchQuery{
				Select: nil,
				Page:   0,
				Limit:  1,
			}, handler)))

			assert.Equal(t, len(usrs), len(seen))
		}))
	})
}
