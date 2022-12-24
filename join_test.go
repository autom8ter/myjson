package gokvkit_test

import (
	"context"
	"testing"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/stretchr/testify/assert"
)

func TestJoin(t *testing.T) {
	t.Run("join user to account", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			var usrs = map[string]*gokvkit.Document{}
			assert.NoError(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				for i := 0; i < 100; i++ {
					u := testutil.NewUserDoc()
					usrs[u.GetString("_id")] = u
					assert.NoError(t, tx.Set(ctx, "user", u))
				}
				return nil
			}))
			assert.NoError(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				assert.NoError(t, tx.Set(ctx, "user", testutil.NewUserDoc()))
				return nil
			}))
			results, err := db.Query(ctx, "user", gokvkit.Q().
				Select(
					gokvkit.Select{Field: "acc._id", As: "account_id"},
					gokvkit.Select{Field: "acc.name", As: "account_name"},
					gokvkit.Select{Field: "_id", As: "user_id"},
				).
				Join(gokvkit.Join{
					Collection: "account",
					On: []gokvkit.Where{
						{
							Field: "_id",
							Op:    gokvkit.WhereOpEq,
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
		}))
	})
	t.Run("join account to user", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			accID := ""
			assert.NoError(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				doc := testutil.NewUserDoc()
				accID = doc.GetString("account_id")
				doc2 := testutil.NewUserDoc()
				assert.Nil(t, doc2.Set("account_id", accID))
				assert.NoError(t, tx.Set(ctx, "user", doc))
				assert.NoError(t, tx.Set(ctx, "user", doc2))
				return nil
			}))
			results, err := db.Query(ctx, "account", gokvkit.Q().
				Select(
					gokvkit.Select{Field: "_id", As: "account_id"},
					gokvkit.Select{Field: "name", As: "account_name"},
					gokvkit.Select{Field: "usr.name"},
				).
				Where(
					gokvkit.Where{
						Field: "_id",
						Op:    gokvkit.WhereOpEq,
						Value: accID,
					},
				).
				Join(gokvkit.Join{
					Collection: "user",
					On: []gokvkit.Where{
						{
							Field: "account_id",
							Op:    gokvkit.WhereOpEq,
							Value: "$_id",
						},
					},
					As: "usr",
				}).
				OrderBy(gokvkit.OrderBy{Field: "account_name", Direction: gokvkit.OrderByDirectionAsc}).
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
}
