package gokvkit_test

import (
	"context"
	"testing"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/stretchr/testify/assert"
)

func TestTx(t *testing.T) {
	t.Run("set then get", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				doc := testutil.NewUserDoc()
				err := tx.Set(ctx, "user", doc)
				assert.Nil(t, err)
				d, err := tx.Get(ctx, "user", doc.GetString("_id"))
				assert.Nil(t, err)
				assert.NotNil(t, d)
				assert.Equal(t, doc.Get("contact.email"), d.GetString("contact.email"))
				return nil
			}))
		}))
	})
	t.Run("create then get", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				doc := testutil.NewUserDoc()
				id, err := tx.Create(ctx, "user", doc)
				assert.Nil(t, err)
				d, err := tx.Get(ctx, "user", id)
				assert.Nil(t, err)
				assert.NotNil(t, d)
				assert.Equal(t, doc.Get("contact.email"), d.GetString("contact.email"))
				return nil
			}))
		}))
	})
	t.Run("create then update then get", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				doc := testutil.NewUserDoc()
				id, err := tx.Create(ctx, "user", doc)
				assert.Nil(t, err)
				err = tx.Update(ctx, "user", id, map[string]any{
					"age": 10,
				})
				assert.Nil(t, err)
				d, err := tx.Get(ctx, "user", id)
				assert.Nil(t, err)
				assert.NotNil(t, d)
				assert.Equal(t, doc.Get("contact.email"), d.GetString("contact.email"))
				return nil
			}))
		}))
	})
	t.Run("create then delete then get", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			assert.Nil(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				doc := testutil.NewUserDoc()
				id, err := tx.Create(ctx, "user", doc)
				assert.Nil(t, err)
				err = tx.Delete(ctx, "user", id)
				assert.Nil(t, err)
				d, err := tx.Get(ctx, "user", id)
				assert.NotNil(t, err)
				assert.Nil(t, d)
				return nil
			}))
		}))
	})
}
