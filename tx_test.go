package gokvkit_test

import (
	"context"
	"fmt"
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
	t.Run("cascade delete", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			assert.NoError(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
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
			assert.NoError(t, db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
				for i := 0; i <= 100; i++ {
					if err := tx.Delete(ctx, "account", fmt.Sprint(i)); err != nil {
						return err
					}
				}
				return nil
			}))
			results, err := db.Query(ctx, "account", gokvkit.Query{Select: []gokvkit.Select{{Field: "*"}}})
			assert.NoError(t, err)
			assert.Equal(t, 0, results.Count, "failed to delete accounts")
			results, err = db.Query(ctx, "user", gokvkit.Query{Select: []gokvkit.Select{{Field: "*"}}})
			assert.NoError(t, err)
			assert.Equal(t, 0, results.Count, "failed to cascade delete users")
			results, err = db.Query(ctx, "task", gokvkit.Query{Select: []gokvkit.Select{{Field: "*"}}})
			assert.NoError(t, err)
			assert.Equal(t, 0, results.Count, "failed to cascade delete tasks")
		}))
	})
}
