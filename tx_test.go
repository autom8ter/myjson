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
			assert.Nil(t, db.Tx(ctx, gokvkit.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
				doc := testutil.NewUserDoc()
				err := tx.Set(ctx, "user", doc)
				assert.NoError(t, err)
				d, err := tx.Get(ctx, "user", doc.GetString("_id"))
				assert.NoError(t, err)
				assert.NotNil(t, d)
				assert.Equal(t, doc.Get("contact.email"), d.GetString("contact.email"))
				return nil
			}))
		}))
	})
	t.Run("create then get", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			assert.Nil(t, db.Tx(ctx, gokvkit.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
				doc := testutil.NewUserDoc()
				id, err := tx.Create(ctx, "user", doc)
				assert.NoError(t, err)
				d, err := tx.Get(ctx, "user", id)
				assert.NoError(t, err)
				assert.NotNil(t, d)
				assert.Equal(t, doc.Get("contact.email"), d.GetString("contact.email"))
				return nil
			}))
		}))
	})
	t.Run("create then update then get", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			assert.Nil(t, db.Tx(ctx, gokvkit.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
				doc := testutil.NewUserDoc()
				id, err := tx.Create(ctx, "user", doc)
				assert.NoError(t, err)
				err = tx.Update(ctx, "user", id, map[string]any{
					"age": 10,
				})
				assert.NoError(t, err)
				d, err := tx.Get(ctx, "user", id)
				assert.NoError(t, err)
				assert.NotNil(t, d)
				assert.Equal(t, doc.Get("contact.email"), d.GetString("contact.email"))
				return nil
			}))
		}))
	})
	t.Run("create then delete then get", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			assert.Nil(t, db.Tx(ctx, gokvkit.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
				doc := testutil.NewUserDoc()
				id, err := tx.Create(ctx, "user", doc)
				assert.NoError(t, err)
				err = tx.Delete(ctx, "user", id)
				assert.NoError(t, err)
				d, err := tx.Get(ctx, "user", id)
				assert.NotNil(t, err)
				assert.Nil(t, d)
				return nil
			}))
		}))
	})
	t.Run("set 10 then forEach", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			assert.Nil(t, db.Tx(ctx, gokvkit.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
				var usrs = map[string]*gokvkit.Document{}
				for i := 0; i < 10; i++ {
					doc := testutil.NewUserDoc()
					err := tx.Set(ctx, "user", doc)
					assert.NoError(t, err)
					usrs[doc.GetString("_id")] = doc
				}
				var count = 0
				_, err := tx.ForEach(ctx, "user", gokvkit.ForEachOpts{}, func(d *gokvkit.Document) (bool, error) {
					assert.NotEmpty(t, usrs[d.GetString("_id")])
					count++
					return true, nil
				})
				assert.NoError(t, err)
				assert.Equal(t, 10, count)
				return nil
			}))
		}))
	})
	t.Run("set 10 then check cdc", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			assert.Nil(t, db.Tx(ctx, gokvkit.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
				md := gokvkit.NewMetadata(map[string]any{
					"testing": true,
				})
				var usrs = map[string]*gokvkit.Document{}
				for i := 0; i < 10; i++ {
					doc := testutil.NewUserDoc()
					err := tx.Set(md.ToContext(ctx), "user", doc)
					assert.NoError(t, err)
					usrs[doc.GetString("_id")] = doc
					assert.Equal(t, "user", tx.CDC()[i].Collection)
					assert.EqualValues(t, gokvkit.Set, tx.CDC()[i].Action)
					assert.EqualValues(t, doc.Get("_id"), tx.CDC()[i].DocumentID)
					assert.NotEmpty(t, tx.CDC()[i].Metadata)
					assert.NotEmpty(t, tx.CDC()[i].Diff)
					v, _ := tx.CDC()[i].Metadata.Get("testing")
					assert.Equal(t, true, v)
				}
				assert.Equal(t, 10, len(tx.CDC()))
				return nil
			}))
		}))
	})
	t.Run("DB() not nil", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
			assert.Nil(t, db.Tx(ctx, gokvkit.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
				assert.NotNil(t, tx.DB())
				return nil
			}))
		}))
	})
}
