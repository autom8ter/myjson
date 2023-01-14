package myjson_test

import (
	"context"
	"testing"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/testutil"
	"github.com/stretchr/testify/assert"
)

func TestTx(t *testing.T) {
	t.Run("set then get", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
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
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
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
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
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
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
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
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				var usrs = map[string]*myjson.Document{}
				for i := 0; i < 10; i++ {
					doc := testutil.NewUserDoc()
					err := tx.Set(ctx, "user", doc)
					assert.NoError(t, err)
					usrs[doc.GetString("_id")] = doc
				}
				var count = 0
				_, err := tx.ForEach(ctx, "user", myjson.ForEachOpts{}, func(d *myjson.Document) (bool, error) {
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

	t.Run("DB() not nil", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				assert.NotNil(t, tx.DB())
				return nil
			}))
		}))
	})
	t.Run("cmd - no cmds", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				result := tx.Cmd(ctx, myjson.TxCmd{
					Create: nil,
					Get:    nil,
					Set:    nil,
					Update: nil,
					Delete: nil,
					Query:  nil,
				})
				assert.Error(t, result.Error)
				return nil
			}))
		}))
	})
	t.Run("cmd - set then get", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				result := tx.Cmd(ctx, myjson.TxCmd{
					Set: &myjson.SetCmd{Collection: "user", Document: testutil.NewUserDoc()},
				})
				assert.Nil(t, result.Error)
				result2 := tx.Cmd(ctx, myjson.TxCmd{
					Get: &myjson.GetCmd{Collection: "user", ID: result.Set.GetString("_id")},
				})
				assert.Nil(t, result2.Error)
				assert.JSONEq(t, result.Set.String(), result2.Get.String())
				return nil
			}))
		}))
	})
	t.Run("cmd - set then update then get", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				result := tx.Cmd(ctx, myjson.TxCmd{
					Set: &myjson.SetCmd{Collection: "user", Document: testutil.NewUserDoc()},
				})
				assert.Nil(t, result.Error)
				id := result.Set.GetString("_id")
				result2 := tx.Cmd(ctx, myjson.TxCmd{
					Update: &myjson.UpdateCmd{
						Collection: "user",
						ID:         id,
						Update: map[string]any{
							"age": 20,
						},
					},
				})
				assert.Nil(t, result2.Error)
				result3 := tx.Cmd(ctx, myjson.TxCmd{
					Get: &myjson.GetCmd{Collection: "user", ID: id},
				})
				assert.Nil(t, result3.Error)
				assert.JSONEq(t, result2.Update.String(), result3.Get.String())
				return nil
			}))
		}))
	})
	t.Run("cmd - set then delete", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				result := tx.Cmd(ctx, myjson.TxCmd{
					Set: &myjson.SetCmd{Collection: "user", Document: testutil.NewUserDoc()},
				})
				assert.Nil(t, result.Error)
				id := result.Set.GetString("_id")
				result = tx.Cmd(ctx, myjson.TxCmd{
					Delete: &myjson.DeleteCmd{
						Collection: "user",
						ID:         id,
					},
				})
				assert.Nil(t, result.Error)
				return nil
			}))
		}))
	})
	t.Run("cmd - query accounts", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			assert.Nil(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				result := tx.Cmd(ctx, myjson.TxCmd{
					Query: &myjson.QueryCmd{Collection: "account", Query: myjson.Query{}},
				})
				assert.Nil(t, result.Error)
				assert.NotEqual(t, 0, len(result.Query.Documents))
				return nil
			}))
		}))
	})
}
