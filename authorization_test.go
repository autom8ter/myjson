package myjson

import (
	"context"
	"testing"

	"github.com/autom8ter/myjson/kv"
	"github.com/stretchr/testify/assert"
)

func TestAuthorization(t *testing.T) {
	t.Run("set as super user (allow)", func(t *testing.T) {
		ctx := SetMetadataRoles(context.Background(), []string{"super_user"})
		db, err := Open(ctx, "badger", map[string]any{})
		assert.NoError(t, err)
		assert.NoError(t, db.Configure(ctx, []string{accountSchema, userSchema, taskSchema}))
		assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx Tx) error {
			_, err := tx.Create(ctx, "account", db.NewDoc().Set(map[string]any{
				"name": "acme",
			}).Doc())
			return err
		}))
	})
	t.Run("set as readonly user (deny)", func(t *testing.T) {
		ctx := SetMetadataRoles(context.Background(), []string{"read_only"})
		db, err := Open(ctx, "badger", map[string]any{})
		assert.NoError(t, err)
		assert.NoError(t, db.Configure(ctx, []string{accountSchema, userSchema, taskSchema}))
		assert.Error(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx Tx) error {
			_, err := tx.Create(ctx, "account", db.NewDoc().Set(map[string]any{
				"name": "acme",
			}).Doc())
			return err
		}))
	})
	t.Run("set as no role user (deny)", func(t *testing.T) {
		ctx := context.Background()
		db, err := Open(ctx, "badger", map[string]any{})
		assert.NoError(t, err)
		assert.NoError(t, db.Configure(ctx, []string{accountSchema, userSchema, taskSchema}))
		assert.Error(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx Tx) error {
			_, err := tx.Create(ctx, "account", db.NewDoc().Set(map[string]any{
				"name": "acme",
			}).Doc())
			return err
		}))
	})
	t.Run("read other account as readonly user (deny)", func(t *testing.T) {
		ctx := SetMetadataRoles(context.Background(), []string{"read_only"})
		db, err := Open(ctx, "badger", map[string]any{})
		assert.NoError(t, err)
		assert.NoError(t, db.Configure(ctx, []string{accountSchema, userSchema, taskSchema}))
		assert.Error(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx Tx) error {
			_, err := tx.Get(ctx, "account", "1")
			return err
		}))
	})
	t.Run("read account as readonly user with proper group (allow)", func(t *testing.T) {
		ctx := context.Background()
		db, err := Open(ctx, "badger", map[string]any{})
		assert.NoError(t, err)
		ctx = SetMetadataRoles(context.Background(), []string{"super_user"})
		assert.NoError(t, db.Configure(ctx, []string{accountSchema, userSchema, taskSchema}))
		assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx Tx) error {
			assert.NoError(t, tx.Set(ctx, "account", db.NewDoc().Set(map[string]any{
				"_id":  "1",
				"name": "acme",
			}).Doc()))
			_, err := tx.Get(ctx, "account", "1")
			return err
		}))
		ctx = SetMetadataRoles(context.Background(), []string{"read_only"})
		ctx = SetMetadataGroups(context.Background(), []string{"1"})
		assert.NoError(t, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx Tx) error {
			_, err := tx.Get(ctx, "account", "1")
			return err
		}))
	})
}
