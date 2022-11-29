package gokvkit_test

import (
	"context"
	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCollection(t *testing.T) {
	t.Run("check primary key", func(t *testing.T) {
		c := gokvkit.NewCollection("user", "_id", gokvkit.WithIndex(gokvkit.Index{
			Collection: "user",
			Name:       "contact.email",
			Fields:     []string{"contact.email"},
			Unique:     true,
			Primary:    false,
		}))
		assert.Equal(t, "_id", c.PrimaryKey())
		assert.Equal(t, 2, len(c.Indexes()))
		assert.Nil(t, c.Validate())
	})
	t.Run("check indexes", func(t *testing.T) {
		c := gokvkit.NewCollection("user", "_id", gokvkit.WithIndex(gokvkit.Index{
			Collection: "user",
			Name:       "contact.email",
			Fields:     []string{"contact.email"},
			Unique:     true,
			Primary:    false,
		}))
		assert.Equal(t, 2, len(c.Indexes()))
		assert.Nil(t, c.Validate())
	})
	t.Run("check collection name", func(t *testing.T) {
		c := gokvkit.NewCollection("user", "_id", gokvkit.WithIndex(gokvkit.Index{
			Collection: "user",
			Name:       "contact.email",
			Fields:     []string{"contact.email"},
			Unique:     true,
			Primary:    false,
		}))
		assert.Equal(t, "user", c.Name())
		assert.Nil(t, c.Validate())
	})
	t.Run("with read hook", func(t *testing.T) {
		c := gokvkit.NewCollection("user", "_id", gokvkit.WithReadHooks(gokvkit.ReadHook{
			Name: "timestamp_hook",
			Func: func(ctx context.Context, db *gokvkit.DB, document *gokvkit.Document) (*gokvkit.Document, error) {
				return document, document.Set("read_at", time.Now())
			},
		}))
		assert.Equal(t, "timestamp_hook", c.ReadHooks()[0].Name)
		assert.Nil(t, c.Validate())
	})
	t.Run("with where hook", func(t *testing.T) {
		c := gokvkit.NewCollection("user", "_id", gokvkit.WithWhereHook(gokvkit.WhereHook{
			Name: "account_id_hook",
			Func: func(ctx context.Context, db *gokvkit.DB, where []gokvkit.Where) ([]gokvkit.Where, error) {
				contxt, _ := gokvkit.GetMetadata(ctx)
				accID, _ := contxt.Get("account_id")
				if accID != "" {
					where = append(where, gokvkit.Where{
						Field: "account_id",
						Op:    gokvkit.Eq,
						Value: accID,
					})
				}
				return where, nil
			},
		}))
		assert.Equal(t, "account_id_hook", c.WhereHooks()[0].Name)
		assert.Nil(t, c.Validate())
	})
	t.Run("with sideEffect hook", func(t *testing.T) {
		c := gokvkit.NewCollection("user", "_id", gokvkit.WithSideEffects(gokvkit.SideEffectHook{
			Name: "updated_at_hook",
			Func: func(ctx context.Context, db *gokvkit.DB, change *gokvkit.DocChange) (*gokvkit.DocChange, error) {
				return change, change.After.Set("updated_at", time.Now())
			},
		}))
		assert.Equal(t, "updated_at_hook", c.SideEffectHooks()[0].Name)
		assert.Nil(t, c.Validate())
	})
	t.Run("get primary key", func(t *testing.T) {
		c := gokvkit.NewCollection("user", "_id")
		doc := testutil.NewUserDoc()
		id := c.GetPrimaryKey(doc)
		assert.Equal(t, doc.Get("_id"), id)
		assert.Nil(t, c.Validate())
	})
	t.Run("set primary key", func(t *testing.T) {
		c := gokvkit.NewCollection("user", "_id")
		doc := testutil.NewUserDoc()
		assert.Nil(t, c.SetPrimaryKey(doc, "1"))
		assert.Equal(t, "1", c.GetPrimaryKey(doc))
		assert.Nil(t, c.Validate())
	})
}
