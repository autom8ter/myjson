package myjson

import (
	"context"
	"testing"

	_ "github.com/autom8ter/myjson/kv/badger"
	"github.com/stretchr/testify/assert"
)

func TestJavascript(t *testing.T) {
	t.Run("bool expression", func(t *testing.T) {
		ctx := context.Background()
		db, err := Open(ctx, "badger", nil)
		assert.NoError(t, err)
		defer db.Close(ctx)
		vm, err := getJavascriptVM(ctx, db, map[string]any{})
		assert.NoError(t, err)
		assert.NotNil(t, vm)
		doc := NewDocument()
		assert.NoError(t, doc.Set("age", 10))
		assert.NoError(t, vm.Set("doc", doc))
		v, err := vm.RunString(`doc.Get("age") > 5`)
		assert.NoError(t, err)
		res := v.Export().(bool)

		assert.True(t, res, v.String())
	})
	t.Run("fetch", func(t *testing.T) {
		ctx := context.Background()
		db, err := Open(ctx, "badger", nil)
		assert.NoError(t, err)
		defer db.Close(ctx)
		vm, err := getJavascriptVM(ctx, db, map[string]any{})
		assert.NoError(t, err)
		assert.NotNil(t, vm)
		var fetch = `
	fetch({
      method: "GET",
      url: "https://google.com",
      query: {
		q: "hello world"
      }
	})
`
		val, err := vm.RunString(fetch)
		assert.NoError(t, err)
		assert.NotNil(t, val)
	})
	t.Run("fetch error", func(t *testing.T) {
		ctx := context.Background()
		db, err := Open(ctx, "badger", nil)
		assert.NoError(t, err)
		defer db.Close(ctx)
		vm, err := getJavascriptVM(ctx, db, map[string]any{})
		assert.NoError(t, err)
		assert.NotNil(t, vm)
		var fetch = `
	fetch({
      method: "GET",
      url: "google.com",
      query: {
		q: "hello world"
      }
	})
`
		_, err = vm.RunString(fetch)
		assert.Error(t, err)
	})
}
