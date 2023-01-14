package myjson_test

import (
	"context"
	"testing"

	"github.com/autom8ter/myjson"
	"github.com/stretchr/testify/assert"
)

func TestMetadata(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		md := myjson.ExtractMetadata(context.Background())
		assert.NotNil(t, md)
		assert.Equal(t, "default", md.GetString(myjson.MetadataKeyNamespace))
	})

	t.Run("set context", func(t *testing.T) {
		ctx := context.Background()
		ctx = myjson.SetMetadataGroups(ctx, []string{"group1", "group2"})
		ctx = myjson.SetMetadataRoles(ctx, []string{"role1", "role2"})
		ctx = myjson.SetMetadataNamespace(ctx, "acme")
		ctx = myjson.SetMetadataUserID(ctx, "123")
		md := myjson.ExtractMetadata(ctx)
		assert.NotNil(t, md)
		assert.Equal(t, "acme", md.GetString(myjson.MetadataKeyNamespace))
		assert.Equal(t, []any{"role1", "role2"}, md.GetArray(myjson.MetadataKeyRoles))
		assert.Equal(t, []any{"group1", "group2"}, md.GetArray(myjson.MetadataKeyGroups))
		assert.Equal(t, "123", md.GetString(myjson.MetadataKeyUserID))
	})
}
