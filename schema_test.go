package gokvkit

import (
	"context"
	_ "embed"
	"fmt"
	"testing"

	"github.com/autom8ter/gokvkit/util"
	"github.com/stretchr/testify/assert"
)

func TestJSONSchema(t *testing.T) {
	t.Run("json schema validation", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(userSchema))
		assert.NoError(t, err)
		assert.NoError(t, schema.ValidateDocument(context.Background(), newUserDoc()))
		assert.Error(t, schema.ValidateDocument(context.Background(), NewDocument()))
	})
	t.Run("primary key", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(taskSchema))
		assert.NoError(t, err)
		assert.Equal(t, "_id", schema.PrimaryKey())
	})
	t.Run("collection", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(taskSchema))
		assert.NoError(t, err)
		assert.Equal(t, "task", schema.Collection())
	})

	schema, err := newCollectionSchema([]byte(userSchema))
	assert.NoError(t, err)
	assert.NotNil(t, schema.Indexing())
}

func TestSchema(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(userSchema))
		assert.NoError(t, err)
		fmt.Println(util.JSONString(schema.Indexing()))
		assert.NotNil(t, schema.Indexing())
		assert.NotEmpty(t, schema.Indexing()["_id.primaryidx"])
		assert.NotEmpty(t, schema.Indexing()["account_id.foreignidx"])
		assert.NotEmpty(t, schema.Indexing()["contact.email.uniqueidx"])
		assert.NotEmpty(t, schema.Indexing()["account_email_idx"])
		assert.NotEmpty(t, schema.Indexing()["language_idx"])
	})
}
