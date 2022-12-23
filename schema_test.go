package gokvkit

import (
	"context"
	_ "embed"
	"fmt"
	"testing"

	"github.com/autom8ter/gokvkit/util"
	"github.com/stretchr/testify/assert"
)

var (
	//go:embed testutil/testdata/task.yaml
	taskSchema string
	//go:embed testutil/testdata/user.yaml
	userSchema string
)

func TestJSONSchema(t *testing.T) {
	t.Run("json schema validation", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(userSchema))
		assert.Nil(t, err)
		assert.Nil(t, schema.ValidateDocument(context.Background(), newUserDoc()))
		assert.NotNil(t, schema.ValidateDocument(context.Background(), NewDocument()))
	})
	t.Run("primary key", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(taskSchema))
		assert.Nil(t, err)
		assert.Equal(t, "_id", schema.PrimaryKey())
	})
	t.Run("collection", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(taskSchema))
		assert.Nil(t, err)
		assert.Equal(t, "task", schema.Collection())
	})
	t.Run("expected errors", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(taskSchema))
		assert.Nil(t, err)
		assert.NotNil(t, schema.DelIndex(schema.PrimaryIndex().Name))
		assert.NotNil(t, schema.SetIndex(schema.PrimaryIndex()))
	})
	t.Run("del / set index", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(userSchema))
		assert.Nil(t, err)
		before := schema.Indexing()["email_idx"]
		assert.NotEmpty(t, before)
		assert.Nil(t, schema.DelIndex("email_idx"))
		assert.Empty(t, schema.Indexing()["email_idx"])
		assert.Nil(t, schema.SetIndex(before))
		assert.NotEmpty(t, schema.Indexing()["email_idx"])
	})

	schema, err := newCollectionSchema([]byte(userSchema))
	assert.Nil(t, err)
	assert.NotNil(t, schema.Indexing())
	assert.NotNil(t, schema.Properties())
	for k, v := range schema.Indexing() {
		assert.NotEmpty(t, k)
		assert.NotEmpty(t, v.Fields)
		assert.NotEmpty(t, v.Name)
		if v.Primary {
			assert.NotNil(t, schema.SetIndex(v))
		} else {
			assert.Nil(t, schema.SetIndex(v))
		}
	}
	assert.Equal(t, 9, len(schema.Properties()))
	for k, v := range schema.Properties() {
		fmt.Println(k, util.JSONString(v))
	}
}
