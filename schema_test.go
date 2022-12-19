package gokvkit

import (
	"context"
	_ "embed"
	"testing"

	"github.com/autom8ter/gokvkit/model"
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
		assert.NotNil(t, schema.ValidateDocument(context.Background(), model.NewDocument()))
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
	schema, err := newCollectionSchema([]byte(userSchema))
	assert.Nil(t, err)
	assert.NotNil(t, schema.Indexing())
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
}
