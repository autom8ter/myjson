package gokvkit

import (
	"context"
	_ "embed"
	"fmt"
	"testing"

	"github.com/autom8ter/gokvkit/util"
	"github.com/stretchr/testify/assert"
)

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
	t.Run("indexing not nil", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(userSchema))
		assert.NoError(t, err)
		assert.NotNil(t, schema.Indexing())
	})
	t.Run("MarshalJSON/UnmarshalJSON", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(userSchema))
		assert.NoError(t, err)
		bits, _ := schema.MarshalJSON()
		assert.NoError(t, schema.UnmarshalJSON(bits))
	})
	t.Run("MarshalYAML/UnmarshalYAML", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(userSchema))
		assert.NoError(t, err)
		before, _ := schema.MarshalJSON()
		bits, _ := schema.MarshalYAML()
		assert.NoError(t, schema.UnmarshalYAML(bits))
		after, _ := schema.MarshalJSON()
		assert.JSONEq(t, string(before), string(after))
	})
	t.Run("properties", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(userSchema))
		assert.NoError(t, err)
		assert.Equal(t, "age", schema.Properties()["age"].Name)
		assert.Equal(t, "name", schema.Properties()["name"].Name)
		assert.Equal(t, "account_id", schema.Properties()["account_id"].Name)
	})
	t.Run("properties foreign", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(userSchema))
		assert.NoError(t, err)
		assert.Equal(t, "account", schema.Properties()["account_id"].ForeignKey.Collection)
		assert.Equal(t, true, schema.Properties()["account_id"].ForeignKey.Cascade)
		assert.Equal(t, true, schema.PropertyPaths()["account_id"].ForeignKey.Cascade)
	})
	t.Run("properties nested", func(t *testing.T) {
		schema, err := newCollectionSchema([]byte(userSchema))
		assert.NoError(t, err)
		assert.Equal(t, "email", schema.Properties()["contact"].Properties["email"].Name)
		assert.Equal(t, "contact.email", schema.Properties()["contact"].Properties["email"].Path)
		assert.Equal(t, "contact.email", schema.PropertyPaths()["contact.email"].Path)
	})

}
