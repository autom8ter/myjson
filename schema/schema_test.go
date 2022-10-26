package schema_test

import (
	"context"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/autom8ter/wolverine/internal/util"
	"github.com/autom8ter/wolverine/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSchema(t *testing.T) {
	bits, err := testutil.UserCollection.Schema().MarshalJSON()
	assert.Nil(t, err)
	t.Run("newJSONSchema", func(t *testing.T) {
		_, err := schema.NewJSONSchema(bits)
		assert.Nil(t, err)
	})
	t.Run("validate", func(t *testing.T) {
		s, err := schema.NewJSONSchema(bits)
		assert.Nil(t, err)
		assert.Nil(t, s.Validate(context.Background(), []byte(util.JSONString(testutil.NewUserDoc()))))
	})
	t.Run("config", func(t *testing.T) {
		s, err := schema.NewJSONSchema(bits)
		assert.Nil(t, err)
		assert.Nil(t, s.Config().Validate())
		assert.NotEmpty(t, s.Config().PrimaryKey)
		assert.NotEmpty(t, s.Config().Collection)
		assert.NotEmpty(t, s.Config().Indexing.Query)
		assert.NotEmpty(t, s.Config().Indexing.Search)
		assert.NotNil(t, s.Config().ForeignKeys)
	})
}
