package schema_test

import (
	"context"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/autom8ter/wolverine/schema"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollection(t *testing.T) {
	usr := testutil.NewUserDoc()
	t.Run("validate", func(t *testing.T) {
		assert.NotNil(t, testutil.UserCollection.Collection())
		assert.NotNil(t, testutil.UserCollection.Indexing())
		assert.True(t, testutil.UserCollection.Indexing().HasSearchIndex())
		assert.NotNil(t, testutil.TaskCollection.Collection())
		assert.NotNil(t, testutil.TaskCollection.Indexing())
		assert.False(t, testutil.TaskCollection.Indexing().HasSearchIndex())
		err := testutil.UserCollection.Validate(context.Background(), usr.Bytes())
		assert.Nil(t, err)
	})
	t.Run("primary index", func(t *testing.T) {
		assert.Equal(t, "_id", testutil.UserCollection.PKey())
		assert.Equal(t, true, testutil.UserCollection.Indexing().HasSearchIndex())
		assert.Equal(t, true, testutil.UserCollection.Indexing().HasQueryIndex())
		assert.Equal(t, "user", testutil.UserCollection.Collection())
		err := testutil.UserCollection.Validate(context.Background(), testutil.NewUserDoc().Bytes())
		assert.Nil(t, err)
		err = testutil.UserCollection.Validate(context.Background(), schema.NewDocument().Bytes())
		assert.NotNil(t, err)
		testutil.UserCollection.PrimaryQueryIndex()
	})
	t.Run("load collections from dir", func(t *testing.T) {
		collections, err := schema.LoadCollectionsFromDir("../internal/testutil/")
		assert.Nil(t, err)
		assert.NotEqual(t, 0, len(collections))
	})
}
