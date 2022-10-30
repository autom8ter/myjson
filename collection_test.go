package wolverine_test

import (
	"context"
	"github.com/autom8ter/wolverine"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollection(t *testing.T) {
	usr := wolverine.NewUserDoc()
	t.Run("validate", func(t *testing.T) {
		assert.NotNil(t, wolverine.UserCollection.Collection())
		assert.NotNil(t, wolverine.UserCollection.Indexing())
		assert.NotNil(t, wolverine.TaskCollection.Collection())
		assert.NotNil(t, wolverine.TaskCollection.Indexing())
		err := wolverine.UserCollection.Validate(context.Background(), usr.Bytes())
		assert.Nil(t, err)
	})
	t.Run("primary index", func(t *testing.T) {
		assert.Equal(t, "_id", wolverine.UserCollection.PrimaryKey())
		assert.Equal(t, true, wolverine.UserCollection.Indexing().HasIndexes())
		assert.Equal(t, "user", wolverine.UserCollection.Collection())
		err := wolverine.UserCollection.Validate(context.Background(), wolverine.NewUserDoc().Bytes())
		assert.Nil(t, err)
		err = wolverine.UserCollection.Validate(context.Background(), wolverine.NewDocument().Bytes())
		assert.NotNil(t, err)
		wolverine.UserCollection.PrimaryIndex()
	})
	t.Run("load collections from dir", func(t *testing.T) {
		collections, err := wolverine.LoadCollectionsFromDir("testdata/")
		assert.Nil(t, err)
		assert.NotEqual(t, 0, len(collections))
	})
}
