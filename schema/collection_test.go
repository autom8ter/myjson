package schema_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/autom8ter/wolverine/internal/testutil"
)

func TestCollection(t *testing.T) {
	usr := testutil.NewUserDoc()
	t.Run("validate", func(t *testing.T) {
		assert.Nil(t, testutil.UserCollection.ParseSchema())
		assert.NotNil(t, testutil.UserCollection.Collection())
		assert.NotNil(t, testutil.UserCollection.Indexing())
		assert.True(t, testutil.UserCollection.Indexing().HasSearchIndex())
		assert.NotNil(t, testutil.TaskCollection.Collection())
		assert.NotNil(t, testutil.TaskCollection.Indexing())
		assert.False(t, testutil.TaskCollection.Indexing().HasSearchIndex())
		valid, err := testutil.UserCollection.Validate(usr)
		assert.Nil(t, err)
		assert.True(t, valid)
	})
}
