package wolverine_test

import (
	"context"
	"github.com/autom8ter/wolverine"
	"github.com/autom8ter/wolverine/testutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollection(t *testing.T) {
	usr := testutil.NewUserDoc()
	t.Run("validate", func(t *testing.T) {
		assert.NotNil(t, testutil.UserCollection.Name())
		assert.NotNil(t, testutil.UserCollection.Indexes())
		assert.NotNil(t, testutil.TaskCollection.Name())
		assert.NotNil(t, testutil.TaskCollection.Indexes())
		err := testutil.UserCollection.Validate(context.Background(), usr)
		assert.Nil(t, err)
	})
	t.Run("primary index", func(t *testing.T) {
		assert.Equal(t, "_id", testutil.UserCollection.PrimaryKey())
		assert.Equal(t, "user", testutil.UserCollection.Name())
		err := testutil.UserCollection.Validate(context.Background(), testutil.NewUserDoc())
		assert.Nil(t, err)
		err = testutil.UserCollection.Validate(context.Background(), wolverine.NewDocument())
		assert.NotNil(t, err)
	})
}
