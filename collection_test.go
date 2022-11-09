package brutus_test

import (
	"context"
	"github.com/autom8ter/brutus"
	"github.com/autom8ter/brutus/testutil"
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
		err := testutil.UserCollection.Validate(context.Background(), nil, &brutus.DocChange{
			Action: brutus.Set,
			After:  usr,
		})
		assert.Nil(t, err)
	})
	t.Run("primary index", func(t *testing.T) {
		assert.Equal(t, "_id", testutil.UserCollection.PrimaryKey())
		assert.Equal(t, "user", testutil.UserCollection.Name())
		err := testutil.UserCollection.Validate(context.Background(), nil, &brutus.DocChange{
			Action: brutus.Set,
			After:  testutil.NewUserDoc(),
		})
		assert.Nil(t, err)
		err = testutil.UserCollection.Validate(context.Background(), nil, &brutus.DocChange{
			Action: brutus.Set,
			After:  testutil.NewTaskDoc("1"),
		})
		assert.NotNil(t, err)
	})
}
