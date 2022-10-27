package core_test

import (
	"context"
	"github.com/autom8ter/wolverine/core"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/autom8ter/wolverine/internal/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSchema(t *testing.T) {
	usrBytes, err := testutil.UserCollection.MarshalJSON()
	assert.Nil(t, err)
	taskBytes, err := testutil.TaskCollection.MarshalJSON()
	assert.Nil(t, err)
	t.Run("newJSONSchema", func(t *testing.T) {
		_, err := core.NewJSONSchema(usrBytes)
		assert.Nil(t, err)
		_, err = core.NewJSONSchema(taskBytes)
		assert.Nil(t, err)
	})
	t.Run("validate", func(t *testing.T) {
		s, err := core.NewJSONSchema(usrBytes)
		assert.Nil(t, err)
		assert.Nil(t, s.Validate(context.Background(), []byte(util.JSONString(testutil.NewUserDoc()))))

		s, err = core.NewJSONSchema(taskBytes)
		assert.Nil(t, err)
		assert.Nil(t, s.Validate(context.Background(), []byte(util.JSONString(testutil.NewTaskDoc("1")))))
	})
	t.Run("config", func(t *testing.T) {
		s, err := core.NewJSONSchema(usrBytes)
		assert.Nil(t, err)
		assert.NotEmpty(t, s.PrimaryKey)
		assert.EqualValues(t, "_id", s.PrimaryKey())
		assert.NotEmpty(t, s.Collection())
		assert.NotEmpty(t, s.Indexing().Indexes)

		s, err = core.NewJSONSchema(taskBytes)
		assert.Nil(t, err)
		assert.NotEmpty(t, s.PrimaryKey())
		assert.NotEmpty(t, s.Collection())
	})
}
