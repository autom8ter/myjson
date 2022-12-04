package gokvkit

import (
	"context"
	_ "embed"
	"github.com/autom8ter/gokvkit/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	//go:embed testutil/testdata/task.yaml
	taskSchema string
	//go:embed testutil/testdata/user.yaml
	userSchema string
)

func TestJSONSchema(t *testing.T) {
	t.Run("json schema validation - success", func(t *testing.T) {
		validator, err := newCollectionSchema([]byte(userSchema))
		assert.Nil(t, err)
		assert.Nil(t, validator.validateCommand(context.Background(), &model.Command{
			Action: model.Set,
			DocID:  "",
			Before: nil,
			After:  newUserDoc(),
		}))
	})
	t.Run("json schema validation - fail", func(t *testing.T) {
		validator, err := newCollectionSchema([]byte(taskSchema))
		assert.Nil(t, err)
		assert.NotNil(t, validator.validateCommand(context.Background(), &model.Command{
			Action: model.Set,
			DocID:  "",
			Before: nil,
			After:  newUserDoc(),
		}))
	})
}
