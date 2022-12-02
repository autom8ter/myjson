package gokvkit

import (
	"context"
	_ "embed"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	//go:embed testutil/testdata/task.json
	taskSchema string
	//go:embed testutil/testdata/user.json
	userSchema string
)

func TestJSONSchema(t *testing.T) {
	t.Run("json schema validation - success", func(t *testing.T) {
		validator, err := newCollectionSchema([]byte(userSchema))
		assert.Nil(t, err)
		assert.Nil(t, validator.validateCommand(context.Background(), &Command{
			Action: SetDocument,
			DocID:  "",
			Before: nil,
			Change: newUserDoc(),
		}))
	})
	t.Run("json schema validation - fail", func(t *testing.T) {
		validator, err := newCollectionSchema([]byte(taskSchema))
		assert.Nil(t, err)
		assert.NotNil(t, validator.validateCommand(context.Background(), &Command{
			Action: SetDocument,
			DocID:  "",
			Before: nil,
			Change: newUserDoc(),
		}))
	})
}
