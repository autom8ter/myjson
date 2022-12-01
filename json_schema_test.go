package gokvkit_test

import (
	"context"
	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJSONSchema(t *testing.T) {
	t.Run("json schema validation - success", func(t *testing.T) {
		validator, err := gokvkit.JSONSchema([]byte(testutil.UserSchema))
		assert.Nil(t, err)
		assert.Nil(t, validator.Valid())
		assert.Nil(t, validator.Func(context.Background(), nil, &gokvkit.Command{
			Action: gokvkit.SetDocument,
			DocID:  "",
			Before: nil,
			Change: testutil.NewUserDoc(),
		}))
	})
	t.Run("json schema validation - fail", func(t *testing.T) {
		_, err := gokvkit.JSONSchema([]byte("hello world"))
		assert.NotNil(t, err)

		validator, err := gokvkit.JSONSchema([]byte(testutil.UserSchema))
		assert.Nil(t, err)
		assert.NotNil(t, validator.Func(context.Background(), nil, &gokvkit.Command{
			Action: gokvkit.SetDocument,
			DocID:  "",
			Before: nil,
			Change: gokvkit.NewDocument(),
		}))
	})
}
