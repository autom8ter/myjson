package brutus_test

import (
	"context"
	"github.com/autom8ter/brutus"
	"github.com/autom8ter/brutus/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJSONSchema(t *testing.T) {
	t.Run("json schema validation - success", func(t *testing.T) {
		validator, err := brutus.JSONSchema([]byte(testutil.UserSchema))
		assert.Nil(t, err)
		assert.Nil(t, validator.Valid())
		assert.Nil(t, validator.Func(context.Background(), nil, &brutus.DocChange{
			Action: brutus.Set,
			DocID:  "",
			Before: nil,
			After:  testutil.NewUserDoc(),
		}))
	})
	t.Run("json schema validation - fail", func(t *testing.T) {
		_, err := brutus.JSONSchema([]byte("hello world"))
		assert.NotNil(t, err)

		validator, err := brutus.JSONSchema([]byte(testutil.UserSchema))
		assert.Nil(t, err)
		assert.NotNil(t, validator.Func(context.Background(), nil, &brutus.DocChange{
			Action: brutus.Set,
			DocID:  "",
			Before: nil,
			After:  brutus.NewDocument(),
		}))
	})
}
