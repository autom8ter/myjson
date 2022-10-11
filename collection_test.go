package wolverine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollection(t *testing.T) {

	usr := newUserDoc()
	t.Run("validate", func(t *testing.T) {
		valid, err := defaultCollections[0].Validate(usr)
		assert.Nil(t, err)
		assert.True(t, valid)
	})
}
