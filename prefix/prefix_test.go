package prefix_test

import (
	"bytes"
	"github.com/autom8ter/wolverine/prefix"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test(t *testing.T) {
	t.Run("next prefix", func(t *testing.T) {
		msg := []byte("hello world")
		pfx := prefix.PrefixNextKey(msg)
		t.Logf("next prefix = %s", string(pfx))
		assert.Greater(t, bytes.Compare(pfx, msg), 0)
	})
}
