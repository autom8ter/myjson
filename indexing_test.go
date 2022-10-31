package wolverine

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPrefixNextKey(t *testing.T) {
	t.Run("next prefix", func(t *testing.T) {
		msg := []byte("hello world")
		i := indexPathPrefix{prefix: [][]byte{
			[]byte("hello world"),
		}}
		pfx := i.NextPrefix()
		t.Logf("next prefix = %s", string(pfx))
		assert.Greater(t, bytes.Compare(pfx, msg), 0)
	})
}
