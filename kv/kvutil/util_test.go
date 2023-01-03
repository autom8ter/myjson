package kvutil_test

import (
	"bytes"
	"github.com/autom8ter/myjson/kv/kvutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKVUtil(t *testing.T) {
	const input = "hello"
	next := kvutil.NextPrefix([]byte(input))
	assert.Equal(t, 1, bytes.Compare(next, []byte(input)))
}
