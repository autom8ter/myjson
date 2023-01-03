package myjson

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpts(t *testing.T) {
	t.Run("test persist cdc opt", func(t *testing.T) {
		d := &defaultDB{}
		WithPersistCDC(true)(d)
		assert.True(t, d.persistCDC)
	})
}
