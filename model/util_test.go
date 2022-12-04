package model

import (
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUtil(t *testing.T) {
	t.Run("compareField", func(t *testing.T) {
		d, err := NewDocumentFrom(map[string]any{
			"age":    50,
			"name":   "coleman",
			"isMale": true,
		})
		assert.Nil(t, err)
		d1, err := NewDocumentFrom(map[string]any{
			"age":  51,
			"name": "lacee",
		})
		assert.Nil(t, err)
		t.Run("compare age", func(t *testing.T) {
			assert.False(t, compareField("age", d, d1))
		})
		t.Run("compare age (reverse)", func(t *testing.T) {
			assert.True(t, compareField("age", d1, d))
		})
		t.Run("compare name", func(t *testing.T) {
			assert.False(t, compareField("name", d, d1))
		})
		t.Run("compare name (reverse)", func(t *testing.T) {
			assert.True(t, compareField("name", d1, d))
		})
		t.Run("compare isMale", func(t *testing.T) {
			assert.True(t, compareField("isMale", d, d1))
		})
		t.Run("compare name (reverse)", func(t *testing.T) {
			assert.False(t, compareField("isMale", d1, d))
		})
	})
	t.Run("decode", func(t *testing.T) {
		d, err := NewDocumentFrom(map[string]any{
			"age":    50,
			"name":   "coleman",
			"isMale": true,
		})
		assert.Nil(t, err)
		d1 := NewDocument()
		assert.Nil(t, util.Decode(d, d1))
		assert.Equal(t, d.String(), d1.String())
	})
}
