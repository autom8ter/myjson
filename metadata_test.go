package gokvkit_test

import (
	"context"
	"encoding/json"
	"github.com/autom8ter/gokvkit"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestContext(t *testing.T) {
	ctx := context.Background()
	c, ok := gokvkit.GetMetadata(ctx)
	assert.False(t, ok)
	assert.NotNil(t, c)
	c = gokvkit.NewMetadata(map[string]any{
		"testing": true,
	})
	v, ok := c.Get("testing")
	assert.True(t, ok)
	assert.True(t, cast.ToBool(v))
	c.Set("testing", false)
	v, ok = c.Get("testing")
	assert.True(t, ok)
	assert.False(t, cast.ToBool(v))
	assert.NotNil(t, c.Map())
	assert.True(t, c.Exists("testing"))
	bits, err := json.Marshal(c)
	assert.Nil(t, err)
	assert.Equal(t, "{\"testing\":false}", string(bits))
	assert.Equal(t, "{\"testing\":false}", c.String())

	c.Del("testing")

	v, ok = c.Get("testing")
	assert.False(t, ok)
	assert.Nil(t, v)

	ctx = c.ToContext(ctx)
	c, ok = gokvkit.GetMetadata(ctx)
	assert.True(t, ok)
	assert.NotNil(t, c)

	assert.Nil(t, json.Unmarshal(bits, c))
	assert.True(t, c.Exists("testing"))
}
