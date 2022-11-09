package brutus_test

import (
	"context"
	"github.com/autom8ter/brutus"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestContext(t *testing.T) {
	ctx := context.Background()
	c, ok := brutus.GetContext(ctx)
	assert.False(t, ok)
	assert.NotNil(t, c)
	c = brutus.NewContext(map[string]any{
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
	c.Del("testing")

	v, ok = c.Get("testing")
	assert.False(t, ok)
	assert.Nil(t, v)

	ctx = c.ToContext(ctx)
	c, ok = brutus.GetContext(ctx)
	assert.True(t, ok)
	assert.NotNil(t, c)
}
