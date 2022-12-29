package gokvkit

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	t.Run("validate empty query", func(t *testing.T) {
		q := Query{
			Select: nil,
			Where:  nil,
		}
		assert.NotNil(t, q.Validate(context.Background()))
	})
	t.Run("validate no select", func(t *testing.T) {
		a := Query{}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad group by", func(t *testing.T) {
		a := Query{
			GroupBy: []string{"account_id"},
			Select: []Select{
				{
					Field:     "age",
					Aggregate: AggregateFunctionSum,
					As:        "age_sum",
				},
			},
			OrderBy: []OrderBy{
				{
					Field:     "account_id",
					Direction: OrderByDirectionAsc,
				},
			},
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad order by", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field: "*",
				},
			},
			OrderBy: []OrderBy{
				{
					Field:     "account_id",
					Direction: "dsc",
				},
			},
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad where op", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field: "*",
				},
			},
			Where: []Where{
				{
					Field: "account_id",
					Op:    "==",
					Value: 9,
				},
			},
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad where field", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field: "*",
				},
			},
			Where: []Where{
				{
					Field: "",
					Op:    WhereOpEq,
					Value: 9,
				},
			},
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad where value", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field: "*",
				},
			},
			Where: []Where{
				{
					Field: "name",
					Op:    WhereOpEq,
				},
			},
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad limit", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field: "*",
				},
			},
			Limit: -1,
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad page", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field: "*",
				},
			},
			Page: -1,
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate good query", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field:     "test",
					Aggregate: AggregateFunctionMax,
					As:        "max_test",
				},
			},
		}
		assert.Nil(t, a.Validate(context.Background()))
	})
}

func TestContext(t *testing.T) {
	ctx := context.Background()
	c, ok := GetMetadata(ctx)
	assert.False(t, ok)
	assert.NotNil(t, c)
	c = NewMetadata(map[string]any{
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
	assert.NoError(t, err)
	assert.Equal(t, "{\"testing\":false}", string(bits))
	assert.Equal(t, "{\"testing\":false}", c.String())

	c.Del("testing")

	v, ok = c.Get("testing")
	assert.False(t, ok)
	assert.Nil(t, v)

	ctx = c.ToContext(ctx)
	c, ok = GetMetadata(ctx)
	assert.True(t, ok)
	assert.NotNil(t, c)

	assert.Nil(t, json.Unmarshal(bits, c))
	assert.True(t, c.Exists("testing"))
}
